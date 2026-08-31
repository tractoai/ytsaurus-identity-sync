package ldap

import (
	"fmt"
	"testing"

	ldapclient "github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
)

var (
	_ source.SourceUser  = LdapUser{}
	_ source.SourceGroup = LdapGroup{}
	_ source.Source      = (*Ldap)(nil)
)

func TestLdapUser(t *testing.T) {
	user := LdapUser{
		Username:  "alice@acme.com",
		UID:       "alice-id",
		FirstName: "Alice",
	}

	raw, err := user.GetRaw()
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"username":   "alice@acme.com",
		"uid":        "alice-id",
		"first_name": "Alice",
	}, raw)
}

func TestLdapGroup(t *testing.T) {
	group := LdapGroup{Groupname: "acme.devs|all"}

	raw, err := group.GetRaw()
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"groupname": "acme.devs|all",
	}, raw)
}

func TestCreateFromRaw(t *testing.T) {
	ldapSource := &Ldap{}

	user, err := ldapSource.CreateUserFromRaw(map[string]any{
		"username":   "alice@acme.com",
		"uid":        "alice-id",
		"first_name": "Alice",
	})
	require.NoError(t, err)
	require.Equal(t, LdapUser{
		Username:  "alice@acme.com",
		UID:       "alice-id",
		FirstName: "Alice",
	}, *user.(*LdapUser))

	group, err := ldapSource.CreateGroupFromRaw(map[string]any{
		"groupname": "acme.devs|all",
	})
	require.NoError(t, err)
	require.Equal(t, LdapGroup{Groupname: "acme.devs|all"}, *group.(*LdapGroup))
}

func TestGetUsersAndGroupsWithMembers(t *testing.T) {
	t.Setenv("LDAP_PASSWORD", openLDAPAdminPassword)

	ldapLocal := NewOpenLdapLocal()
	require.NoError(t, ldapLocal.Start())
	defer func() { require.NoError(t, ldapLocal.Stop()) }()

	cfg, err := ldapLocal.GetConfig()
	require.NoError(t, err)
	ldapSource, err := NewLdap(cfg, logging.NewDevelopment())
	require.NoError(t, err)

	users := []LdapUser{
		{Username: "alice@acme.com", UID: "1001", FirstName: "Alice"},
		{Username: "bob@acme.com", UID: "1002", FirstName: "Bob"},
	}
	groups := []source.SourceGroupWithMembers{
		{
			SourceGroup: LdapGroup{Groupname: "acme.devs|all"},
			Members:     NewStringSetFromItems("1001", "1002"),
		},
	}
	setupLDAPObjects(t, ldapSource.connection, users, groups)

	actualUsers, err := ldapSource.GetUsers()
	require.NoError(t, err)
	require.ElementsMatch(t, []source.SourceUser{users[0], users[1]}, actualUsers)

	actualGroups, err := ldapSource.GetGroupsWithMembers()
	require.NoError(t, err)
	require.Len(t, actualGroups, 1)
	require.Equal(t, groups[0].SourceGroup, actualGroups[0].SourceGroup)
	require.ElementsMatch(t, groups[0].Members.ToSlice(), actualGroups[0].Members.ToSlice())
}

func setupLDAPObjects(t *testing.T, conn *ldapclient.Conn, users []LdapUser, groups []source.SourceGroupWithMembers) {
	t.Helper()

	require.NoError(t, conn.Add(&ldapclient.AddRequest{
		DN: "ou=People,dc=example,dc=org",
		Attributes: []ldapclient.Attribute{
			{Type: "objectClass", Vals: []string{"organizationalUnit"}},
			{Type: "ou", Vals: []string{"People"}},
		},
	}))
	require.NoError(t, conn.Add(&ldapclient.AddRequest{
		DN: "ou=Group,dc=example,dc=org",
		Attributes: []ldapclient.Attribute{
			{Type: "objectClass", Vals: []string{"organizationalUnit"}},
			{Type: "ou", Vals: []string{"Group"}},
		},
	}))

	for _, user := range users {
		require.NoError(t, conn.Add(&ldapclient.AddRequest{
			DN: fmt.Sprintf("uid=%s,ou=People,dc=example,dc=org", user.GetID()),
			Attributes: []ldapclient.Attribute{
				{Type: "objectClass", Vals: []string{"top", "posixAccount", "inetOrgPerson"}},
				{Type: "ou", Vals: []string{"People"}},
				{Type: "cn", Vals: []string{user.GetName()}},
				{Type: "uid", Vals: []string{user.GetID()}},
				{Type: "uidNumber", Vals: []string{user.GetID()}},
				{Type: "gidNumber", Vals: []string{user.GetID()}},
				{Type: "givenName", Vals: []string{user.FirstName}},
				{Type: "homeDirectory", Vals: []string{user.GetID()}},
				{Type: "sn", Vals: []string{user.GetName() + "-surname"}},
			},
		}))
	}

	for groupID, group := range groups {
		ldapGroup := group.SourceGroup.(LdapGroup)
		require.NoError(t, conn.Add(&ldapclient.AddRequest{
			DN: fmt.Sprintf("cn=%s,ou=Group,dc=example,dc=org", ldapGroup.GetID()),
			Attributes: []ldapclient.Attribute{
				{Type: "objectClass", Vals: []string{"top", "organizationalRole", "posixGroup"}},
				{Type: "cn", Vals: []string{ldapGroup.GetName()}},
				{Type: "gidNumber", Vals: []string{fmt.Sprint(groupID)}},
				{Type: "memberUid", Vals: group.Members.ToSlice()},
			},
		}))
	}
}

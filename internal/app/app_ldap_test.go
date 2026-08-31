package app

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"
	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
	ldapsource "github.com/tractoai/ytsaurus-identity-sync/internal/sources/ldap"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	testclock "k8s.io/utils/clock/testing"
)

func createLdapUser(name string) ldapsource.LdapUser {
	return ldapsource.LdapUser{
		Username:  fmt.Sprintf("%s@acme.com", name),
		UID:       getUserID(name),
		FirstName: name,
	}
}

func createUpdatedLdapUser(name string) ldapsource.LdapUser {
	user := createLdapUser(name)
	return ldapsource.LdapUser{
		Username:  user.Username,
		UID:       user.UID,
		FirstName: user.FirstName + "-updated",
	}
}

func createYtsaurusUser(name string) internalyt.YtsaurusUser {
	originalUsername := fmt.Sprintf("%s@acme.com", name)
	ytUsername := originalUsername
	for _, replacement := range defaultUsernameReplacements {
		ytUsername = strings.Replace(ytUsername, replacement.From, replacement.To, -1)
	}

	return internalyt.YtsaurusUser{Username: ytUsername, SourceRaw: map[string]any{
		"username":   originalUsername,
		"uid":        getUserID(name),
		"first_name": name,
	}}
}

func createUpdatedYtsaurusUser(name string) internalyt.YtsaurusUser {
	user := createYtsaurusUser(name)
	user.SourceRaw["first_name"] = name + "-updated"
	return user
}

func bannedYtsaurusUser(ytUser internalyt.YtsaurusUser, bannedSince time.Time) internalyt.YtsaurusUser {
	return internalyt.YtsaurusUser{Username: ytUser.Username, SourceRaw: ytUser.SourceRaw, BannedSince: bannedSince}
}

func createLdapGroup(name string) ldapsource.LdapGroup {
	name = "acme." + name
	return ldapsource.LdapGroup{
		Groupname: fmt.Sprintf("%v|all", name),
	}
}

func createYtsaurusGroup(name string) internalyt.YtsaurusGroup {
	name = "acme." + name
	originalName := fmt.Sprintf("%v|all", name)
	ytName := originalName
	for _, replacement := range defaultGroupnameReplacements {
		ytName = strings.Replace(ytName, replacement.From, replacement.To, -1)
	}
	return internalyt.YtsaurusGroup{Name: name, SourceRaw: map[string]any{
		"groupname": originalName,
	}}
}

var (
	ldapTestCases = []testCase{
		{
			name: "a-skip-b-create-c-remove",
			sourceUsersSetUp: []source.SourceUser{
				createLdapUser(aliceName),
				createLdapUser(bobName),
			},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(carolName),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
			},
		},
		{
			name: "bob-is-banned",
			appConfig: &config.AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
			},
			sourceUsersSetUp: []source.SourceUser{
				createLdapUser(aliceName),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				bannedYtsaurusUser(createYtsaurusUser(bobName), initialTestTime),
			},
		},
		{
			name: "bob-was-banned-now-deleted-carol-was-banned-now-back",
			// Bob was banned at initialTestTime,
			// 2 days have passed (more than setting allows) —> he should be removed.
			// Carol was banned 8 hours ago and has been found in Azure -> she should be restored.
			testTime: initialTestTime.Add(48 * time.Hour),
			appConfig: &config.AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				bannedYtsaurusUser(createYtsaurusUser(bobName), initialTestTime),
				bannedYtsaurusUser(createYtsaurusUser(carolName), initialTestTime.Add(40*time.Hour)),
			},
			sourceUsersSetUp: []source.SourceUser{
				createLdapUser(aliceName),
				createLdapUser(carolName),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(carolName),
			},
		},
		{
			name: "remove-limit-users-3",
			appConfig: &config.AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			sourceUsersSetUp: []source.SourceUser{},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			// No one is deleted: limitation works.
			ytUsersExpected: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
		},
		{
			name: "remove-limit-groups-3",
			appConfig: &config.AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			sourceGroupsSetUp: []source.SourceGroupWithMembers{},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				internalyt.NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("devs")),
				internalyt.NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("qa")),
				internalyt.NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("hq")),
			},
			// No group is deleted: limitation works.
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				internalyt.NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("devs")),
				internalyt.NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("qa")),
				internalyt.NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("hq")),
			},
		},
		{
			name: "a-changed-name-b-changed-email",
			sourceUsersSetUp: []source.SourceUser{
				createUpdatedLdapUser(aliceName),
				createUpdatedLdapUser(bobName),
			},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{
				createUpdatedYtsaurusUser(aliceName),
				createUpdatedYtsaurusUser(bobName),
			},
		},
		{
			name: "skip-create-remove-group-no-members-change-correct-name-replace",
			sourceUsersSetUp: []source.SourceUser{
				createLdapUser(aliceName),
				createLdapUser(bobName),
				createLdapUser(carolName),
			},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroup("devs"),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: createYtsaurusGroup("qa"),
					Members:       NewStringSetFromItems(bobName),
				},
			},
			sourceGroupsSetUp: []source.SourceGroupWithMembers{
				{
					SourceGroup: createLdapGroup("devs"),
					Members:     NewStringSetFromItems(getUserID(aliceName)),
				},
				{
					SourceGroup: createLdapGroup("hq"),
					Members:     NewStringSetFromItems(getUserID(carolName)),
				},
			},
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroup("devs"),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: createYtsaurusGroup("hq"),
					Members:       NewStringSetFromItems(carolName),
				},
			},
		},
		{
			name: "memberships-add-remove",
			sourceUsersSetUp: []source.SourceUser{
				createLdapUser(aliceName),
				createLdapUser(bobName),
				createLdapUser(carolName),
			},
			ytUsersSetUp: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroup("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
					),
				},
			},
			sourceGroupsSetUp: []source.SourceGroupWithMembers{
				{
					SourceGroup: createLdapGroup("devs"),
					Members: NewStringSetFromItems(
						getUserID(aliceName),
						getUserID(carolName),
					),
				},
			},
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroup("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						carolName,
					),
				},
			},
		},
	}
)

func (suite *AppTestSuite) TestLdapSyncOnce() {
	t := suite.T()

	for _, tc := range ldapTestCases {
		t.Run(
			tc.name,
			func(tc testCase) func(t *testing.T) {
				return func(t *testing.T) {
					defer suite.clear()

					if tc.testTime.IsZero() {
						tc.testTime = initialTestTime
					}
					passiveClock := testclock.NewFakePassiveClock(tc.testTime)

					ldapLocal := NewOpenLdapLocal()

					defer func() { require.NoError(t, ldapLocal.Stop()) }()
					require.NoError(t, ldapLocal.Start())

					ldapConfig, err := ldapLocal.GetConfig()
					require.NoError(t, err)
					ldapSource, err := ldapsource.NewLdap(ldapConfig, logging.NewDevelopment())
					require.NoError(t, err)

					setupLdapObjects(t, ldapSource.Connection(), tc.sourceUsersSetUp, tc.sourceGroupsSetUp)

					setupYtsaurusObjects(
						t,
						suite.ytsaurusClient,
						tc.ytUsersSetUp,
						tc.ytGroupsSetUp,
					)

					suite.syncOnce(t, ldapSource, passiveClock, tc.appConfig)

					suite.check(t, tc.ytUsersExpected, tc.ytGroupsExpected)
				}
			}(tc),
		)
	}
}

func setupLdapObjects(t *testing.T, conn *ldap.Conn, users []source.SourceUser, groups []source.SourceGroupWithMembers) {
	require.NoError(t, conn.Add(&ldap.AddRequest{
		DN: "ou=People,dc=example,dc=org",
		Attributes: []ldap.Attribute{
			{
				Type: "objectClass",
				Vals: []string{"organizationalUnit"},
			},
			{
				Type: "ou",
				Vals: []string{"People"},
			},
		},
	}))

	require.NoError(t, conn.Add(&ldap.AddRequest{
		DN: "ou=Group,dc=example,dc=org",
		Attributes: []ldap.Attribute{
			{
				Type: "objectClass",
				Vals: []string{"organizationalUnit"},
			},
			{
				Type: "ou",
				Vals: []string{"Group"},
			},
		},
	}))

	for _, user := range users {
		ldapUser := user.(ldapsource.LdapUser)
		addRequest := ldap.AddRequest{
			DN: fmt.Sprintf("uid=%s,ou=People,dc=example,dc=org", user.GetID()),
			Attributes: []ldap.Attribute{
				{
					Type: "objectClass",
					Vals: []string{"top", "posixAccount", "inetOrgPerson"},
				},
				{
					Type: "ou",
					Vals: []string{"People"},
				},
				{
					Type: "cn",
					Vals: []string{user.GetName()},
				},
				{
					Type: "uid",
					Vals: []string{user.GetID()},
				},
				{
					Type: "uidNumber",
					Vals: []string{user.GetID()},
				},
				{
					Type: "gidNumber",
					Vals: []string{user.GetID()},
				},
				{
					Type: "givenName",
					Vals: []string{ldapUser.FirstName},
				},
				{
					Type: "homeDirectory",
					Vals: []string{ldapUser.GetID()},
				},
				{
					Type: "sn",
					Vals: []string{ldapUser.GetName() + "-surname"},
				},
			},
		}
		require.NoError(t, conn.Add(&addRequest))
	}

	for groupID, group := range groups {
		ldapGroup := group.SourceGroup.(ldapsource.LdapGroup)

		members := make([]string, 0)
		for member := range group.Members.Iter() {
			members = append(members, member)
		}

		addRequest := ldap.AddRequest{
			DN: fmt.Sprintf("cn=%s,ou=Group,dc=example,dc=org", ldapGroup.GetID()),
			Attributes: []ldap.Attribute{
				{
					Type: "objectClass",
					Vals: []string{"top", "posixGroup"},
				},
				{
					Type: "cn",
					Vals: []string{ldapGroup.GetName()},
				},
				{
					Type: "gidNumber",
					Vals: []string{fmt.Sprint(groupID)},
				},
				{
					Type: "memberUid",
					Vals: members,
				},
			},
		}
		require.NoError(t, conn.Add(&addRequest))
	}
}

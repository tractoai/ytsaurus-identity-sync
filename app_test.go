package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	testclock "k8s.io/utils/clock/testing"

	"go.ytsaurus.tech/yt/go/yt"
)

const (
	ytDevToken = "password"
	aliceName  = "alice"
	bobName    = "bob"
	carolName  = "carol"
)

type testCase struct {
	name      string
	appConfig *AppConfig
	testTime  time.Time

	sourceType SourceType

	sourceUsersSetUp []SourceUser
	ytUsersSetUp     []YtsaurusUser
	ytUsersExpected  []YtsaurusUser

	sourceGroupsSetUp []SourceGroupWithMembers
	ytGroupsSetUp     []YtsaurusGroupWithMembers
	ytGroupsExpected  []YtsaurusGroupWithMembers
}

func getUserID(name string) string {
	switch name {
	case aliceName:
		return "1"
	case bobName:
		return "2"
	case carolName:
		return "3"
	}
	return "4"
}

func getSourceUser(name string, sourceType SourceType) SourceUser {
	switch sourceType {
	case LdapSourceType:
		return LdapUser{
			Username:  fmt.Sprintf("%v@acme.com", name),
			UID:       getUserID(name),
			FirstName: fmt.Sprintf("%v@acme.com-firstname", name),
		}
	case AzureSourceType:
		return AzureUser{
			PrincipalName: fmt.Sprintf("%v@acme.com", name),
			AzureID:       fmt.Sprintf("fake-az-id-%v", name),
			Email:         fmt.Sprintf("%v@acme.com", name),
			FirstName:     fmt.Sprintf("%v@acme.com-firstname", name),
			LastName:      fmt.Sprintf("%v-lastname", name),
			DisplayName:   fmt.Sprintf("Henderson, %v (ACME)", name),
		}
	}
	return nil
}

func getUpdatedSourceUser(name string, sourceType SourceType) SourceUser {
	sourceUser := getSourceUser(name, sourceType)
	switch sourceType {
	case LdapSourceType:
		ldapSourceUser := sourceUser.(LdapUser)
		return LdapUser{
			Username:  ldapSourceUser.Username,
			UID:       ldapSourceUser.UID,
			FirstName: ldapSourceUser.FirstName + "-updated",
		}
	case AzureSourceType:
		azureSourceUser := sourceUser.(AzureUser)
		return AzureUser{
			PrincipalName: azureSourceUser.PrincipalName,
			AzureID:       azureSourceUser.AzureID,
			Email:         azureSourceUser.Email + "-updated",
			FirstName:     azureSourceUser.FirstName,
			LastName:      azureSourceUser.LastName,
			DisplayName:   azureSourceUser.DisplayName,
		}
	}
	return nil
}

func getYtsaurusUser(sourceUser SourceUser) YtsaurusUser {
	name := sourceUser.GetName()
	for _, replacement := range defaultUsernameReplacements {
		name = strings.Replace(name, replacement.From, replacement.To, -1)
	}
	return YtsaurusUser{Username: name, SourceUser: sourceUser}
}

func bannedYtsaurusUser(ytUser YtsaurusUser, bannedSince time.Time) YtsaurusUser {
	return YtsaurusUser{Username: ytUser.Username, SourceUser: ytUser.SourceUser, BannedSince: bannedSince}
}

func getSourceGroup(name string, sourceType SourceType) SourceGroup {
	switch sourceType {
	case AzureSourceType:
		return AzureGroup{
			Identity:    fmt.Sprintf("acme.%v|all", name),
			AzureID:     fmt.Sprintf("fake-az-acme.%v", name),
			DisplayName: fmt.Sprintf("acme.%v|all", name),
		}
	case LdapSourceType:
		return LdapGroup{
			Groupname: fmt.Sprintf("acme.%v|all", name),
		}
	}
	return nil
}

func getUpdatedSourceGroup(name string, sourceType SourceType) SourceGroup {
	sourceGroup := getSourceGroup(name, sourceType)
	switch sourceType {
	case LdapSourceType:
		// TODO(nadya73): add more fields.
		ldapSourceGroup := sourceGroup.(LdapGroup)
		return LdapGroup{
			Groupname: ldapSourceGroup.Groupname,
		}
	case AzureSourceType:
		azureSourceGroup := sourceGroup.(AzureGroup)
		return AzureGroup{
			Identity:    azureSourceGroup.Identity,
			AzureID:     azureSourceGroup.AzureID,
			DisplayName: azureSourceGroup.DisplayName + "-updated",
		}
	}
	return nil
}

func getChangedBackwardCompatibleSourceGroup(name string, sourceType SourceType) SourceGroup {
	if sourceType != AzureSourceType {
		return nil
	}
	sourceGroup := getSourceGroup(name, sourceType)
	azureSourceGroup := sourceGroup.(AzureGroup)
	return AzureGroup{
		Identity:    azureSourceGroup.Identity + "-changed",
		AzureID:     azureSourceGroup.AzureID,
		DisplayName: azureSourceGroup.DisplayName + "-updated",
	}
}

func getYtsaurusGroup(sourceGroup SourceGroup) YtsaurusGroup {
	name := sourceGroup.GetName()
	for _, replacement := range defaultGroupnameReplacements {
		name = strings.Replace(name, replacement.From, replacement.To, -1)
	}
	return YtsaurusGroup{Name: name, SourceGroup: sourceGroup}
}

// We test several things in each test case, because of long wait for local ytsaurus
// container start.
func getTestCases(sourceType SourceType) []testCase {
	testCases := []testCase{
		{
			name:       "a-skip-b-create-c-remove",
			sourceType: sourceType,
			sourceUsersSetUp: []SourceUser{
				getSourceUser(aliceName, sourceType),
				getSourceUser(bobName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
			},
		},
		{
			name:       "bob-is-banned",
			sourceType: sourceType,
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			sourceUsersSetUp: []SourceUser{
				getSourceUser(aliceName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				bannedYtsaurusUser(getYtsaurusUser(getSourceUser(bobName, sourceType)), initialTestTime),
			},
		},
		{
			name:       "bob-was-banned-now-deleted-carol-was-banned-now-back",
			sourceType: sourceType,
			// Bob was banned at initialTestTime,
			// 2 days have passed (more than setting allows) —> he should be removed.
			// Carol was banned 8 hours ago and has been found in Source -> she should be restored.
			testTime: initialTestTime.Add(48 * time.Hour),
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			sourceUsersSetUp: []SourceUser{
				getSourceUser(aliceName, sourceType),
				getSourceUser(carolName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				bannedYtsaurusUser(getYtsaurusUser(getSourceUser(bobName, sourceType)), initialTestTime),
				bannedYtsaurusUser(getYtsaurusUser(getSourceUser(carolName, sourceType)), initialTestTime.Add(40*time.Hour)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
		},
		{
			name:       "remove-limit-users-3",
			sourceType: sourceType,
			appConfig: &AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			sourceUsersSetUp: []SourceUser{},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			// no one is deleted: limitation works
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
		},
		{
			name:       "remove-limit-groups-3",
			sourceType: sourceType,
			appConfig: &AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(getYtsaurusGroup(getSourceGroup("dev", sourceType))),
				NewEmptyYtsaurusGroupWithMembers(getYtsaurusGroup(getSourceGroup("qa", sourceType))),
				NewEmptyYtsaurusGroupWithMembers(getYtsaurusGroup(getSourceGroup("hq", sourceType))),
			},
			// no group is deleted: limitation works
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(getYtsaurusGroup(getSourceGroup("dev", sourceType))),
				NewEmptyYtsaurusGroupWithMembers(getYtsaurusGroup(getSourceGroup("qa", sourceType))),
				NewEmptyYtsaurusGroupWithMembers(getYtsaurusGroup(getSourceGroup("hq", sourceType))),
			},
		},
		{
			name:       "a-changed-name-b-changed-email",
			sourceType: sourceType,
			sourceUsersSetUp: []SourceUser{
				getUpdatedSourceUser(aliceName, sourceType),
				getUpdatedSourceUser(bobName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getUpdatedSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getUpdatedSourceUser(bobName, sourceType)),
			},
		},
		{
			name:       "skip-create-remove-group-no-members-change-correct-name-replace",
			sourceType: sourceType,
			sourceUsersSetUp: []SourceUser{
				getSourceUser(aliceName, sourceType),
				getSourceUser(bobName, sourceType),
				getSourceUser(carolName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("devs", sourceType)),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("qa", sourceType)),
					Members:       NewStringSetFromItems(bobName),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: getSourceGroup("devs", sourceType),
					Members:     NewStringSetFromItems(getSourceUser(aliceName, sourceType).GetID()),
				},
				{
					SourceGroup: getSourceGroup("hq", sourceType),
					Members:     NewStringSetFromItems(getSourceUser(carolName, sourceType).GetID()),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("devs", sourceType)),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("hq", sourceType)),
					Members:       NewStringSetFromItems(carolName),
				},
			},
		},
		{
			name:       "memberships-add-remove",
			sourceType: sourceType,
			sourceUsersSetUp: []SourceUser{
				getSourceUser(aliceName, sourceType),
				getSourceUser(bobName, sourceType),
				getSourceUser(carolName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("devs", sourceType)),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
					),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: getSourceGroup("devs", sourceType),
					Members: NewStringSetFromItems(
						getSourceUser(aliceName, sourceType).GetID(),
						getSourceUser(carolName, sourceType).GetID(),
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("devs", sourceType)),
					Members: NewStringSetFromItems(
						aliceName,
						carolName,
					),
				},
			},
		},
	}

	if sourceType == AzureSourceType {
		testCases = append(testCases, testCase{
			name:       "display-name-changes",
			sourceType: sourceType,
			sourceUsersSetUp: []SourceUser{
				getSourceUser(aliceName, sourceType),
				getSourceUser(bobName, sourceType),
				getSourceUser(carolName, sourceType),
			},
			ytUsersSetUp: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytUsersExpected: []YtsaurusUser{
				getYtsaurusUser(getSourceUser(aliceName, sourceType)),
				getYtsaurusUser(getSourceUser(bobName, sourceType)),
				getYtsaurusUser(getSourceUser(carolName, sourceType)),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("devs", sourceType)),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
					),
				},
				{
					YtsaurusGroup: getYtsaurusGroup(getSourceGroup("hq", sourceType)),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
					),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					// This group should be updated.
					SourceGroup: getUpdatedSourceGroup("devs", sourceType),
					// Members list are also updated.
					Members: NewStringSetFromItems(
						getSourceUser(aliceName, sourceType).GetID(),
						getSourceUser(carolName, sourceType).GetID(),
					),
				},
				{
					// for this group only displayName should be updated
					SourceGroup: getChangedBackwardCompatibleSourceGroup("hq", sourceType),
					// members also changed
					Members: NewStringSetFromItems(
						getSourceUser(aliceName, sourceType).GetID(),
						getSourceUser(carolName, sourceType).GetID(),
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: getYtsaurusGroup(getUpdatedSourceGroup("devs", sourceType)),
					Members: NewStringSetFromItems(
						aliceName,
						carolName,
					),
				},
				{
					YtsaurusGroup: getYtsaurusGroup(getChangedBackwardCompatibleSourceGroup("hq", sourceType)),
					Members: NewStringSetFromItems(
						aliceName,
						carolName,
					),
				},
			},
		})
	}
	return testCases
}

var (
	testTimeStr     = "2023-10-20T12:00:00Z"
	initialTestTime = parseAppTime(testTimeStr)

	defaultUsernameReplacements = []ReplacementPair{
		{"@acme.com", ""},
		{"@", ":"},
	}
	defaultGroupnameReplacements = []ReplacementPair{
		{"|all", ""},
	}
	defaultAppConfig = &AppConfig{
		UsernameReplacements:  defaultUsernameReplacements,
		GroupnameReplacements: defaultGroupnameReplacements,
	}
)

// TestAppSync uses local YTsaurus container and some source (Azure or Ldap) to test all the cases:
// [x] If azur user not in YTsaurus -> created;
// [x] If Source user already in YTsaurus no changes -> skipped;
// [x] If Source user already in YTsaurus with changes -> updated;
// [x] If user in YTsaurus but not in Source (and ban_before_remove_duration=0) -> removed;
// [x] If user in YTsaurus but not in Source (and ban_before_remove_duration != 0) -> banned -> removed;
// [x] If Azure user without @azure attribute in YTsaurus —> ignored;
// [x] Source user field updates is reflected in YTsaurus user;
// [x] YTsaurus username is built according to config;
// [x] YTsaurus username is in lowercase;
// [x] If Source group is not exist in YTsaurus -> creating YTsaurus with members;
// [x] If YTsaurus group is not exist in Source -> delete YTsaurus group;
// [x] If Source group membership changed -> update in YTsaurus group membership;
// [x] If Source group fields (though there are none extra fields) changed -> update YTsaurus group;
// [x] If Source group displayName changed -> recreate YTsaurus group;
// [x] If Source group displayName changed AND Source members changed -> recreate YTsaurus group with actual members set;
// [x] YTsaurus group name is built according to config;
// [x] Remove limits config option works.
func TestAppSync(t *testing.T) {
	require.NoError(t, os.Setenv(defaultYtsaurusSecretEnvVar, ytDevToken))
	for _, sourceType := range []SourceType{LdapSourceType, AzureSourceType} {
		for _, tc := range getTestCases(sourceType) {
			t.Run(
				fmt.Sprintf("%v-%v", sourceType, tc.name),
				func(tc testCase) func(t *testing.T) {
					return func(t *testing.T) {
						if tc.testTime.IsZero() {
							tc.testTime = initialTestTime
						}
						clock := testclock.NewFakePassiveClock(initialTestTime)

						ytLocal := NewYtsaurusLocal()
						defer func() { require.NoError(t, ytLocal.Stop()) }()
						require.NoError(t, ytLocal.Start())

						var source Source

						switch tc.sourceType {
						case AzureSourceType:
							azure := NewAzureFake()
							azure.setUsers(tc.sourceUsersSetUp)
							azure.setGroups(tc.sourceGroupsSetUp)

							source = azure
						case LdapSourceType:
							ldapLocal := NewOpenLdapLocal()

							defer func() { require.NoError(t, ldapLocal.Stop()) }()
							require.NoError(t, ldapLocal.Start())

							ldapConfig, err := ldapLocal.GetConfig()
							require.NoError(t, err)
							ldapSource, err := NewLdap(ldapConfig, getDevelopmentLogger())
							require.NoError(t, err)

							setupLdapObjects(t, ldapSource.Connection, tc.sourceUsersSetUp, tc.sourceGroupsSetUp)
							source = ldapSource
						}

						ytClient, err := ytLocal.GetClient()
						require.NoError(t, err)

						initialYtUsers, initialYtGroups := getAllYtsaurusObjects(t, ytClient)
						setupYtsaurusObjects(t, ytClient, tc.ytUsersSetUp, tc.ytGroupsSetUp)

						if tc.appConfig == nil {
							tc.appConfig = defaultAppConfig
						}
						app, err := NewAppCustomized(
							&Config{
								App:   *tc.appConfig,
								Azure: &AzureConfig{},
								Ldap:  &LdapConfig{},
								Ytsaurus: YtsaurusConfig{
									Proxy:              ytLocal.GetProxy(),
									ApplyUserChanges:   true,
									ApplyGroupChanges:  true,
									ApplyMemberChanges: true,
									LogLevel:           "DEBUG",
								},
							},
							getDevelopmentLogger(),
							source,
							clock,
						)
						require.NoError(t, err)

						app.syncOnce()

						// We have eventually here, because user removal takes some time.
						require.Eventually(
							t,
							func() bool {
								udiff, gdiff := diffYtsaurusObjects(t, ytClient, tc.ytUsersExpected, initialYtUsers, tc.ytGroupsExpected, initialYtGroups)
								actualUsers, actualGroups := getAllYtsaurusObjects(t, ytClient)
								if udiff != "" {
									t.Log("Users diff is not empty yet:", udiff)
									t.Log("expected users", tc.ytUsersExpected)
									t.Log("actual users", actualUsers)
								}
								if gdiff != "" {
									t.Log("Groups diff is not empty yet:", gdiff)
									t.Log("expected groups", tc.ytGroupsExpected)
									t.Log("actual groups", actualGroups)
								}
								return udiff == "" && gdiff == ""
							},
							3*time.Second,
							300*time.Millisecond,
						)
					}
				}(tc),
			)
		}
	}
}

func TestManageUnmanagedUsersIsForbidden(t *testing.T) {
	ytLocal := NewYtsaurusLocal()
	defer func() { require.NoError(t, ytLocal.Stop()) }()
	require.NoError(t, ytLocal.Start())

	ytClient, err := ytLocal.GetClient()
	require.NoError(t, err)

	ytsaurus, err := NewYtsaurus(
		&YtsaurusConfig{
			Proxy:    ytLocal.GetProxy(),
			LogLevel: "DEBUG",
		},
		getDevelopmentLogger(),
		testclock.NewFakePassiveClock(time.Now()),
	)
	require.NoError(t, err)

	unmanagedOleg := "oleg"

	err = doCreateYtsaurusUser(
		context.Background(),
		ytClient,
		unmanagedOleg,
		nil,
	)
	require.NoError(t, err)

	for _, username := range []string{
		"root",
		"guest",
		"job",
		unmanagedOleg,
	} {
		require.ErrorContains(t,
			ytsaurus.RemoveUser(username),
			"Prevented attempt to change manual managed user",
		)
		require.ErrorContains(t,
			ytsaurus.UpdateUser(username, YtsaurusUser{Username: username, SourceUser: AzureUser{Email: "dummy@acme.com"}}),
			"Prevented attempt to change manual managed user",
		)
	}
}

func getAllYtsaurusObjects(t *testing.T, client yt.Client) (users []YtsaurusUser, groups []YtsaurusGroupWithMembers) {
	allUsers, err := doGetAllYtsaurusUsers(context.Background(), client)
	require.NoError(t, err)
	allGroups, err := doGetAllYtsaurusGroupsWithMembers(context.Background(), client)
	require.NoError(t, err)
	return allUsers, allGroups
}

func setupLdapObjects(t *testing.T, conn *ldap.Conn, users []SourceUser, groups []SourceGroupWithMembers) {
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
		ldapUser := user.(LdapUser)
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
		ldapGroup := group.SourceGroup.(LdapGroup)

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

func setupYtsaurusObjects(t *testing.T, client yt.Client, users []YtsaurusUser, groups []YtsaurusGroupWithMembers) {
	t.Log("Setting up yt for test")
	for _, user := range users {
		t.Logf("creating user: %v", user)
		err := doCreateYtsaurusUser(
			context.Background(),
			client,
			user.Username,
			buildUserAttributes(user),
		)
		require.NoError(t, err)
	}

	for _, group := range groups {
		t.Log("creating group:", group)
		err := doCreateYtsaurusGroup(
			context.Background(),
			client,
			group.Name,
			buildGroupAttributes(group.YtsaurusGroup),
		)
		require.NoError(t, err)
		for member := range group.Members.Iter() {
			err = doAddMemberYtsaurusGroup(
				context.Background(),
				client,
				member,
				group.Name,
			)
		}
		require.NoError(t, err)
	}
}

func diffYtsaurusObjects(t *testing.T, client yt.Client, expectedUsers, initialUsers []YtsaurusUser, expectedGroups, initalGroups []YtsaurusGroupWithMembers) (string, string) {
	actualUsers, actualGroups := getAllYtsaurusObjects(t, client)
	allExpectedUsers := append(initialUsers, expectedUsers...)
	allExpectedGroups := append(initalGroups, expectedGroups...)

	// It seems that `users`  group @members attr contains not the all users in the system:
	// for example it doesn't include:
	// alien_cell_synchronizer, file_cache, guest, operations_cleaner, operations_client, etc...
	// we don't want to test that.
	// Though we expect it to include users created in test, so we update group members in out expected group list.
	var expectedNewUsernamesInUsersGroup []string
	for _, u := range expectedUsers {
		expectedNewUsernamesInUsersGroup = append(expectedNewUsernamesInUsersGroup, u.Username)
	}
	for idx, group := range allExpectedGroups {
		if group.Name == "users" {
			for _, uname := range expectedNewUsernamesInUsersGroup {
				allExpectedGroups[idx].Members.Add(uname)
			}
		}
	}

	uDiff := cmp.Diff(
		actualUsers,
		allExpectedUsers,
		cmpopts.SortSlices(func(left, right YtsaurusUser) bool {
			return left.Username < right.Username
		}),
	)
	gDiff := cmp.Diff(
		actualGroups,
		allExpectedGroups,
		cmpopts.SortSlices(func(left, right YtsaurusGroupWithMembers) bool {
			return left.Name < right.Name
		}),
	)

	return uDiff, gDiff
}

func parseAppTime(timStr string) time.Time {
	parsed, err := time.Parse(appTimeFormat, timStr)
	if err != nil {
		panic("parsing " + timStr + " failed")
	}
	return parsed
}

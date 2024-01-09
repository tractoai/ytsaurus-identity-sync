package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	testclock "k8s.io/utils/clock/testing"

	"go.ytsaurus.tech/yt/go/yt"
)

const (
	ytDevToken = "password"
)

type testCase struct {
	name      string
	appConfig *AppConfig
	testTime  time.Time

	azureUsersSetUp []AzureUser
	ytUsersSetUp    []YtsaurusUser
	ytUsersExpected []YtsaurusUser

	azureGroupsSetUp []AzureGroupWithMembers
	ytGroupsSetUp    []YtsaurusGroupWithMembers
	ytGroupsExpected []YtsaurusGroupWithMembers
}

var (
	testTimeStr     = "2023-10-20T12:00:00Z"
	initialTestTime = parseAppTime(testTimeStr)

	aliceAzure = AzureUser{
		PrincipalName: "alice@acme.com",
		AzureID:       "fake-az-id-alice",
		Email:         "alice@acme.com",
		FirstName:     "Alice",
		LastName:      "Henderson",
		DisplayName:   "Henderson, Alice (ACME)",
	}
	bobAzure = AzureUser{
		PrincipalName: "Bob@acme.com",
		AzureID:       "fake-az-id-bob",
		Email:         "Bob@acme.com",
		FirstName:     "Bob",
		LastName:      "Sanders",
		DisplayName:   "Sanders, Bob (ACME)",
	}
	carolAzure = AzureUser{
		PrincipalName: "carol@acme.com",
		AzureID:       "fake-az-id-carol",
		Email:         "carol@acme.com",
		FirstName:     "Carol",
		LastName:      "Sanders",
		DisplayName:   "Sanders, Carol (ACME)",
	}
	aliceAzureChangedLastName = AzureUser{
		PrincipalName: aliceAzure.PrincipalName,
		AzureID:       aliceAzure.AzureID,
		Email:         aliceAzure.Email,
		FirstName:     aliceAzure.FirstName,
		LastName:      "Smith",
		DisplayName:   aliceAzure.DisplayName,
	}
	bobAzureChangedEmail = AzureUser{
		PrincipalName: "bobby@example.com",
		AzureID:       bobAzure.AzureID,
		Email:         "bobby@example.com",
		FirstName:     bobAzure.FirstName,
		LastName:      bobAzure.LastName,
		DisplayName:   bobAzure.DisplayName,
	}
	devsAzureGroup = AzureGroup{
		Identity:    "acme.devs|all",
		AzureID:     "fake-az-acme.devs",
		DisplayName: "acme.devs|all",
	}
	hqAzureGroup = AzureGroup{
		Identity:    "acme.hq",
		AzureID:     "fake-az-acme.hq",
		DisplayName: "acme.hq",
	}
	devsAzureGroupChangedDisplayName = AzureGroup{
		Identity:    "acme.developers|all",
		AzureID:     devsAzureGroup.AzureID,
		DisplayName: "acme.developers|all",
	}
	hqAzureGroupChangedBackwardCompatible = AzureGroup{
		Identity:    "acme.hq|all",
		AzureID:     hqAzureGroup.AzureID,
		DisplayName: "acme.hq|all",
	}

	aliceYtsaurus = YtsaurusUser{
		Username:      "alice",
		AzureID:       aliceAzure.AzureID,
		PrincipalName: aliceAzure.PrincipalName,
		Email:         aliceAzure.Email,
		FirstName:     aliceAzure.FirstName,
		LastName:      aliceAzure.LastName,
		DisplayName:   aliceAzure.DisplayName,
	}
	bobYtsaurus = YtsaurusUser{
		Username:      "bob",
		AzureID:       bobAzure.AzureID,
		PrincipalName: bobAzure.PrincipalName,
		Email:         bobAzure.Email,
		FirstName:     bobAzure.FirstName,
		LastName:      bobAzure.LastName,
		DisplayName:   bobAzure.DisplayName,
	}
	carolYtsaurus = YtsaurusUser{
		Username:      "carol",
		AzureID:       carolAzure.AzureID,
		PrincipalName: carolAzure.PrincipalName,
		Email:         carolAzure.Email,
		FirstName:     carolAzure.FirstName,
		LastName:      carolAzure.LastName,
		DisplayName:   carolAzure.DisplayName,
	}
	aliceYtsaurusChangedLastName = YtsaurusUser{
		Username:      aliceYtsaurus.Username,
		AzureID:       aliceYtsaurus.AzureID,
		PrincipalName: aliceYtsaurus.PrincipalName,
		Email:         aliceYtsaurus.Email,
		FirstName:     aliceYtsaurus.FirstName,
		LastName:      aliceAzureChangedLastName.LastName,
		DisplayName:   aliceYtsaurus.DisplayName,
	}
	bobYtsaurusChangedEmail = YtsaurusUser{
		Username:      "bobby:example.com",
		AzureID:       bobYtsaurus.AzureID,
		PrincipalName: bobAzureChangedEmail.PrincipalName,
		Email:         bobAzureChangedEmail.Email,
		FirstName:     bobYtsaurus.FirstName,
		LastName:      bobYtsaurus.LastName,
		DisplayName:   bobYtsaurus.DisplayName,
	}
	bobYtsaurusBanned = YtsaurusUser{
		Username:      bobYtsaurus.Username,
		AzureID:       bobYtsaurus.AzureID,
		PrincipalName: bobYtsaurus.PrincipalName,
		Email:         bobYtsaurus.Email,
		FirstName:     bobYtsaurus.FirstName,
		LastName:      bobYtsaurus.LastName,
		DisplayName:   bobYtsaurus.DisplayName,
		BannedSince:   initialTestTime,
	}
	carolYtsaurusBanned = YtsaurusUser{
		Username:      carolYtsaurus.Username,
		AzureID:       carolYtsaurus.AzureID,
		PrincipalName: carolYtsaurus.PrincipalName,
		Email:         carolYtsaurus.Email,
		FirstName:     carolYtsaurus.FirstName,
		LastName:      carolYtsaurus.LastName,
		DisplayName:   carolYtsaurus.DisplayName,
		BannedSince:   initialTestTime.Add(40 * time.Hour),
	}
	devsYtsaurusGroup = YtsaurusGroup{
		Name:        "acme.devs",
		AzureID:     devsAzureGroup.AzureID,
		DisplayName: "acme.devs|all",
	}
	qaYtsaurusGroup = YtsaurusGroup{
		Name:        "acme.qa",
		AzureID:     "fake-az-acme.qa",
		DisplayName: "acme.qa|all",
	}
	hqYtsaurusGroup = YtsaurusGroup{
		Name:        "acme.hq",
		AzureID:     hqAzureGroup.AzureID,
		DisplayName: "acme.hq",
	}
	devsYtsaurusGroupChangedDisplayName = YtsaurusGroup{
		Name:        "acme.developers",
		AzureID:     devsAzureGroup.AzureID,
		DisplayName: "acme.developers|all",
	}
	hqYtsaurusGroupChangedBackwardCompatible = YtsaurusGroup{
		Name:        "acme.hq",
		AzureID:     hqAzureGroup.AzureID,
		DisplayName: "acme.hq|all",
	}

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

	// we test several things in each test case, because of long wait for local ytsaurus
	// container start.
	testCases = []testCase{
		{
			name: "a-skip-b-create-c-remove",
			azureUsersSetUp: []AzureUser{
				aliceAzure,
				bobAzure,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				carolYtsaurus,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
			},
		},
		{
			name: "bob-is-banned",
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
			},
			azureUsersSetUp: []AzureUser{
				aliceAzure,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurusBanned,
			},
		},
		{
			name: "bob-was-banned-now-deleted-carol-was-banned-now-back",
			// Bob was banned at initialTestTime,
			// 2 days have passed (more than setting allows) —> he should be removed.
			// Carol was banned 8 hours ago and has been found in Azure -> she should be restored.
			testTime: initialTestTime.Add(48 * time.Hour),
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurusBanned,
				carolYtsaurusBanned,
			},
			azureUsersSetUp: []AzureUser{
				aliceAzure,
				carolAzure,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				carolYtsaurus,
			},
		},
		{
			name: "remove-limit-users-3",
			appConfig: &AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			azureUsersSetUp: []AzureUser{},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			// no one is deleted: limitation works
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
		},
		{
			name: "remove-limit-groups-3",
			appConfig: &AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			azureGroupsSetUp: []AzureGroupWithMembers{},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(devsYtsaurusGroup),
				NewEmptyYtsaurusGroupWithMembers(qaYtsaurusGroup),
				NewEmptyYtsaurusGroupWithMembers(hqYtsaurusGroup),
			},
			// no group is deleted: limitation works
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(devsYtsaurusGroup),
				NewEmptyYtsaurusGroupWithMembers(qaYtsaurusGroup),
				NewEmptyYtsaurusGroupWithMembers(hqYtsaurusGroup),
			},
		},
		{
			name: "a-changed-name-b-changed-email",
			azureUsersSetUp: []AzureUser{
				aliceAzureChangedLastName,
				bobAzureChangedEmail,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurusChangedLastName,
				bobYtsaurusChangedEmail,
			},
		},
		{
			name: "skip-create-remove-group-no-members-change-correct-name-replace",
			azureUsersSetUp: []AzureUser{
				aliceAzure,
				bobAzure,
				carolAzure,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: devsYtsaurusGroup,
					Members:       NewStringSetFromItems(aliceYtsaurus.Username),
				},
				{
					YtsaurusGroup: qaYtsaurusGroup,
					Members:       NewStringSetFromItems(bobYtsaurus.Username),
				},
			},
			azureGroupsSetUp: []AzureGroupWithMembers{
				{
					AzureGroup: devsAzureGroup,
					Members:    NewStringSetFromItems(aliceAzure.AzureID),
				},
				{
					AzureGroup: hqAzureGroup,
					Members:    NewStringSetFromItems(carolAzure.AzureID),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: devsYtsaurusGroup,
					Members:       NewStringSetFromItems(aliceYtsaurus.Username),
				},
				{
					YtsaurusGroup: hqYtsaurusGroup,
					Members:       NewStringSetFromItems(carolYtsaurus.Username),
				},
			},
		},
		{
			name: "memberships-add-remove",
			azureUsersSetUp: []AzureUser{
				aliceAzure,
				bobAzure,
				carolAzure,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: devsYtsaurusGroup,
					Members: NewStringSetFromItems(
						aliceYtsaurus.Username,
						bobYtsaurus.Username,
					),
				},
			},
			azureGroupsSetUp: []AzureGroupWithMembers{
				{
					AzureGroup: devsAzureGroup,
					Members: NewStringSetFromItems(
						aliceAzure.AzureID,
						carolAzure.AzureID,
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: devsYtsaurusGroup,
					Members: NewStringSetFromItems(
						aliceYtsaurus.Username,
						carolYtsaurus.Username,
					),
				},
			},
		},
		{
			name: "display-name-changes",
			azureUsersSetUp: []AzureUser{
				aliceAzure,
				bobAzure,
				carolAzure,
			},
			ytUsersSetUp: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			ytUsersExpected: []YtsaurusUser{
				aliceYtsaurus,
				bobYtsaurus,
				carolYtsaurus,
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: devsYtsaurusGroup,
					Members: NewStringSetFromItems(
						aliceYtsaurus.Username,
						bobYtsaurus.Username,
					),
				},
				{
					YtsaurusGroup: hqYtsaurusGroup,
					Members: NewStringSetFromItems(
						aliceYtsaurus.Username,
						bobYtsaurus.Username,
					),
				},
			},
			azureGroupsSetUp: []AzureGroupWithMembers{
				{
					// This group should be updated.
					AzureGroup: devsAzureGroupChangedDisplayName,
					// Members list are also updated.
					Members: NewStringSetFromItems(
						aliceAzure.AzureID,
						carolAzure.AzureID,
					),
				},
				{
					// for this group only displayName should be updated
					AzureGroup: hqAzureGroupChangedBackwardCompatible,
					// members also changed
					Members: NewStringSetFromItems(
						aliceAzure.AzureID,
						carolAzure.AzureID,
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: devsYtsaurusGroupChangedDisplayName,
					Members: NewStringSetFromItems(
						aliceYtsaurus.Username,
						carolYtsaurus.Username,
					),
				},
				{
					YtsaurusGroup: hqYtsaurusGroupChangedBackwardCompatible,
					Members: NewStringSetFromItems(
						aliceYtsaurus.Username,
						carolYtsaurus.Username,
					),
				},
			},
		},
	}
)

// TestAppSync uses local YTsaurus container and fake Azure to test all the cases:
// [x] If Azure user not in YTsaurus -> created;
// [x] If Azure user already in YTsaurus no changes -> skipped;
// [x] If Azure user already in YTsaurus with changes -> updated;
// [x] If user in YTsaurus but not in Azure (and ban_before_remove_duration=0) -> removed;
// [x] If user in YTsaurus but not in Azure (and ban_before_remove_duration != 0) -> banned -> removed;
// [x] If Azure user without @azure attribute in YTsaurus —> ignored;
// [x] Azure user field updates is reflected in YTsaurus user;
// [x] YTsaurus username is built according to config;
// [x] YTsaurus username is in lowercase;
// [x] If Azure group is not exist in YTsaurus -> creating YTsaurus with members;
// [x] If YTsaurus group is not exist in Azure -> delete YTsaurus group;
// [x] If Azure group membership changed -> update in YTsaurus group membership;
// [x] If Azure group fields (though there are none extra fields) changed -> update YTsaurus group;
// [x] If Azure group displayName changed -> recreate YTsaurus group;
// [x] If Azure group displayName changed AND Azure members changed -> recreate YTsaurus group with actual members set;
// [x] YTsaurus group name is built according to config;
// [x] Remove limits config option works.
func TestAppSync(t *testing.T) {
	require.NoError(t, os.Setenv(defaultYtsaurusSecretEnvVar, ytDevToken))
	for _, tc := range testCases {
		t.Run(
			tc.name,
			func(tc testCase) func(t *testing.T) {
				return func(t *testing.T) {
					if tc.testTime.IsZero() {
						tc.testTime = initialTestTime
					}
					clock := testclock.NewFakePassiveClock(initialTestTime)

					ytLocal := NewYtsaurusLocal()
					defer func() { require.NoError(t, ytLocal.Stop()) }()
					require.NoError(t, ytLocal.Start())

					azure := NewAzureFake()
					azure.setUsers(tc.azureUsersSetUp)
					azure.setGroups(tc.azureGroupsSetUp)

					ytClient, err := ytLocal.GetClient()
					require.NoError(t, err)

					initialYtUsers, initialYtGroups := getAllYtsaurusObjects(t, ytClient)
					setupYtsaurusObjects(t, ytClient, tc.ytUsersSetUp, tc.ytGroupsSetUp)

					if tc.appConfig == nil {
						tc.appConfig = defaultAppConfig
					}
					app, err := NewAppCustomized(
						&Config{
							App:   tc.appConfig,
							Azure: &AzureConfig{},
							Ytsaurus: &YtsaurusConfig{
								Proxy:              ytLocal.GetProxy(),
								ApplyUserChanges:   true,
								ApplyGroupChanges:  true,
								ApplyMemberChanges: true,
								LogLevel:           "DEBUG",
							},
						}, getDevelopmentLogger(),
						azure,
						clock,
					)
					require.NoError(t, err)

					app.syncOnce()

					// we have eventually here, because user removal takes some time.
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
			ytsaurus.UpdateUser(username, YtsaurusUser{Username: username, Email: "dummy@acme.com"}),
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

package main

import (
	"testing"
	"time"

	testclock "k8s.io/utils/clock/testing"
)

var (
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
		AzureID:     "fake-az-acme.devs",
		DisplayName: "acme.devs|all",
	}
	hqAzureGroup = AzureGroup{
		AzureID:     "fake-az-acme.hq",
		DisplayName: "acme.hq",
	}
	devsAzureGroupChangedDisplayName = AzureGroup{
		AzureID:     devsAzureGroup.AzureID,
		DisplayName: "acme.developers|all",
	}
	hqAzureGroupChangedBackwardCompatible = AzureGroup{
		AzureID:     hqAzureGroup.AzureID,
		DisplayName: "acme.hq|all",
	}

	aliceYtsaurus = YtsaurusUser{
		Username: "alice",
		SourceRaw: map[string]any{
			"id":             aliceAzure.AzureID,
			"principal_name": aliceAzure.PrincipalName,
			"email":          aliceAzure.Email,
			"first_name":     aliceAzure.FirstName,
			"last_name":      aliceAzure.LastName,
			"display_name":   aliceAzure.DisplayName,
		},
	}
	bobYtsaurus = YtsaurusUser{
		Username: "bob",
		SourceRaw: map[string]any{
			"id":             bobAzure.AzureID,
			"principal_name": bobAzure.PrincipalName,
			"email":          bobAzure.Email,
			"first_name":     bobAzure.FirstName,
			"last_name":      bobAzure.LastName,
			"display_name":   bobAzure.DisplayName,
		},
	}
	carolYtsaurus = YtsaurusUser{
		Username: "carol",
		SourceRaw: map[string]any{
			"id":             carolAzure.AzureID,
			"principal_name": carolAzure.PrincipalName,
			"email":          carolAzure.Email,
			"first_name":     carolAzure.FirstName,
			"last_name":      carolAzure.LastName,
			"display_name":   carolAzure.DisplayName,
		},
	}
	aliceYtsaurusChangedLastName = YtsaurusUser{
		Username: aliceYtsaurus.Username,
		SourceRaw: map[string]any{
			"id":             aliceYtsaurus.SourceRaw["id"],
			"principal_name": aliceYtsaurus.SourceRaw["principal_name"],
			"email":          aliceYtsaurus.SourceRaw["email"],
			"first_name":     aliceYtsaurus.SourceRaw["first_name"],
			"last_name":      aliceAzureChangedLastName.LastName,
			"display_name":   aliceYtsaurus.SourceRaw["display_name"],
		},
	}
	bobYtsaurusChangedEmail = YtsaurusUser{
		Username: "bobby:example.com",
		SourceRaw: map[string]any{
			"id":             bobYtsaurus.SourceRaw["id"],
			"principal_name": bobAzureChangedEmail.PrincipalName,
			"email":          bobAzureChangedEmail.Email,
			"first_name":     bobYtsaurus.SourceRaw["first_name"],
			"last_name":      bobYtsaurus.SourceRaw["last_name"],
			"display_name":   bobYtsaurus.SourceRaw["display_name"],
		},
	}
	bobYtsaurusBanned = YtsaurusUser{
		Username: bobYtsaurus.Username,
		SourceRaw: map[string]any{
			"id":             bobYtsaurus.SourceRaw["id"],
			"principal_name": bobYtsaurus.SourceRaw["principal_name"],
			"email":          bobYtsaurus.SourceRaw["email"],
			"first_name":     bobYtsaurus.SourceRaw["first_name"],
			"last_name":      bobYtsaurus.SourceRaw["last_name"],
			"display_name":   bobYtsaurus.SourceRaw["display_name"],
		},
		BannedSince: initialTestTime,
	}
	carolYtsaurusBanned = YtsaurusUser{
		Username: carolYtsaurus.Username,
		SourceRaw: map[string]any{
			"id":             carolYtsaurus.SourceRaw["id"],
			"principal_name": carolYtsaurus.SourceRaw["principal_name"],
			"email":          carolYtsaurus.SourceRaw["email"],
			"first_name":     carolYtsaurus.SourceRaw["first_name"],
			"last_name":      carolYtsaurus.SourceRaw["last_name"],
			"display_name":   carolYtsaurus.SourceRaw["display_name"],
		},
		BannedSince: initialTestTime.Add(40 * time.Hour),
	}
	devsYtsaurusGroup = YtsaurusGroup{
		Name: "acme.devs",
		SourceRaw: map[string]any{
			"id":           devsAzureGroup.AzureID,
			"display_name": "acme.devs|all",
		},
	}
	qaYtsaurusGroup = YtsaurusGroup{
		Name: "acme.qa",
		SourceRaw: map[string]any{
			"id":           "fake-az-acme.qa",
			"display_name": "acme.qa|all",
		},
	}
	hqYtsaurusGroup = YtsaurusGroup{
		Name: "acme.hq",
		SourceRaw: map[string]any{
			"id":           hqAzureGroup.AzureID,
			"display_name": "acme.hq",
		},
	}
	devsYtsaurusGroupChangedDisplayName = YtsaurusGroup{
		Name: "acme.developers",
		SourceRaw: map[string]any{
			"id":           devsAzureGroup.AzureID,
			"display_name": "acme.developers|all",
		},
	}
	hqYtsaurusGroupChangedBackwardCompatible = YtsaurusGroup{
		Name: "acme.hq",
		SourceRaw: map[string]any{
			"id":           hqAzureGroup.AzureID,
			"display_name": "acme.hq|all",
		},
	}

	// we test several things in each test case, because of long wait for local ytsaurus
	// container start.
	azureTestCases = []testCase{
		{
			name: "a-skip-b-create-c-remove",
			sourceUsersSetUp: []SourceUser{
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
			sourceUsersSetUp: []SourceUser{
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
			sourceUsersSetUp: []SourceUser{
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
			sourceUsersSetUp: []SourceUser{},
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
			sourceGroupsSetUp: []SourceGroupWithMembers{},
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
			sourceUsersSetUp: []SourceUser{
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
			sourceUsersSetUp: []SourceUser{
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
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: devsAzureGroup,
					Members:     NewStringSetFromItems(aliceAzure.AzureID),
				},
				{
					SourceGroup: hqAzureGroup,
					Members:     NewStringSetFromItems(carolAzure.AzureID),
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
			sourceUsersSetUp: []SourceUser{
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
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: devsAzureGroup,
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
			sourceUsersSetUp: []SourceUser{
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
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					// This group should be updated.
					SourceGroup: devsAzureGroupChangedDisplayName,
					// Members list are also updated.
					Members: NewStringSetFromItems(
						aliceAzure.AzureID,
						carolAzure.AzureID,
					),
				},
				{
					// For this group only displayName should be updated.
					SourceGroup: hqAzureGroupChangedBackwardCompatible,
					// Members also changed.
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

// TestAzureSyncOnce uses local YTsaurus container and fake Azure to test all the cases:
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
func (suite *AppTestSuite) TestAzureSyncOnce() {
	t := suite.T()

	for _, tc := range azureTestCases {
		t.Run(
			tc.name,
			func(tc testCase) func(t *testing.T) {
				return func(t *testing.T) {
					defer suite.clear()

					if tc.testTime.IsZero() {
						tc.testTime = initialTestTime
					}
					passiveClock := testclock.NewFakePassiveClock(tc.testTime)

					azure := NewAzureFake()
					azure.setUsers(tc.sourceUsersSetUp)
					azure.setGroups(tc.sourceGroupsSetUp)

					setupYtsaurusObjects(
						t,
						suite.ytsaurusClient,
						tc.ytUsersSetUp,
						tc.ytGroupsSetUp,
					)

					suite.syncOnce(t, azure, passiveClock, tc.appConfig)

					suite.check(t, tc.ytUsersExpected, tc.ytGroupsExpected)
				}
			}(tc),
		)
	}
}

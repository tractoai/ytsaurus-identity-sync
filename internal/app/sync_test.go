package app

import (
	"strings"
	"testing"
	"time"

	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	testclock "k8s.io/utils/clock/testing"
)

var (
	aliceSource = fakeUser{
		ID:        "source-id-alice",
		Name:      "alice@acme.com",
		Email:     "alice@acme.com",
		FirstName: "Alice",
	}
	bobSource = fakeUser{
		ID:        "source-id-bob",
		Name:      "Bob@acme.com",
		Email:     "bob@acme.com",
		FirstName: "Bob",
	}
	carolSource = fakeUser{
		ID:        "source-id-carol",
		Name:      "carol@acme.com",
		Email:     "carol@acme.com",
		FirstName: "Carol",
	}
	aliceSourceChanged = fakeUser{
		ID:        aliceSource.ID,
		Name:      aliceSource.Name,
		Email:     aliceSource.Email,
		FirstName: "Alice-updated",
	}
	bobSourceChanged = fakeUser{
		ID:        bobSource.ID,
		Name:      "bobby@example.com",
		Email:     "bobby@example.com",
		FirstName: "Bob-updated",
	}

	devsSourceGroup = fakeGroup{ID: "source-group-devs", Name: "acme.devs|all"}
	qaSourceGroup   = fakeGroup{ID: "source-group-qa", Name: "acme.qa|all"}
	hqSourceGroup   = fakeGroup{ID: "source-group-hq", Name: "acme.hq"}

	devsSourceGroupRenamed = fakeGroup{ID: devsSourceGroup.ID, Name: "acme.developers|all"}
	hqSourceGroupChanged   = fakeGroup{ID: hqSourceGroup.ID, Name: "acme.hq|all"}

	aliceYT        = ytsaurusUserFromSource(aliceSource)
	bobYT          = ytsaurusUserFromSource(bobSource)
	carolYT        = ytsaurusUserFromSource(carolSource)
	aliceYTChanged = ytsaurusUserFromSource(aliceSourceChanged)
	bobYTChanged   = ytsaurusUserFromSource(bobSourceChanged)

	devsYTGroup        = ytsaurusGroupFromSource(devsSourceGroup)
	qaYTGroup          = ytsaurusGroupFromSource(qaSourceGroup)
	hqYTGroup          = ytsaurusGroupFromSource(hqSourceGroup)
	devsYTGroupRenamed = ytsaurusGroupFromSource(devsSourceGroupRenamed)
	hqYTGroupChanged   = ytsaurusGroupFromSource(hqSourceGroupChanged)

	syncTestCases = []testCase{
		{
			name:             "a-skip-b-create-c-remove",
			sourceUsersSetUp: sourceUsers(aliceSource, bobSource),
			ytUsersSetUp:     []internalyt.YtsaurusUser{aliceYT, carolYT},
			ytUsersExpected:  []internalyt.YtsaurusUser{aliceYT, bobYT},
		},
		{
			name: "bob-is-banned",
			appConfig: &config.AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			sourceUsersSetUp: sourceUsers(aliceSource),
			ytUsersSetUp:     []internalyt.YtsaurusUser{aliceYT, bobYT},
			ytUsersExpected:  []internalyt.YtsaurusUser{aliceYT, bannedUser(bobYT, initialTestTime)},
		},
		{
			name:     "bob-was-banned-now-deleted-carol-was-banned-now-back",
			testTime: initialTestTime.Add(48 * time.Hour),
			appConfig: &config.AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			sourceUsersSetUp: sourceUsers(aliceSource, carolSource),
			ytUsersSetUp: []internalyt.YtsaurusUser{
				aliceYT,
				bannedUser(bobYT, initialTestTime),
				bannedUser(carolYT, initialTestTime.Add(40*time.Hour)),
			},
			ytUsersExpected: []internalyt.YtsaurusUser{aliceYT, carolYT},
		},
		{
			name: "remove-limit-users-3",
			appConfig: &config.AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			ytUsersSetUp:    []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytUsersExpected: []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
		},
		{
			name: "remove-limit-groups-3",
			appConfig: &config.AppConfig{
				UsernameReplacements:  defaultUsernameReplacements,
				GroupnameReplacements: defaultGroupnameReplacements,
				RemoveLimit:           3,
			},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				internalyt.NewEmptyYtsaurusGroupWithMembers(devsYTGroup),
				internalyt.NewEmptyYtsaurusGroupWithMembers(qaYTGroup),
				internalyt.NewEmptyYtsaurusGroupWithMembers(hqYTGroup),
			},
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				internalyt.NewEmptyYtsaurusGroupWithMembers(devsYTGroup),
				internalyt.NewEmptyYtsaurusGroupWithMembers(qaYTGroup),
				internalyt.NewEmptyYtsaurusGroupWithMembers(hqYTGroup),
			},
		},
		{
			name:             "a-changed-name-b-changed-email",
			sourceUsersSetUp: sourceUsers(aliceSourceChanged, bobSourceChanged),
			ytUsersSetUp:     []internalyt.YtsaurusUser{aliceYT, bobYT},
			ytUsersExpected:  []internalyt.YtsaurusUser{aliceYTChanged, bobYTChanged},
		},
		{
			name:             "skip-create-remove-group-no-members-change-correct-name-replace",
			sourceUsersSetUp: sourceUsers(aliceSource, bobSource, carolSource),
			ytUsersSetUp:     []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytUsersExpected:  []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroup, aliceYT.Username),
				ytsaurusGroup(qaYTGroup, bobYT.Username),
			},
			sourceGroupsSetUp: []source.SourceGroupWithMembers{
				sourceGroup(devsSourceGroup, aliceSource.ID),
				sourceGroup(hqSourceGroup, carolSource.ID),
			},
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroup, aliceYT.Username),
				ytsaurusGroup(hqYTGroup, carolYT.Username),
			},
		},
		{
			name:             "memberships-add-remove",
			sourceUsersSetUp: sourceUsers(aliceSource, bobSource, carolSource),
			ytUsersSetUp:     []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytUsersExpected:  []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroup, aliceYT.Username, bobYT.Username),
			},
			sourceGroupsSetUp: []source.SourceGroupWithMembers{
				sourceGroup(devsSourceGroup, aliceSource.ID, carolSource.ID),
			},
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroup, aliceYT.Username, carolYT.Username),
			},
		},
		{
			name:             "display-name-changes",
			sourceUsersSetUp: sourceUsers(aliceSource, bobSource, carolSource),
			ytUsersSetUp:     []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytUsersExpected:  []internalyt.YtsaurusUser{aliceYT, bobYT, carolYT},
			ytGroupsSetUp: []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroup, aliceYT.Username, bobYT.Username),
				ytsaurusGroup(hqYTGroup, aliceYT.Username, bobYT.Username),
			},
			sourceGroupsSetUp: []source.SourceGroupWithMembers{
				sourceGroup(devsSourceGroupRenamed, aliceSource.ID, carolSource.ID),
				sourceGroup(hqSourceGroupChanged, aliceSource.ID, carolSource.ID),
			},
			ytGroupsExpected: []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroupRenamed, aliceYT.Username, carolYT.Username),
				ytsaurusGroup(hqYTGroupChanged, aliceYT.Username, carolYT.Username),
			},
		},
	}
)

func TestSyncCaseInventory(t *testing.T) {
	originalCases := map[string][]string{
		"azure": {
			"a-skip-b-create-c-remove",
			"bob-is-banned",
			"bob-was-banned-now-deleted-carol-was-banned-now-back",
			"remove-limit-users-3",
			"remove-limit-groups-3",
			"a-changed-name-b-changed-email",
			"skip-create-remove-group-no-members-change-correct-name-replace",
			"memberships-add-remove",
			"display-name-changes",
		},
		"ldap": {
			"a-skip-b-create-c-remove",
			"bob-is-banned",
			"bob-was-banned-now-deleted-carol-was-banned-now-back",
			"remove-limit-users-3",
			"remove-limit-groups-3",
			"a-changed-name-b-changed-email",
			"skip-create-remove-group-no-members-change-correct-name-replace",
			"memberships-add-remove",
		},
	}

	actual := make(map[string]struct{}, len(syncTestCases))
	for _, tc := range syncTestCases {
		if _, duplicate := actual[tc.name]; duplicate {
			t.Fatalf("duplicate sync test case %q", tc.name)
		}
		actual[tc.name] = struct{}{}
	}

	for sourceName, cases := range originalCases {
		for _, caseName := range cases {
			if _, ok := actual[caseName]; !ok {
				t.Errorf("%s sync case %q was lost", sourceName, caseName)
			}
		}
	}
}

func (suite *AppTestSuite) TestSyncOnce() {
	t := suite.T()

	for _, tc := range syncTestCases {
		t.Run(tc.name, func(t *testing.T) {
			defer suite.clear()

			testTime := tc.testTime
			if testTime.IsZero() {
				testTime = initialTestTime
			}

			src := &fakeSource{
				users:  tc.sourceUsersSetUp,
				groups: tc.sourceGroupsSetUp,
			}
			setupYtsaurusObjects(t, suite.ytsaurusClient, tc.ytUsersSetUp, tc.ytGroupsSetUp)
			suite.syncOnce(t, src, testclock.NewFakePassiveClock(testTime), tc.appConfig)
			suite.check(t, tc.ytUsersExpected, tc.ytGroupsExpected)
		})
	}
}

func ytsaurusUserFromSource(user fakeUser) internalyt.YtsaurusUser {
	raw, err := user.GetRaw()
	if err != nil {
		panic(err)
	}

	name := user.Name
	for _, replacement := range defaultUsernameReplacements {
		name = strings.ReplaceAll(name, replacement.From, replacement.To)
	}
	return internalyt.YtsaurusUser{Username: strings.ToLower(name), SourceRaw: raw}
}

func ytsaurusGroupFromSource(group fakeGroup) internalyt.YtsaurusGroup {
	raw, err := group.GetRaw()
	if err != nil {
		panic(err)
	}

	name := group.Name
	for _, replacement := range defaultGroupnameReplacements {
		name = strings.ReplaceAll(name, replacement.From, replacement.To)
	}
	return internalyt.YtsaurusGroup{Name: strings.ToLower(name), SourceRaw: raw}
}

func bannedUser(user internalyt.YtsaurusUser, since time.Time) internalyt.YtsaurusUser {
	user.BannedSince = since
	return user
}

func ytsaurusGroup(group internalyt.YtsaurusGroup, members ...string) internalyt.YtsaurusGroupWithMembers {
	return internalyt.YtsaurusGroupWithMembers{
		YtsaurusGroup: group,
		Members:       NewStringSetFromItems(members...),
	}
}

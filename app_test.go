package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-ldap/ldap/v3"

	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.ytsaurus.tech/yt/go/ypath"
	"k8s.io/utils/clock"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	testclock "k8s.io/utils/clock/testing"

	"go.ytsaurus.tech/yt/go/yt"
)

const (
	aliceName               = "alice"
	bobName                 = "bob"
	carolName               = "carol"
	ytDevToken             = "password"
	reuseYtContainerEnvVar = "REUSE_YT_CONTAINER"
)

type testCase struct {
	name      string
	appConfig *AppConfig
	testTime  time.Time

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

func createLdapUser(name string) LdapUser {
	return LdapUser{
		Username:  fmt.Sprintf("%s@acme.com", name),
		UID:       getUserID(name),
		FirstName: name,
	}
}

func createUpdatedLdapUser(name string) LdapUser {
	user := createLdapUser(name)
	return LdapUser{
		Username:  user.Username,
		UID:       user.UID,
		FirstName: user.FirstName + "-updated",
	}
}

func createYtsaurusUser(name string) YtsaurusUser {
	originalUsername := fmt.Sprintf("%s@acme.com", name)
	ytUsername := originalUsername
	for _, replacement := range defaultUsernameReplacements {
		ytUsername = strings.Replace(ytUsername, replacement.From, replacement.To, -1)
	}

	return YtsaurusUser{Username: ytUsername, SourceRaw: map[string]any{
		"username":   originalUsername,
		"uid":        getUserID(name),
		"first_name": name,
	}}
}

func createUpdatedYtsaurusUser(name string) YtsaurusUser {
	user := createYtsaurusUser(name)
	user.SourceRaw["first_name"] = name + "-updated"
	return user
}

func bannedYtsaurusUser(ytUser YtsaurusUser, bannedSince time.Time) YtsaurusUser {
	return YtsaurusUser{Username: ytUser.Username, SourceRaw: ytUser.SourceRaw, BannedSince: bannedSince}
}

func createLdapGroup(name string) LdapGroup {
	name = "acme." + name
	return LdapGroup{
		Groupname: fmt.Sprintf("%v|all", name),
	}
}

func createYtsaurusGroup(name string) YtsaurusGroup {
	name = "acme." + name
	originalName := fmt.Sprintf("%v|all", name)
	ytName := originalName
	for _, replacement := range defaultGroupnameReplacements {
		ytName = strings.Replace(ytName, replacement.From, replacement.To, -1)
	}
	return YtsaurusGroup{Name: name, SourceRaw: map[string]any{
		"groupname": originalName,
	}}
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
			"identity":     "acme.devs|all",
		},
	}
	qaYtsaurusGroup = YtsaurusGroup{
		Name: "acme.qa",
		SourceRaw: map[string]any{
			"id":           "fake-az-acme.qa",
			"display_name": "acme.qa|all",
			"identity":     "acme.qa",
		},
	}
	hqYtsaurusGroup = YtsaurusGroup{
		Name: "acme.hq",
		SourceRaw: map[string]any{
			"id":           hqAzureGroup.AzureID,
			"display_name": "acme.hq",
			"identity":     "acme.hq",
		},
	}
	devsYtsaurusGroupChangedDisplayName = YtsaurusGroup{
		Name: "acme.developers",
		SourceRaw: map[string]any{
			"id":           devsAzureGroup.AzureID,
			"display_name": "acme.developers|all",
			"identity":     "acme.developers|all",
		},
	}
	hqYtsaurusGroupChangedBackwardCompatible = YtsaurusGroup{
		Name: "acme.hq",
		SourceRaw: map[string]any{
			"id":           hqAzureGroup.AzureID,
			"display_name": "acme.hq|all",
			"identity":     "acme.hq|all",
		},
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

	ldapTestCases = []testCase{
		{
			name: "a-skip-b-create-c-remove",
			sourceUsersSetUp: []SourceUser{
				createLdapUser(aliceName),
				createLdapUser(bobName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
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
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
			},
			sourceUsersSetUp: []SourceUser{
				createLdapUser(aliceName),
			},
			ytUsersExpected: []YtsaurusUser{
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
			appConfig: &AppConfig{
				UsernameReplacements:    defaultUsernameReplacements,
				GroupnameReplacements:   defaultGroupnameReplacements,
				BanBeforeRemoveDuration: 24 * time.Hour,
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				bannedYtsaurusUser(createYtsaurusUser(bobName), initialTestTime),
				bannedYtsaurusUser(createYtsaurusUser(carolName), initialTestTime.Add(40*time.Hour)),
			},
			sourceUsersSetUp: []SourceUser{
				createLdapUser(aliceName),
				createLdapUser(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(carolName),
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
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			// No one is deleted: limitation works.
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
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
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("devs")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("qa")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("hq")),
			},
			// No group is deleted: limitation works.
			ytGroupsExpected: []YtsaurusGroupWithMembers{
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("devs")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("qa")),
				NewEmptyYtsaurusGroupWithMembers(createYtsaurusGroup("hq")),
			},
		},
		{
			name: "a-changed-name-b-changed-email",
			sourceUsersSetUp: []SourceUser{
				createUpdatedLdapUser(aliceName),
				createUpdatedLdapUser(bobName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
			},
			ytUsersExpected: []YtsaurusUser{
				createUpdatedYtsaurusUser(aliceName),
				createUpdatedYtsaurusUser(bobName),
			},
		},
		{
			name: "skip-create-remove-group-no-members-change-correct-name-replace",
			sourceUsersSetUp: []SourceUser{
				createLdapUser(aliceName),
				createLdapUser(bobName),
				createLdapUser(carolName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroup("devs"),
					Members:       NewStringSetFromItems(aliceName),
				},
				{
					YtsaurusGroup: createYtsaurusGroup("qa"),
					Members:       NewStringSetFromItems(bobName),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createLdapGroup("devs"),
					Members:     NewStringSetFromItems(getUserID(aliceName)),
				},
				{
					SourceGroup: createLdapGroup("hq"),
					Members:     NewStringSetFromItems(getUserID(carolName)),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
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
			sourceUsersSetUp: []SourceUser{
				createLdapUser(aliceName),
				createLdapUser(bobName),
				createLdapUser(carolName),
			},
			ytUsersSetUp: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytUsersExpected: []YtsaurusUser{
				createYtsaurusUser(aliceName),
				createYtsaurusUser(bobName),
				createYtsaurusUser(carolName),
			},
			ytGroupsSetUp: []YtsaurusGroupWithMembers{
				{
					YtsaurusGroup: createYtsaurusGroup("devs"),
					Members: NewStringSetFromItems(
						aliceName,
						bobName,
					),
				},
			},
			sourceGroupsSetUp: []SourceGroupWithMembers{
				{
					SourceGroup: createLdapGroup("devs"),
					Members: NewStringSetFromItems(
						getUserID(aliceName),
						getUserID(carolName),
					),
				},
			},
			ytGroupsExpected: []YtsaurusGroupWithMembers{
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

type AppTestSuite struct {
	suite.Suite
	ytsaurusLocal             *YtsaurusLocal
	ytsaurusClient            yt.Client
	initialYtsaurusUsers      []YtsaurusUser
	initialYtsaurusGroups     []YtsaurusGroupWithMembers
	initialYtsaurusUsernames  []string
	initialYtsaurusGroupnames []string
	ctx                       context.Context
}

func (suite *AppTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	suite.ytsaurusLocal = NewYtsaurusLocal()

	if err := suite.ytsaurusLocal.Start(); err != nil {
		log.Fatalf("error starting ytsaurus local container: %s", err)
	}

	err := os.Setenv(defaultYtsaurusSecretEnvVar, ytDevToken)
	if err != nil {
		log.Fatalf("failed to set YT_TOKEN: %s", err)
	}

	ytsaurusClient, err := suite.ytsaurusLocal.GetClient()
	if err != nil {
		log.Fatalf("error creating ytsaurus local client: %s", err)
	}

	suite.ytsaurusClient = ytsaurusClient

	suite.initialYtsaurusUsers, suite.initialYtsaurusGroups, err = suite.getAllYtsaurusObjects()
	if err != nil {
		log.Fatalf("error getting initial ytsaurus objects: %s", err)
	}

	for _, user := range suite.initialYtsaurusUsers {
		suite.initialYtsaurusUsernames = append(suite.initialYtsaurusUsernames, user.Username)
	}

	for _, group := range suite.initialYtsaurusGroups {
		suite.initialYtsaurusGroupnames = append(suite.initialYtsaurusGroupnames, group.Name)
	}
}

func (suite *AppTestSuite) TearDownSuite() {
	if err := suite.ytsaurusLocal.Stop(); err != nil {
		log.Fatalf("error terminating ytsaurus local container: %s", err)
	}
}

func (suite *AppTestSuite) restartYtsaurusIfNeeded() {
	if os.Getenv(reuseYtContainerEnvVar) != "1" && os.Getenv(reuseYtContainerEnvVar) != "yes" {
		suite.TearDownSuite()
		suite.SetupSuite()
	}
}

func (suite *AppTestSuite) getAllYtsaurusObjects() (users []YtsaurusUser, groups []YtsaurusGroupWithMembers, err error) {
	allUsers, err := doGetAllYtsaurusUsers(context.Background(), suite.ytsaurusClient, "azure")
	if err != nil {
		return nil, nil, err
	}
	allGroups, err := doGetAllYtsaurusGroupsWithMembers(context.Background(), suite.ytsaurusClient, "azure")
	return allUsers, allGroups, err
}

func (suite *AppTestSuite) diffYtsaurusObjects(expectedUsers []YtsaurusUser, expectedGroups []YtsaurusGroupWithMembers) (string, string) {
	actualUsers, actualGroups, err := suite.getAllYtsaurusObjects()
	if err != nil {
		log.Fatalf("failed to get all ytsaurus objects: %s", err)
	}
	allExpectedUsers := append(suite.initialYtsaurusUsers, expectedUsers...)

	// It seems that `users` group @members attr contains not the all users in the system:
	// for example it doesn't include:
	// alien_cell_synchronizer, file_cache, guest, operations_cleaner, operations_client, etc...
	// we don't want to test that.
	// Though we expect it to include users created in test, so we update group members in out expected group list.
	var expectedNewUsernamesInUsersGroup []string
	for _, u := range expectedUsers {
		expectedNewUsernamesInUsersGroup = append(expectedNewUsernamesInUsersGroup, u.Username)
	}

	var allExpectedGroups []YtsaurusGroupWithMembers
	for _, initialGroup := range suite.initialYtsaurusGroups {
		group := YtsaurusGroupWithMembers{YtsaurusGroup: initialGroup.YtsaurusGroup, Members: NewStringSet()}
		if initialGroup.Name != "users" {
			group.Members = initialGroup.Members
		} else {
			for member := range initialGroup.Members.Iter() {
				group.Members.Add(member)
			}
			for _, uname := range expectedNewUsernamesInUsersGroup {
				group.Members.Add(uname)
			}
		}
		allExpectedGroups = append(allExpectedGroups, group)
	}
	allExpectedGroups = append(allExpectedGroups, expectedGroups...)

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

func (suite *AppTestSuite) clear() {
	users, groups, err := suite.getAllYtsaurusObjects()
	if err != nil {
		log.Fatalf("failed to get ytsaurus objects: %s", err)
	}

	for _, user := range users {
		if !slices.Contains(suite.initialYtsaurusUsernames, user.Username) {
			path := ypath.Path(fmt.Sprintf("//sys/users/%s", user.Username))
			err := suite.ytsaurusClient.RemoveNode(suite.ctx, path, nil)
			if err != nil {
				log.Fatalf("failed to remove user: %s", user.Username)
			}

			exists := true
			for exists {
				exists, err = suite.ytsaurusClient.NodeExists(suite.ctx, path, nil)
				if err != nil {
					log.Fatalf("failed to check is group removed")
				}
			}
		}
	}

	for _, group := range groups {
		if !slices.Contains(suite.initialYtsaurusGroupnames, group.Name) {
			path := ypath.Path(fmt.Sprintf("//sys/groups/%s", group.Name))
			err := suite.ytsaurusClient.RemoveNode(suite.ctx, path, nil)
			if err != nil {
				log.Fatalf("failed to remove group: %s", err)
			}
			exists := true
			for exists {
				exists, err = suite.ytsaurusClient.NodeExists(suite.ctx, path, nil)
				if err != nil {
					log.Fatalf("failed to check is group removed")
				}
			}
		}
	}

	suite.restartYtsaurusIfNeeded()
}

func (suite *AppTestSuite) syncOnce(t *testing.T, source Source, clock clock.PassiveClock, appConfig *AppConfig) {
	if appConfig == nil {
		appConfig = defaultAppConfig
	}

	app, err := NewAppCustomized(
		&Config{
			App:   *appConfig,
			Azure: &AzureConfig{},
			Ytsaurus: YtsaurusConfig{
				Proxy:               suite.ytsaurusLocal.GetProxy(),
				ApplyUserChanges:    true,
				ApplyGroupChanges:   true,
				ApplyMemberChanges:  true,
				LogLevel:            "DEBUG",
				SourceAttributeName: "azure",
			},
		}, getDevelopmentLogger(),
		source,
		clock,
	)
	require.NoError(t, err)

	app.syncOnce()
}

func (suite *AppTestSuite) check(t *testing.T, expectedUsers []YtsaurusUser, expectedGroups []YtsaurusGroupWithMembers) {
	// We have eventually here, because user removal takes some time.
	require.Eventually(
		t,
		func() bool {
			udiff, gdiff := suite.diffYtsaurusObjects(expectedUsers, expectedGroups)
			actualUsers, actualGroups, err := suite.getAllYtsaurusObjects()
			if err != nil {
				log.Fatalf("failed to get all ytsaurus objects: %s", err)
			}
			if udiff != "" {
				t.Log("Users diff is not empty yet:", udiff)
				t.Log("expected users", expectedUsers)
				t.Log("actual users", actualUsers)
			}
			if gdiff != "" {
				t.Log("Groups diff is not empty yet:", gdiff)
				t.Log("expected groups", expectedGroups)
				t.Log("actual groups", actualGroups)
			}
			return udiff == "" && gdiff == ""
		},
		3*time.Second,
		300*time.Millisecond,
	)
}

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
					ldapSource, err := NewLdap(ldapConfig, getDevelopmentLogger())
					require.NoError(t, err)

					setupLdapObjects(t, ldapSource.connection, tc.sourceUsersSetUp, tc.sourceGroupsSetUp)

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

func (suite *AppTestSuite) TestManageUnmanagedUsersIsForbidden() {
	t := suite.T()

	defer suite.clear()

	ytsaurus, err := NewYtsaurus(
		&YtsaurusConfig{
			Proxy:    suite.ytsaurusLocal.GetProxy(),
			LogLevel: "DEBUG",
		},
		getDevelopmentLogger(),
		testclock.NewFakePassiveClock(time.Now()),
	)
	require.NoError(t, err)

	unmanagedOleg := "oleg"

	err = doCreateYtsaurusUser(
		context.Background(),
		suite.ytsaurusClient,
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
			ytsaurus.UpdateUser(username, YtsaurusUser{Username: username, SourceRaw: map[string]any{
				"email": "dummy@acme.com",
			}}),
			"Prevented attempt to change manual managed user",
		)
	}
}

func TestAppTestSuite(t *testing.T) {
	suite.Run(t, new(AppTestSuite))
}

func setupYtsaurusObjects(t *testing.T, client yt.Client, users []YtsaurusUser, groups []YtsaurusGroupWithMembers) {
	t.Log("Setting up ytsaurus for test")

	for _, user := range users {
		t.Logf("creating user: %v", user)

		userAttributes := buildUserAttributes(user, "azure")
		err := doCreateYtsaurusUser(
			context.Background(),
			client,
			user.Username,
			userAttributes,
		)
		require.NoError(t, err)
	}

	for _, group := range groups {
		t.Log("creating group:", group)

		groupAttributes := buildGroupAttributes(group.YtsaurusGroup, "azure")
		err := doCreateYtsaurusGroup(
			context.Background(),
			client,
			group.Name,
			groupAttributes,
		)

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

func parseAppTime(timStr string) time.Time {
	parsed, err := time.Parse(appTimeFormat, timStr)
	if err != nil {
		panic("parsing " + timStr + " failed")
	}
	return parsed
}

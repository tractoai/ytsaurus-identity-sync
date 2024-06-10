//go:build integration

package main

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/stretchr/testify/require"
)

//go:embed config.local.yaml
var _localConfig embed.FS

// TestPrintAzureUsersIntegration tests nothing, but can be used to debug Azure users retrieved from ms graph api.
// In particular, it can be used to tune userFilter for production use.
// It requires AZURE_CLIENT_SECRET env var and `config.local.yaml` file (which is .gitignored).
func TestPrintAzureUsersIntegration(t *testing.T) {
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	logger, err := configureLogger(&cfg.Logging)
	require.NoError(t, err)
	azure, err := NewAzureReal(cfg.Azure, logger)
	require.NoError(t, err)

	fieldsToSelect := append(
		defaultUserFieldsToSelect,
		"jobTitle",
		"userType",
		"accountEnabled",
	)
	//filter := ""
	filter := cfg.Azure.UsersFilter
	usersRaw, err := azure.getUsersRaw(context.Background(), fieldsToSelect, filter)
	require.NoError(t, err)

	t.Log("got", len(usersRaw), "users")
	for _, user := range usersRaw {
		if handleNil(user.GetGivenName()) != "" {
			continue
		}
		t.Log(
			strings.Join(
				[]string{
					"id",
					"mail",
					"job title",
					"given name",
					"surname",
					"accountEnabled",
					"userType",
				},
				"|",
			),
		)
		enabled := "false"
		if handleNil(user.GetAccountEnabled()) {
			enabled = "true"
		}

		t.Log(
			strings.Join(
				[]string{
					handleNil(user.GetId()),
					handleNil(user.GetMail()),
					handleNil(user.GetJobTitle()),
					handleNil(user.GetGivenName()),
					handleNil(user.GetSurname()),
					enabled,
					handleNil(user.GetUserType()),
				},
				"|",
			),
		)
	}
	require.NotEmpty(t, usersRaw)
}

// TestPrintAzureUsersDiffIntegration can be used to tune user filter by reviewing a diff ao users for two filters.
func TestPrintAzureUsersDiffIntegration(t *testing.T) {
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	logger, err := configureLogger(&cfg.Logging)
	require.NoError(t, err)
	azure, err := NewAzureReal(cfg.Azure, logger)
	require.NoError(t, err)

	fieldsToSelect := append(
		defaultUserFieldsToSelect,
		"jobTitle",
		"userType",
		"accountEnabled",
	)
	filterFromConfig := cfg.Azure.UsersFilter
	filterToDiff := `
		(accountEnabled eq true) and (userType eq 'Member')
		and not (jobTitle in ('Shared mailbox', 'Contractor'))
	`
	usersRawForConfig, err := azure.getUsersRaw(context.Background(), fieldsToSelect, filterFromConfig)
	require.NoError(t, err)
	usersRawForDiff, err := azure.getUsersRaw(context.Background(), fieldsToSelect, filterToDiff)
	require.NoError(t, err)

	usersBefore := rawUsersToTestUsers(usersRawForConfig)
	usersAfter := rawUsersToTestUsers(usersRawForDiff)

	t.Log("New users:")
	for id, user := range usersAfter {
		if _, existed := usersBefore[id]; !existed {
			t.Logf("%+v", user)
		}
	}
	t.Log("Removed users:")
	for id, user := range usersBefore {
		if _, exists := usersAfter[id]; !exists {
			t.Logf("%+v", user)
		}
	}
}

type testUser struct {
	Id             string
	PrincipalName  string
	Mail           string
	FirstName      string
	LastName       string
	DisplayName    string
	JobTitle       string
	UserType       string
	AccountEnabled bool
}

func rawUsersToTestUsers(usersRaw []models.Userable) map[string]testUser {
	users := make(map[string]testUser)
	for _, user := range usersRaw {
		testu := testUser{
			Id:             handleNil(user.GetId()),
			PrincipalName:  handleNil(user.GetUserPrincipalName()),
			Mail:           handleNil(user.GetMail()),
			FirstName:      handleNil(user.GetGivenName()),
			LastName:       handleNil(user.GetSurname()),
			DisplayName:    handleNil(user.GetDisplayName()),
			JobTitle:       handleNil(user.GetJobTitle()),
			UserType:       handleNil(user.GetUserType()),
			AccountEnabled: handleNil(user.GetAccountEnabled()),
		}
		users[testu.Id] = testu
	}
	return users
}

// TestPrintAzureGroupsIntegration tests nothing, but can be used to debug Azure groups retrieved from ms graph api.
// In particular, it can be used to tune groupsFilter for production use.
// It requires AZURE_CLIENT_SECRET env var and `config.local.yaml` file (which is .gitignored).
func TestPrintAzureGroupsIntegrationRaw(t *testing.T) {
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	logger, err := configureLogger(&cfg.Logging)
	require.NoError(t, err)
	azure, err := NewAzureReal(cfg.Azure, logger)
	require.NoError(t, err)

	filter := cfg.Azure.GroupsFilter
	groupsRaw, err := azure.getGroupsWithMembersRaw(context.Background(), defaultGroupFieldsToSelect, filter)
	require.NoError(t, err)

	t.Log("got", len(groupsRaw), "groups")
	for _, group := range groupsRaw {
		displayName := handleNil(group.GetDisplayName())
		if displayName == "" {
			continue
		}
		if !strings.HasSuffix(displayName, "|all") {
			continue
		}
		t.Log(
			strings.Join(
				[]string{
					"id",
					"displayName",
					"members",
				},
				"|",
			),
		)
		members := group.GetMembers()
		membersList := ""
		for _, member := range members {
			membersList += handleNil(member.GetId())
		}
		t.Log(
			strings.Join(
				[]string{
					handleNil(group.GetId()),
					displayName,
					membersList,
				},
				"|",
			),
		)
	}
	require.NotEmpty(t, groupsRaw)
}

func TestPrintAzureGroupsIntegration(t *testing.T) {
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	logger, err := configureLogger(&cfg.Logging)
	require.NoError(t, err)
	azure, err := NewAzureReal(cfg.Azure, logger)
	require.NoError(t, err)

	groups, err := azure.GetGroupsWithMembers()
	require.NoError(t, err)

	t.Log("got", len(groups), "groups")
	t.Log(
		strings.Join(
			[]string{
				"id",
				"name",
			},
			"|",
		),
	)
	for _, group := range groups {
		t.Log(
			strings.Join(
				[]string{
					group.SourceGroup.GetName(),
					group.SourceGroup.GetID(),
				},
				"|",
			),
		)
		for member := range group.Members.Iter() {
			t.Log("\t ", member)
		}
	}
	require.NotEmpty(t, groups)
}

// TestDiffAzureGroups is a script that can check difference between
// collected groups for two configurations.
func TestDiffAzureGroupsNames(t *testing.T) {
	cfgBefore, err := loadConfig("config.local.yaml")
	require.NoError(t, err)
	logger, err := configureLogger(&cfgBefore.Logging)
	require.NoError(t, err)

	azureBefore, err := NewAzureReal(cfgBefore.Azure, logger)
	require.NoError(t, err)
	groupsBefore, err := azureBefore.GetGroupsWithMembers()
	require.NoError(t, err)
	var groupnamesBefore []string
	for _, gr := range groupsBefore {
		groupnamesBefore = append(groupnamesBefore, gr.SourceGroup.GetName())
	}

	cfgAfter, err := loadConfig("config.local.yaml")
	cfgAfter.Azure.GroupsDisplayNameRegexPostFilter = `\|.+$`
	require.NoError(t, err)

	azureAfter, err := NewAzureReal(cfgAfter.Azure, logger)
	require.NoError(t, err)
	groupsAfter, err := azureAfter.GetGroupsWithMembers()
	require.NoError(t, err)
	var groupnamesAfter []string
	for _, gr := range groupsAfter {
		groupnamesAfter = append(groupnamesAfter, gr.SourceGroup.GetName())
	}

	diff := NewStringSetFromItems(groupnamesAfter...).SymmetricDifference(NewStringSetFromItems(groupnamesBefore...))
	fmt.Println(len(diff.ToSlice()), diff)
	strDiff := cmp.Diff(groupnamesBefore, groupnamesAfter, cmpopts.SortSlices(func(a, b string) bool {
		return a < b
	}))
	require.Empty(t, strDiff)
}

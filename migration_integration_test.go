//go:build integration

package main

import (
	"context"
	"embed"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
)

//go:embed config.local.yaml
var prodConfig embed.FS

// TestUsersMigration is not really a test, but a migration tool
// It finds users that can't be created in YTsaurus because they already exist
// and sets correct @azure attribute for them
func TestUsersMigration(t *testing.T) {
	dryRun := true
	t.Skip("One-time launch assumed")
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	logger, err := configureLogger(cfg.Logging)
	require.NoError(t, err)
	app, err := NewApp(cfg, logger)
	require.NoError(t, err)
	azure, err := NewAzureReal(cfg.Azure, logger)
	require.NoError(t, err)
	yt, err := NewYtsaurus(cfg.Ytsaurus, logger, clock.RealClock{})
	require.NoError(t, err)

	azureUsers, err := azure.GetUsers()
	require.NoError(t, err)
	t.Log("Got", len(azureUsers), "Azure users")

	ytUsers, err := doGetAllYtsaurusUsers(context.Background(), yt.client)
	require.NoError(t, err)
	t.Log("Got", len(ytUsers), "raw YTsaurus users")

	manuallyCreatedUsers := make(map[string]YtsaurusUser)
	for _, user := range ytUsers {
		if user.IsManuallyManaged() {
			manuallyCreatedUsers[user.Username] = user
		}
	}
	t.Log("Got", len(manuallyCreatedUsers), "manually create YTsaurus users")

	ytUsersToUpdate := make(map[string]YtsaurusUser)
	for _, azureUser := range azureUsers {
		convertedUser := app.buildYtsaurusUser(azureUser)
		if _, match := manuallyCreatedUsers[convertedUser.Username]; !match {
			continue
		}
		ytUsersToUpdate[convertedUser.Username] = convertedUser
		t.Log("Need update user", convertedUser.Username)
	}

	t.Log("Got", len(ytUsersToUpdate), "users to update")

	for username, user := range ytUsersToUpdate {
		attrValue := user.SourceUser

		if dryRun {
			t.Log("[DRY-RUN] will set @azure=", attrValue, "for", username)
			continue
		}

		t.Log("will set @azure=", attrValue, "for", username)
		err = doSetAzureAttributeForYtsaurusUser(
			context.Background(),
			yt.client,
			username,
			"azure",
			attrValue,
		)
		require.NoError(t, err)
	}
}

// TestGroupsMigration is not really a test, but a migration tool
// It finds groups that can't be created in YTsaurus because they already exist
// and sets correct @azure attribute for them
func TestGroupsMigration(t *testing.T) {
	dryRun := true
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	logger, err := configureLogger(cfg.Logging)
	require.NoError(t, err)
	app, err := NewApp(cfg, logger)
	require.NoError(t, err)
	azure, err := NewAzureReal(cfg.Azure, logger)
	require.NoError(t, err)
	yt, err := NewYtsaurus(cfg.Ytsaurus, logger, clock.RealClock{})
	require.NoError(t, err)

	azureGroups, err := azure.GetGroupsWithMembers()
	require.NoError(t, err)
	t.Log("Got", len(azureGroups), "Azure groups")

	ytGroups, err := doGetAllYtsaurusGroupsWithMembers(context.Background(), yt.client)
	require.NoError(t, err)
	t.Log("Got", len(ytGroups), "raw YTsaurus groups")

	manuallyCreatedGroups := make(map[string]YtsaurusGroupWithMembers)
	for _, group := range ytGroups {
		if group.IsManuallyManaged() {
			manuallyCreatedGroups[group.Name] = group
		}
	}
	t.Log("Got", len(manuallyCreatedGroups), "manually create YTsaurus groups")

	ytGroupsToUpdate := make(map[string]YtsaurusGroup)
	for _, azureGroup := range azureGroups {
		convertedGroup := app.buildYtsaurusGroup(azureGroup.AzureGroup)
		if _, match := manuallyCreatedGroups[convertedGroup.Name]; !match {
			continue
		}
		ytGroupsToUpdate[convertedGroup.Name] = convertedGroup
		t.Log("Need update group", convertedGroup.Name)
	}

	t.Log("Got", len(ytGroupsToUpdate), "groups to update")

	for groupname, group := range ytGroupsToUpdate {
		attrValue := group
		if dryRun {
			t.Log("[DRY-RUN] will set @azure=", attrValue, "for", groupname)
			continue
		}

		t.Log("will set @azure=", attrValue, "for", groupname)
		err = doSetAzureAttributeForYtsaurusGroup(
			context.Background(),
			yt.client,
			groupname,
			"azure",
			attrValue,
		)
		require.NoError(t, err)

	}
}

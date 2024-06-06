package main

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	ytcontainer "github.com/tractoai/testcontainers-ytsaurus"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"k8s.io/utils/clock"
	testclock "k8s.io/utils/clock/testing"
)

const (
	aliceName              = "alice"
	bobName                = "bob"
	carolName              = "carol"
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
	h := fnv.New32a()
	_, err := h.Write([]byte(name))
	if err != nil {
		log.Fatalf("failed to generate user id: %v", err)
	}
	return fmt.Sprintf("%v", h.Sum32())
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

type AppTestSuite struct {
	suite.Suite
	ytsaurusLocal             *ytcontainer.YTsaurusContainer
	ytsaurusClient            yt.Client
	initialYtsaurusUsers      []YtsaurusUser
	initialYtsaurusGroups     []YtsaurusGroupWithMembers
	initialYtsaurusUsernames  []string
	initialYtsaurusGroupnames []string
	ctx                       context.Context
}

func (suite *AppTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	var err error

	if suite.ytsaurusLocal, err = ytcontainer.RunContainer(suite.ctx); err != nil {
		log.Fatalf("error starting ytsaurus local container: %s", err)
	}

	err = os.Setenv(defaultYtsaurusSecretEnvVar, ytDevToken)
	if err != nil {
		log.Fatalf("failed to set YT_TOKEN: %s", err)
	}

	ytsaurusClient, err := suite.ytsaurusLocal.NewClient(suite.ctx)
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
	if err := suite.ytsaurusLocal.Terminate(suite.ctx); err != nil {
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

	proxy, err := suite.ytsaurusLocal.ConnectionHost(suite.ctx)
	require.NoError(t, err)
	app, err := NewAppCustomized(
		&Config{
			App:   *appConfig,
			Azure: &AzureConfig{},
			Ytsaurus: YtsaurusConfig{
				Proxy:               proxy,
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

func (suite *AppTestSuite) TestManageUnmanagedUsersIsForbidden() {
	t := suite.T()

	defer suite.clear()

	proxy, err := suite.ytsaurusLocal.ConnectionHost(suite.ctx)
	ytsaurus, err := NewYtsaurus(
		&YtsaurusConfig{
			Proxy:    proxy,
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

func parseAppTime(timStr string) time.Time {
	parsed, err := time.Parse(appTimeFormat, timStr)
	if err != nil {
		panic("parsing " + timStr + " failed")
	}
	return parsed
}

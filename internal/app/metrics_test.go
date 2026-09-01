package app

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
)

type recordingMetrics struct {
	syncStarted        int
	syncSucceeded      int
	syncPartial        int
	syncDurations      []time.Duration
	lastSuccess        time.Time
	lastCompleted      time.Time
	syncErrors         map[string]int
	ytMutationSuccess  map[string]int
	ytMutationErrors   map[string]int
	removeLimitReached map[string]int
}

type metricsYtsaurus struct {
	Ytsaurus
	users           []internalyt.YtsaurusUser
	groups          []internalyt.YtsaurusGroupWithMembers
	getUsersErr     error
	getGroupsErr    error
	createUserErr   error
	updateUserErr   error
	removeUserErr   error
	banUserErr      error
	createGroupErr  error
	updateGroupErr  error
	removeGroupErr  error
	addMemberErr    error
	removeMemberErr error
}

func (y *metricsYtsaurus) GetUsers() ([]internalyt.YtsaurusUser, error) {
	return y.users, y.getUsersErr
}

func (y *metricsYtsaurus) CreateUser(internalyt.YtsaurusUser) error {
	return y.createUserErr
}

func (y *metricsYtsaurus) UpdateUser(string, internalyt.YtsaurusUser) error {
	return y.updateUserErr
}

func (y *metricsYtsaurus) RemoveUser(string) error {
	return y.removeUserErr
}

func (y *metricsYtsaurus) BanUser(string) error {
	return y.banUserErr
}

func (y *metricsYtsaurus) GetGroupsWithMembers() ([]internalyt.YtsaurusGroupWithMembers, error) {
	return y.groups, y.getGroupsErr
}

func (y *metricsYtsaurus) CreateGroup(internalyt.YtsaurusGroup) error {
	return y.createGroupErr
}

func (y *metricsYtsaurus) UpdateGroup(string, internalyt.YtsaurusGroup) error {
	return y.updateGroupErr
}

func (y *metricsYtsaurus) RemoveGroup(string) error {
	return y.removeGroupErr
}

func (y *metricsYtsaurus) AddMember(string, string) error {
	return y.addMemberErr
}

func (y *metricsYtsaurus) RemoveMember(string, string) error {
	return y.removeMemberErr
}

func newRecordingMetrics() *recordingMetrics {
	return &recordingMetrics{
		syncErrors:         make(map[string]int),
		ytMutationSuccess:  make(map[string]int),
		ytMutationErrors:   make(map[string]int),
		removeLimitReached: make(map[string]int),
	}
}

func (m *recordingMetrics) SyncStarted() {
	m.syncStarted++
}

func (m *recordingMetrics) SyncSucceeded() {
	m.syncSucceeded++
}

func (m *recordingMetrics) SyncPartiallySucceeded() {
	m.syncPartial++
}

func (m *recordingMetrics) ObserveSyncDuration(duration time.Duration) {
	m.syncDurations = append(m.syncDurations, duration)
}

func (m *recordingMetrics) SetLastSuccessTimestamp(timestamp time.Time) {
	m.lastSuccess = timestamp
}

func (m *recordingMetrics) SetLastCompletedTimestamp(timestamp time.Time) {
	m.lastCompleted = timestamp
}

func (m *recordingMetrics) SyncFailed(stage string) {
	m.syncErrors[stage]++
}

func (m *recordingMetrics) YTMutationSucceeded(objectType, operation string) {
	m.ytMutationSuccess[objectType+"/"+operation]++
}

func (m *recordingMetrics) YTMutationFailed(objectType, operation string) {
	m.ytMutationErrors[objectType+"/"+operation]++
}

func (m *recordingMetrics) ResetYTMutationErrors() {
	clear(m.ytMutationErrors)
}

func (m *recordingMetrics) RemoveLimitExceeded(objectType string) {
	m.removeLimitReached[objectType]++
}

func (y *emptyYtsaurus) GetGroupsWithMembers() ([]internalyt.YtsaurusGroupWithMembers, error) {
	return nil, nil
}

func TestSuccessfulSyncMetrics(t *testing.T) {
	metrics := newRecordingMetrics()
	metrics.ytMutationErrors[objectTypeUser+"/"+mutationCreate] = 1
	application := &App{
		logger:   logging.NewDevelopment(),
		source:   &failingSource{},
		ytsaurus: &emptyYtsaurus{},
		metrics:  metrics,
	}

	require.NoError(t, application.syncOnce())
	require.Equal(t, 1, metrics.syncStarted)
	require.Equal(t, 1, metrics.syncSucceeded)
	require.Zero(t, metrics.syncPartial)
	require.Len(t, metrics.syncDurations, 1)
	require.False(t, metrics.lastSuccess.IsZero())
	require.Equal(t, metrics.lastSuccess, metrics.lastCompleted)
	require.Empty(t, metrics.syncErrors)
	require.Empty(t, metrics.ytMutationErrors)
}

func TestYTMutationSuccessMetrics(t *testing.T) {
	t.Run("users", func(t *testing.T) {
		metrics := newRecordingMetrics()
		application := &App{
			logger: logging.NewDevelopment(),
			source: &fakeSource{
				users: sourceUsers(
					fakeUser{ID: "new-user", Name: "new-user"},
					fakeUser{ID: "updated-user", Name: "new-name"},
				),
			},
			ytsaurus: &metricsYtsaurus{
				users: []internalyt.YtsaurusUser{testYtsaurusUserWithID("updated-user", "old-name")},
			},
			metrics: metrics,
		}

		_, err := application.syncUsers()
		require.NoError(t, err)
		require.Equal(t, map[string]int{
			objectTypeUser + "/" + mutationCreate: 1,
			objectTypeUser + "/" + mutationUpdate: 1,
		}, metrics.ytMutationSuccess)
		require.Empty(t, metrics.ytMutationErrors)
	})

	t.Run("user ban and remove", func(t *testing.T) {
		metrics := newRecordingMetrics()
		application := &App{
			logger:   logging.NewDevelopment(),
			ytsaurus: &metricsYtsaurus{},
			metrics:  metrics,
		}

		_, removed, err := application.banOrRemoveUser(testYtsaurusUser("removed-user"))
		require.NoError(t, err)
		require.True(t, removed)

		application.banDuration = time.Hour
		banned, _, err := application.banOrRemoveUser(testYtsaurusUser("banned-user"))
		require.NoError(t, err)
		require.True(t, banned)

		require.Equal(t, map[string]int{
			objectTypeUser + "/" + mutationRemove: 1,
			objectTypeUser + "/" + mutationBan:    1,
		}, metrics.ytMutationSuccess)
		require.Empty(t, metrics.ytMutationErrors)
	})

	t.Run("groups and memberships", func(t *testing.T) {
		metrics := newRecordingMetrics()
		application := &App{
			logger: logging.NewDevelopment(),
			source: &fakeSource{
				groups: []source.SourceGroupWithMembers{
					sourceGroup(fakeGroup{ID: "new-group", Name: "new-group"}),
					sourceGroup(fakeGroup{ID: "updated-group", Name: "new-name"}),
					sourceGroup(fakeGroup{ID: "membership-group", Name: "membership-group"}, "added-user"),
				},
			},
			ytsaurus: &metricsYtsaurus{
				groups: []internalyt.YtsaurusGroupWithMembers{
					testYtsaurusGroup("updated-group", "old-name"),
					testYtsaurusGroup("removed-group", "removed-group"),
					testYtsaurusGroupWithMembers("membership-group", "membership-group", "removed-user"),
				},
			},
			metrics: metrics,
		}
		users := map[source.ObjectID]internalyt.YtsaurusUser{
			"added-user": testYtsaurusUser("added-user"),
		}

		err := application.syncGroups(users)
		require.NoError(t, err)
		require.Equal(t, map[string]int{
			objectTypeGroup + "/" + mutationCreate:      1,
			objectTypeGroup + "/" + mutationUpdate:      1,
			objectTypeGroup + "/" + mutationRemove:      1,
			objectTypeMembership + "/" + mutationAdd:    1,
			objectTypeMembership + "/" + mutationRemove: 1,
		}, metrics.ytMutationSuccess)
		require.Empty(t, metrics.ytMutationErrors)
	})
}

func TestYTMutationErrorMetrics(t *testing.T) {
	expectedErr := errors.New("YT mutation failed")

	testCases := []struct {
		name              string
		source            *fakeSource
		ytsaurus          *metricsYtsaurus
		banDuration       time.Duration
		expectedOperation string
	}{
		{
			name: "create user",
			source: &fakeSource{
				users: sourceUsers(fakeUser{ID: "user", Name: "user"}),
			},
			ytsaurus:          &metricsYtsaurus{createUserErr: expectedErr},
			expectedOperation: objectTypeUser + "/" + mutationCreate,
		},
		{
			name:              "ban user",
			source:            &fakeSource{},
			ytsaurus:          &metricsYtsaurus{users: []internalyt.YtsaurusUser{testYtsaurusUser("user")}, banUserErr: expectedErr},
			banDuration:       time.Hour,
			expectedOperation: objectTypeUser + "/" + mutationBan,
		},
		{
			name: "create group",
			source: &fakeSource{
				groups: []source.SourceGroupWithMembers{sourceGroup(fakeGroup{ID: "group", Name: "group"})},
			},
			ytsaurus:          &metricsYtsaurus{createGroupErr: expectedErr},
			expectedOperation: objectTypeGroup + "/" + mutationCreate,
		},
		{
			name: "update group",
			source: &fakeSource{
				groups: []source.SourceGroupWithMembers{sourceGroup(fakeGroup{ID: "group", Name: "new-group"})},
			},
			ytsaurus: &metricsYtsaurus{
				groups:         []internalyt.YtsaurusGroupWithMembers{testYtsaurusGroup("group", "old-group")},
				updateGroupErr: expectedErr,
			},
			expectedOperation: objectTypeGroup + "/" + mutationUpdate,
		},
		{
			name: "add membership",
			source: &fakeSource{
				users:  sourceUsers(fakeUser{ID: "user", Name: "user"}),
				groups: []source.SourceGroupWithMembers{sourceGroup(fakeGroup{ID: "group", Name: "group"}, "user")},
			},
			ytsaurus: &metricsYtsaurus{
				users:        []internalyt.YtsaurusUser{testYtsaurusUser("user")},
				groups:       []internalyt.YtsaurusGroupWithMembers{testYtsaurusGroup("group", "group")},
				addMemberErr: expectedErr,
			},
			expectedOperation: objectTypeMembership + "/" + mutationAdd,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := newRecordingMetrics()
			application := &App{
				logger:      logging.NewDevelopment(),
				source:      testCase.source,
				ytsaurus:    testCase.ytsaurus,
				banDuration: testCase.banDuration,
				metrics:     metrics,
			}

			err := application.syncOnce()
			require.ErrorIs(t, err, ErrSyncPartial)
			require.Equal(t, 1, metrics.syncStarted)
			require.Zero(t, metrics.syncSucceeded)
			require.Equal(t, 1, metrics.syncPartial)
			require.Len(t, metrics.syncDurations, 1)
			require.True(t, metrics.lastSuccess.IsZero())
			require.False(t, metrics.lastCompleted.IsZero())
			require.Empty(t, metrics.syncErrors)
			require.Equal(t, 1, metrics.ytMutationErrors[testCase.expectedOperation])
			require.Empty(t, metrics.ytMutationSuccess)
		})
	}
}

func TestUserMutationFailureDoesNotStopGroupSynchronization(t *testing.T) {
	metrics := newRecordingMetrics()
	application := &App{
		logger: logging.NewDevelopment(),
		source: &fakeSource{
			users:  sourceUsers(fakeUser{ID: "user", Name: "user"}),
			groups: []source.SourceGroupWithMembers{sourceGroup(fakeGroup{ID: "group", Name: "group"}, "user")},
		},
		ytsaurus: &metricsYtsaurus{createUserErr: errors.New("failed to create user")},
		metrics:  metrics,
	}

	err := application.syncOnce()
	require.ErrorIs(t, err, ErrSyncPartial)
	require.Equal(t, 1, metrics.syncPartial)
	require.Equal(t, 1, metrics.ytMutationErrors[objectTypeUser+"/"+mutationCreate])
	require.Equal(t, 1, metrics.ytMutationSuccess[objectTypeGroup+"/"+mutationCreate])
	require.Equal(t, 1, metrics.ytMutationSuccess[objectTypeMembership+"/"+mutationAdd])
}

func TestGroupMutationFailureDoesNotStopMembershipSynchronization(t *testing.T) {
	metrics := newRecordingMetrics()
	application := &App{
		logger: logging.NewDevelopment(),
		source: &fakeSource{
			users:  sourceUsers(fakeUser{ID: "user", Name: "user"}),
			groups: []source.SourceGroupWithMembers{sourceGroup(fakeGroup{ID: "group", Name: "group"}, "user")},
		},
		ytsaurus: &metricsYtsaurus{
			users:          []internalyt.YtsaurusUser{testYtsaurusUser("user")},
			createGroupErr: errors.New("failed to create group"),
		},
		metrics: metrics,
	}

	err := application.syncOnce()
	require.ErrorIs(t, err, ErrSyncPartial)
	require.Equal(t, 1, metrics.syncPartial)
	require.Equal(t, 1, metrics.ytMutationErrors[objectTypeGroup+"/"+mutationCreate])
	require.Equal(t, 1, metrics.ytMutationSuccess[objectTypeMembership+"/"+mutationAdd])
}

func TestRemoveLimitMetrics(t *testing.T) {
	testCases := []struct {
		name           string
		ytsaurus       *metricsYtsaurus
		expectedStage  string
		expectedObject string
	}{
		{
			name:           "users",
			ytsaurus:       &metricsYtsaurus{users: []internalyt.YtsaurusUser{testYtsaurusUser("user")}},
			expectedStage:  syncStageUsers,
			expectedObject: objectTypeUser,
		},
		{
			name:           "groups",
			ytsaurus:       &metricsYtsaurus{groups: []internalyt.YtsaurusGroupWithMembers{testYtsaurusGroup("group", "group")}},
			expectedStage:  syncStageGroups,
			expectedObject: objectTypeGroup,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := newRecordingMetrics()
			application := &App{
				logger:      logging.NewDevelopment(),
				source:      &fakeSource{},
				ytsaurus:    testCase.ytsaurus,
				removeLimit: 1,
				metrics:     metrics,
			}

			err := application.syncOnce()
			require.ErrorContains(t, err, "delete limit in one cycle reached")
			require.Equal(t, 1, metrics.syncErrors[testCase.expectedStage])
			require.Equal(t, 1, metrics.removeLimitReached[testCase.expectedObject])
		})
	}
}

func testYtsaurusUser(name string) internalyt.YtsaurusUser {
	return testYtsaurusUserWithID(name, name)
}

func testYtsaurusUserWithID(id, name string) internalyt.YtsaurusUser {
	return internalyt.YtsaurusUser{
		Username: name,
		SourceRaw: map[string]any{
			"id":         id,
			"name":       name,
			"email":      "",
			"first_name": "",
		},
	}
}

func testYtsaurusGroup(id, name string) internalyt.YtsaurusGroupWithMembers {
	return testYtsaurusGroupWithMembers(id, name)
}

func testYtsaurusGroupWithMembers(id, name string, members ...string) internalyt.YtsaurusGroupWithMembers {
	return internalyt.YtsaurusGroupWithMembers{
		YtsaurusGroup: internalyt.YtsaurusGroup{
			Name: name,
			SourceRaw: map[string]any{
				"id":   id,
				"name": name,
			},
		},
		Members: NewStringSetFromItems(members...),
	}
}

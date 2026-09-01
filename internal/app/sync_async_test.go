package app

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var errStopTestSync = errors.New("stop test sync")

type blockingSource struct {
	started chan struct{}
	release chan struct{}
}

type failingSource struct {
	source.Source
	usersErr  error
	groupsErr error
}

func (s *failingSource) GetUsers() ([]source.SourceUser, error) {
	return nil, s.usersErr
}

func (s *failingSource) GetGroupsWithMembers() ([]source.SourceGroupWithMembers, error) {
	return nil, s.groupsErr
}

type emptyYtsaurus struct {
	Ytsaurus
}

func (y *emptyYtsaurus) GetUsers() ([]internalyt.YtsaurusUser, error) {
	return nil, nil
}

func newBlockingSource() *blockingSource {
	return &blockingSource{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingSource) GetUsers() ([]source.SourceUser, error) {
	close(s.started)
	<-s.release
	return nil, errStopTestSync
}

func (s *blockingSource) GetGroupsWithMembers() ([]source.SourceGroupWithMembers, error) {
	return nil, errStopTestSync
}

func (s *blockingSource) CreateUserFromRaw(map[string]any) (source.SourceUser, error) {
	return nil, errStopTestSync
}

func (s *blockingSource) CreateGroupFromRaw(map[string]any) (source.SourceGroup, error) {
	return nil, errStopTestSync
}

func TestSyncAsyncRejectsConcurrentSync(t *testing.T) {
	source := newBlockingSource()
	application := &App{
		logger: logging.NewDevelopment(),
		source: source,
	}

	require.NoError(t, application.SyncAsync())
	awaitSyncStart(t, source)
	require.ErrorIs(t, application.SyncAsync(), ErrSyncInProgress)

	close(source.release)
	awaitSyncEnd(t, application)
}

func TestSyncOnceBlocksSyncAsync(t *testing.T) {
	source := newBlockingSource()
	application := &App{
		logger: logging.NewDevelopment(),
		source: source,
	}

	syncResult := make(chan error, 1)
	go func() {
		syncResult <- application.syncOnce()
	}()
	awaitSyncStart(t, source)
	require.ErrorIs(t, application.SyncAsync(), ErrSyncInProgress)

	close(source.release)
	require.ErrorIs(t, awaitSyncResult(t, syncResult), errStopTestSync)
}

func TestSyncOnceRejectsConcurrentSync(t *testing.T) {
	source := newBlockingSource()
	application := &App{
		logger: logging.NewDevelopment(),
		source: source,
	}

	require.NoError(t, application.SyncAsync())
	awaitSyncStart(t, source)
	require.ErrorIs(t, application.syncOnce(), ErrSyncInProgress)

	close(source.release)
	awaitSyncEnd(t, application)
}

func TestStartLogsRejectedSync(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	application := &App{
		logger: zap.New(core).Sugar(),
		stopCh: make(chan struct{}),
		sigCh:  make(chan os.Signal, 1),
	}
	application.syncMutex.Lock()
	defer application.syncMutex.Unlock()

	startDone := make(chan struct{})
	go func() {
		application.Start()
		close(startDone)
	}()
	application.sigCh <- syscall.SIGUSR1

	require.Eventually(t, func() bool {
		return logs.FilterMessage("Sync failed").Len() == 1
	}, time.Second, 10*time.Millisecond)
	entries := logs.FilterMessage("Sync failed").All()
	require.Equal(t, ErrSyncInProgress.Error(), entries[0].ContextMap()["error"])

	application.Stop()
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("application did not stop")
	}
}

func TestSyncOnceReturnsUserSyncError(t *testing.T) {
	expectedErr := errors.New("users unavailable")
	metrics := newRecordingMetrics()
	application := &App{
		logger:  logging.NewDevelopment(),
		source:  &failingSource{usersErr: expectedErr},
		metrics: metrics,
	}

	err := application.syncOnce()
	require.ErrorIs(t, err, expectedErr)
	require.ErrorContains(t, err, "user sync failed")
	require.Equal(t, 1, metrics.syncStarted)
	require.Zero(t, metrics.syncSucceeded)
	require.Zero(t, metrics.syncPartial)
	require.Equal(t, 1, metrics.syncErrors[syncStageUsers])
	require.True(t, metrics.lastCompleted.IsZero())
}

func TestSyncOnceReturnsGroupSyncError(t *testing.T) {
	expectedErr := errors.New("groups unavailable")
	metrics := newRecordingMetrics()
	application := &App{
		logger:   logging.NewDevelopment(),
		source:   &failingSource{groupsErr: expectedErr},
		ytsaurus: &emptyYtsaurus{},
		metrics:  metrics,
	}

	err := application.syncOnce()
	require.ErrorIs(t, err, expectedErr)
	require.ErrorContains(t, err, "group sync failed")
	require.Equal(t, 1, metrics.syncStarted)
	require.Zero(t, metrics.syncSucceeded)
	require.Zero(t, metrics.syncPartial)
	require.Equal(t, 1, metrics.syncErrors[syncStageGroups])
	require.True(t, metrics.lastCompleted.IsZero())
}

func TestSyncOnceReturnsYtsaurusListErrors(t *testing.T) {
	expectedErr := errors.New("YTsaurus unavailable")
	testCases := []struct {
		name          string
		ytsaurus      *metricsYtsaurus
		expectedStage string
	}{
		{
			name:          "users",
			ytsaurus:      &metricsYtsaurus{getUsersErr: expectedErr},
			expectedStage: syncStageUsers,
		},
		{
			name:          "groups",
			ytsaurus:      &metricsYtsaurus{getGroupsErr: expectedErr},
			expectedStage: syncStageGroups,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			metrics := newRecordingMetrics()
			application := &App{
				logger:   logging.NewDevelopment(),
				source:   &fakeSource{},
				ytsaurus: testCase.ytsaurus,
				metrics:  metrics,
			}

			err := application.syncOnce()
			require.ErrorIs(t, err, expectedErr)
			require.Equal(t, 1, metrics.syncErrors[testCase.expectedStage])
			require.Zero(t, metrics.syncPartial)
			require.True(t, metrics.lastCompleted.IsZero())
		})
	}
}

func TestSyncAsyncLogsSyncError(t *testing.T) {
	expectedErr := errors.New("users unavailable")
	core, logs := observer.New(zap.ErrorLevel)
	application := &App{
		logger: zap.New(core).Sugar(),
		source: &failingSource{usersErr: expectedErr},
	}

	require.NoError(t, application.SyncAsync())
	require.Eventually(t, func() bool {
		return logs.FilterMessage("Sync failed").Len() == 1
	}, time.Second, 10*time.Millisecond)
	entries := logs.FilterMessage("Sync failed").All()
	require.Contains(t, entries[0].ContextMap()["error"], "user sync failed")
	awaitSyncEnd(t, application)
}

func TestSyncAsyncLogsPartialSync(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	application := &App{
		logger: zap.New(core).Sugar(),
		source: &fakeSource{
			users: sourceUsers(fakeUser{ID: "user", Name: "user"}),
		},
		ytsaurus: &metricsYtsaurus{createUserErr: errors.New("failed to create user")},
	}

	require.NoError(t, application.SyncAsync())
	require.Eventually(t, func() bool {
		return logs.FilterLevelExact(zap.WarnLevel).Len() == 1
	}, time.Second, 10*time.Millisecond)
	warnings := logs.FilterLevelExact(zap.WarnLevel).All()
	require.Equal(t, "sync partially completed: user sync: 1 YTsaurus mutations failed", warnings[0].Message)
	require.NotContains(t, warnings[0].ContextMap(), "error")
	require.Zero(t, logs.FilterMessage("Sync failed").Len())
	awaitSyncEnd(t, application)
}

func awaitSyncStart(t *testing.T, source *blockingSource) {
	t.Helper()
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("sync did not start")
	}
}

func awaitSyncEnd(t *testing.T, application *App) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !application.syncMutex.TryLock() {
			return false
		}
		application.syncMutex.Unlock()
		return true
	}, time.Second, 10*time.Millisecond)
}

func awaitSyncResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("sync did not finish")
		return nil
	}
}

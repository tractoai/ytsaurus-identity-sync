package app

import (
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"
)

// ErrSyncInProgress indicates that another sync iteration owns the sync lock.
var ErrSyncInProgress = errors.New("sync is already in progress")

// ErrSyncPartial indicates that all synchronization stages ran, but one or more mutations failed.
var ErrSyncPartial = errors.New("sync partially completed")

type partialSyncError struct {
	causes []error
}

func (e *partialSyncError) Error() string {
	return ErrSyncPartial.Error() + ": " + errors.Join(e.causes...).Error()
}

func (e *partialSyncError) Unwrap() []error {
	errs := make([]error, 0, len(e.causes)+1)
	errs = append(errs, ErrSyncPartial)
	return append(errs, e.causes...)
}

// Ytsaurus contains the YTsaurus operations required by App.
type Ytsaurus interface {
	GetUsers() ([]internalyt.YtsaurusUser, error)
	CreateUser(user internalyt.YtsaurusUser) error
	UpdateUser(username string, user internalyt.YtsaurusUser) error
	RemoveUser(username string) error
	BanUser(username string) error

	GetGroupsWithMembers() ([]internalyt.YtsaurusGroupWithMembers, error)
	CreateGroup(group internalyt.YtsaurusGroup) error
	UpdateGroup(groupname string, group internalyt.YtsaurusGroup) error
	RemoveGroup(groupname string) error

	AddMember(username, groupname string) error
	RemoveMember(username, groupname string) error
}

type App struct {
	syncInterval      time.Duration
	usernameReplaces  []config.ReplacementPair
	groupnameReplaces []config.ReplacementPair
	removeLimit       int
	banDuration       time.Duration

	ytsaurus Ytsaurus
	source   source.Source
	metrics  Metrics

	stopCh chan struct{}
	sigCh  chan os.Signal
	logger *zap.SugaredLogger

	stopOnce  sync.Once
	syncMutex sync.Mutex
}

func NewApp(cfg *config.Config, logger *zap.SugaredLogger, source source.Source, ytsaurus Ytsaurus, metrics Metrics) *App {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)

	return &App{
		syncInterval:      cfg.App.SyncInterval,
		usernameReplaces:  cfg.App.UsernameReplacements,
		groupnameReplaces: cfg.App.GroupnameReplacements,
		removeLimit:       cfg.App.RemoveLimit,
		banDuration:       cfg.App.BanBeforeRemoveDuration,

		ytsaurus: ytsaurus,
		source:   source,
		metrics:  metrics,

		stopCh: make(chan struct{}),
		sigCh:  sigCh,
		logger: logger,
	}
}

func (a *App) Start() {
	a.logger.Info("Starting the application")

	var ticker *time.Ticker
	var tickerCh <-chan time.Time
	if a.syncInterval > 0 {
		ticker = time.NewTicker(a.syncInterval)
		tickerCh = ticker.C
		defer ticker.Stop()
	} else {
		a.logger.Info(
			"app.sync_interval config variable is not specified or is not greater than zero, " +
				"auto sync is disabled. Send SIGUSR1 for manual sync.",
		)
	}

	for {
		select {
		case <-a.stopCh:
			a.logger.Info("Stopping the application")
			return
		case <-tickerCh:
			a.logger.Debug("Received next tick")
			a.logSyncResult(a.syncOnce())
		case <-a.sigCh:
			a.logger.Info("Received SIGUSR1")
			a.logSyncResult(a.syncOnce())
		}
	}
}

func (a *App) Stop() {
	a.stopOnce.Do(func() {
		signal.Stop(a.sigCh)
		close(a.stopCh)
	})
}

// SyncAsync starts a sync iteration in the background.
func (a *App) SyncAsync() error {
	if !a.syncMutex.TryLock() {
		return ErrSyncInProgress
	}

	go func() {
		defer a.syncMutex.Unlock()
		a.logSyncResult(a.syncOnceLocked())
	}()

	return nil
}

func (a *App) logSyncResult(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, ErrSyncPartial) {
		a.logger.Warn(err)
		return
	}
	a.logger.Errorw("Sync failed", zap.Error(err))
}

func newPartialSyncError(errs ...error) error {
	causes := make([]error, 0, len(errs))
	for _, err := range errs {
		var partialErr *partialSyncError
		if errors.As(err, &partialErr) {
			causes = append(causes, partialErr.causes...)
			continue
		}
		causes = append(causes, err)
	}
	return &partialSyncError{causes: causes}
}

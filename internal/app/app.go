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

	stopCh chan struct{}
	sigCh  chan os.Signal
	logger *zap.SugaredLogger

	stopOnce  sync.Once
	syncMutex sync.Mutex
}

func NewApp(cfg *config.Config, logger *zap.SugaredLogger, source source.Source, ytsaurus Ytsaurus) *App {
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
			if err := a.syncOnce(); err != nil {
				a.logger.Errorw("Sync failed", zap.Error(err))
			}
		case <-a.sigCh:
			a.logger.Info("Received SIGUSR1")
			if err := a.syncOnce(); err != nil {
				a.logger.Errorw("Sync failed", zap.Error(err))
			}
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
		if err := a.syncOnceLocked(); err != nil {
			a.logger.Errorw("Sync failed", zap.Error(err))
		}
	}()

	return nil
}

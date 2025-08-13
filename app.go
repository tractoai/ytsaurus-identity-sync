package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pkg/errors"

	"k8s.io/utils/clock"
)

type ObjectID = string

type Source interface {
	GetUsers() ([]SourceUser, error)
	GetGroupsWithMembers() ([]SourceGroupWithMembers, error)
	CreateUserFromRaw(raw map[string]any) (SourceUser, error)
	CreateGroupFromRaw(raw map[string]any) (SourceGroup, error)
	// GetUsersByGroups returns users that belong to the specified groups
	GetUsersByGroups(groups []SourceGroupWithMembers) ([]SourceUser, error)
}

type App struct {
	syncInterval      time.Duration
	syncStrategy      SyncStrategy
	usernameReplaces  []ReplacementPair
	groupnameReplaces []ReplacementPair
	removeLimit       int
	banDuration       time.Duration

	ytsaurus *Ytsaurus
	source   Source

	stopCh chan struct{}
	sigCh  chan os.Signal
	logger appLoggerType
}

func NewApp(cfg *Config, logger appLoggerType) (*App, error) {
	if (cfg.Azure == nil) == (cfg.Ldap == nil) {
		return nil, errors.New("one and only one source should be specified")
	}

	var err error
	var source Source
	if cfg.Azure != nil {
		source, err = NewAzureReal(cfg.Azure, logger)
		if err != nil {
			return nil, err
		}
	}

	if cfg.Ldap != nil {
		source, err = NewLdap(cfg.Ldap, logger)
		if err != nil {
			return nil, err
		}
	}

	return NewAppCustomized(cfg, logger, source, clock.RealClock{})
}

// NewAppCustomized used in tests.
func NewAppCustomized(cfg *Config, logger appLoggerType, source Source, clock clock.PassiveClock) (*App, error) {
	yt, err := NewYtsaurus(&cfg.Ytsaurus, logger, clock)
	if err != nil {
		return nil, err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)

	// Default to users_first strategy if not specified
	strategy := cfg.App.SyncStrategy
	if strategy == "" {
		strategy = SyncStrategyUsersFirst
	}

	return &App{
		syncInterval:      cfg.App.SyncInterval,
		syncStrategy:      strategy,
		usernameReplaces:  cfg.App.UsernameReplacements,
		groupnameReplaces: cfg.App.GroupnameReplacements,
		removeLimit:       cfg.App.RemoveLimit,
		banDuration:       cfg.App.BanBeforeRemoveDuration,

		ytsaurus: yt,
		source:   source,

		stopCh: make(chan struct{}),
		sigCh:  sigCh,
		logger: logger,
	}, nil
}

func (a *App) Start() {
	a.logger.Info("Starting the application")
	if a.syncInterval > 0 {
		ticker := time.NewTicker(a.syncInterval)
		for {
			select {
			case <-a.stopCh:
				a.logger.Info("Stopping the application")
				return
			case <-ticker.C:
				a.logger.Debug("Received next tick")
				a.syncOnce()
			case <-a.sigCh:
				a.logger.Info("Received SIGUSR1")
				a.syncOnce()
			}
		}
	} else {
		a.logger.Info(
			"app.sync_interval config variable is not specified or is not greater than zero, " +
				"auto sync is disabled. Send SIGUSR1 for manual sync.",
		)
		for {
			select {
			case <-a.stopCh:
				a.logger.Info("Stopping the application")
				return
			case <-a.sigCh:
				a.logger.Info("Received SIGUSR1")
				a.syncOnce()
			}
		}
	}

}

func (a *App) Stop() {
	close(a.stopCh)
}

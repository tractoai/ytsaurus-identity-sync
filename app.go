package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/utils/clock"
)

type Azure interface {
	GetUsers() ([]AzureUser, error)
	GetGroupsWithMembers() ([]AzureGroupWithMembers, error)
}

type App struct {
	syncInterval      time.Duration
	usernameReplaces  []ReplacementPair
	groupnameReplaces []ReplacementPair
	removeLimit       int
	banDuration       time.Duration

	ytsaurus *Ytsaurus
	azure    Azure

	stopCh chan struct{}
	sigCh  chan os.Signal
	logger appLoggerType
}

func NewApp(cfg *Config, logger appLoggerType) (*App, error) {
	azure, err := NewAzureReal(cfg.Azure, logger)
	if err != nil {
		return nil, err
	}

	return NewAppCustomized(cfg, logger, azure, clock.RealClock{})
}

// NewAppCustomized used in tests.
func NewAppCustomized(cfg *Config, logger appLoggerType, azure Azure, clock clock.PassiveClock) (*App, error) {
	yt, err := NewYtsaurus(cfg.Ytsaurus, logger, clock)
	if err != nil {
		return nil, err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGUSR1)

	return &App{
		syncInterval:      cfg.App.SyncInterval,
		usernameReplaces:  cfg.App.UsernameReplacements,
		groupnameReplaces: cfg.App.GroupnameReplacements,
		removeLimit:       cfg.App.RemoveLimit,
		banDuration:       cfg.App.BanBeforeRemoveDuration,

		ytsaurus: yt,
		azure:    azure,

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
			"app.sync_interval config variable is not greater than zero, " +
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

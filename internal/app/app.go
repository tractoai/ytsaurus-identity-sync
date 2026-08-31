package app

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"

	"k8s.io/utils/clock"
)

type App struct {
	syncInterval      time.Duration
	usernameReplaces  []config.ReplacementPair
	groupnameReplaces []config.ReplacementPair
	removeLimit       int
	banDuration       time.Duration

	ytsaurus *internalyt.Ytsaurus
	source   source.Source

	stopCh chan struct{}
	sigCh  chan os.Signal
	logger *zap.SugaredLogger
}

func NewApp(cfg *config.Config, logger *zap.SugaredLogger, source source.Source) (*App, error) {
	return NewAppCustomized(cfg, logger, source, clock.RealClock{})
}

// NewAppCustomized used in tests.
func NewAppCustomized(cfg *config.Config, logger *zap.SugaredLogger, source source.Source, clock clock.PassiveClock) (*App, error) {
	yt, err := internalyt.NewYtsaurus(&cfg.Ytsaurus, logger, clock)
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

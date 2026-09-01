package main

import (
	"fmt"

	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/tractoai/ytsaurus-identity-sync/internal/app"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
	"github.com/tractoai/ytsaurus-identity-sync/internal/server"
	"github.com/tractoai/ytsaurus-identity-sync/internal/sources"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"go.uber.org/zap"
	"k8s.io/utils/clock"
)

var options struct {
	ConfigFile string `long:"config" description:"Config file path" required:"true"`
}

func main() {
	_, err := flags.Parse(&options)
	if err != nil {
		panic("failed to parse options: " + err.Error())
	}

	err = run(options.ConfigFile)
	if err != nil {
		panic("failed to start the application: " + err.Error())
	}
}

func run(configFilePath string) error {
	fmt.Println("Config file path:", configFilePath)
	cfg, err := config.Load(configFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to load config %s", configFilePath)
	}

	logger, err := logging.New(&cfg.Logging)
	if err != nil {
		return errors.Wrapf(err, "failed to configure logging %+v", cfg.Logging)
	}
	defer func() {
		err = logger.Sync() // flushes buffer, if any
		if err != nil {
			logger.Error("log flush failed", zap.Error(err))
		}
	}()

	logger.Infow("Config",
		"struct", cfg,
	)

	src, err := sources.New(cfg, logger)
	if err != nil {
		return err
	}

	ytsaurus, err := internalyt.NewYtsaurus(&cfg.Ytsaurus, logger, clock.RealClock{})
	if err != nil {
		return err
	}
	application := app.NewApp(cfg, logger, src, ytsaurus)

	defer application.Stop()
	healthServer := server.New(logger, &cfg.App.Server, application)
	if err := healthServer.Start(); err != nil {
		return err
	}
	defer healthServer.Stop()

	application.Start()

	logger.Info("Application stopped")
	return nil
}

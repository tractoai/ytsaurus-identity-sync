package main

import (
	"fmt"

	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"go.uber.org/zap"
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
	content, err := readConfig(configFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to load config %s", configFilePath)
	}
	fmt.Print("Config content:\n", string(content))
	cfg, err := unmarshallConfig(content)
	if err != nil {
		return errors.Wrapf(err, "failed to load config %s", configFilePath)
	}

	logger, err := configureLogger(cfg.Logging)
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

	app, err := NewApp(cfg, logger)
	if err != nil {
		return err
	}

	defer app.Stop()
	app.Start()

	logger.Info("Application stopped")
	return nil
}

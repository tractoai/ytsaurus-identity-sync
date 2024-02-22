package main

import (
	"os"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

const (
	appTimeFormat            = "2006-01-02T15:04:05Z0700"
	defaultAzureTimeout      = 3 * time.Second
	defaultAzureSecretEnvVar = "AZURE_CLIENT_SECRET"
	defaultAppRemoveLimit    = 10
)

type appLoggerType = *zap.SugaredLogger

func readConfig(filename string) ([]byte, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return []byte{}, errors.Wrapf(err, "failed to read file %s", filename)
	}
	return data, err
}

func unmarshallConfig(content []byte) (*Config, error) {
	cfg := &Config{}
	err := yaml.Unmarshal(content, cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshall config file: %s", content)
	}
	return cfg, nil
}

func loadConfig(filename string) (*Config, error) {
	content, err := readConfig(filename)
	if err != nil {
		return nil, err
	}
	return unmarshallConfig(content)
}

func configureLogger(cfg *LoggingConfig, opts ...zap.Option) (*zap.SugaredLogger, error) {
	var zapConfig zap.Config
	if cfg.IsProduction {
		zapConfig = zap.NewProductionConfig()
		zapConfig.Sampling = nil // disable logs sampling, we don't have that much
		zapConfig.EncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.UTC().Format(appTimeFormat))
			// 2019-08-13T04:39:11Z
		}
	} else {
		zapConfig = zap.NewDevelopmentConfig()
	}
	zapLevel, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	zapConfig.Level.SetLevel(zapLevel)
	logger, err := zapConfig.Build(opts...)
	if err != nil {
		return nil, err
	}
	return logger.Sugar(), nil
}

func getDevelopmentLogger() appLoggerType {
	return zap.Must(zap.NewDevelopment()).Sugar()
}

type StringSet mapset.Set[string]

func NewStringSet() StringSet {
	return mapset.NewSet[string]()
}

func NewStringSetFromItems(slice ...string) StringSet {
	set := NewStringSet()
	for _, item := range slice {
		set.Add(item)
	}
	return set
}

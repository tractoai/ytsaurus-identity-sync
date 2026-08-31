package logging

import (
	"time"

	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const timeFormat = "2006-01-02T15:04:05Z0700"

// New configures the application logger.
func New(cfg *config.LoggingConfig, opts ...zap.Option) (*zap.SugaredLogger, error) {
	var zapConfig zap.Config
	if cfg.IsProduction {
		zapConfig = zap.NewProductionConfig()
		zapConfig.Sampling = nil
		zapConfig.EncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.UTC().Format(timeFormat))
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

// NewDevelopment creates a development logger for tests and local tools.
func NewDevelopment() *zap.SugaredLogger {
	return zap.Must(zap.NewDevelopment()).Sugar()
}

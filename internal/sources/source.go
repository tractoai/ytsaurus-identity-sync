package sources

import (
	"github.com/pkg/errors"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/sources/azure"
	ldapsource "github.com/tractoai/ytsaurus-identity-sync/internal/sources/ldap"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"
)

func New(cfg *config.Config, logger *zap.SugaredLogger) (source.Source, error) {
	if (cfg.Azure == nil) == (cfg.Ldap == nil) {
		return nil, errors.New("one and only one source should be specified")
	}

	if cfg.Azure != nil {
		return azure.NewAzureReal(cfg.Azure, logger)
	}

	return ldapsource.NewLdap(cfg.Ldap, logger)
}

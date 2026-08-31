package sources

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/logging"
)

func TestNewRequiresExactlyOneConfiguredSource(t *testing.T) {
	t.Parallel()

	logger := logging.NewDevelopment()

	_, err := New(&config.Config{}, logger)
	require.EqualError(t, err, "one and only one source should be specified")

	_, err = New(&config.Config{
		Azure: &config.AzureConfig{},
		Ldap:  &config.LdapConfig{},
	}, logger)
	require.EqualError(t, err, "one and only one source should be specified")
}

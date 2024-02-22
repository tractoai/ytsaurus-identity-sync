package main

import (
	"embed"
	"go.ytsaurus.tech/library/go/ptr"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

//go:embed config.example.yaml
var _ embed.FS

func TestConfig(t *testing.T) {
	configPath := "config.example.yaml"

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)

	require.Equal(t, ptr.Duration(5*time.Minute), cfg.App.SyncInterval)
	require.Equal(t, []ReplacementPair{
		{From: "@acme.com", To: ""},
		{From: "@", To: ":"},
	}, cfg.App.UsernameReplacements)
	require.Equal(t, []ReplacementPair{
		{From: "|all", To: ""},
	}, cfg.App.GroupnameReplacements)
	require.Equal(t, ptr.Int(10), cfg.App.RemoveLimit)
	require.Equal(t, ptr.Duration(7*24*time.Hour), cfg.App.BanBeforeRemoveDuration)

	require.Equal(t, "acme.onmicrosoft.com", cfg.Azure.Tenant)
	require.Equal(t, "abcdefgh-a000-b111-c222-abcdef123456", cfg.Azure.ClientID)
	require.Equal(t, 1*time.Second, cfg.Azure.Timeout)
	require.Equal(t, "(accountEnabled eq true) and (userType eq 'Member')", cfg.Azure.UsersFilter)
	require.Equal(t, "displayName -ne ''", cfg.Azure.GroupsFilter)
	require.Equal(t, ".dev", cfg.Azure.GroupsDisplayNameSuffixPostFilter)

	require.Equal(t, "localhost:10110", cfg.Ytsaurus.Proxy)
	require.Equal(t, true, cfg.Ytsaurus.ApplyUserChanges)
	require.Equal(t, true, cfg.Ytsaurus.ApplyGroupChanges)
	require.Equal(t, true, cfg.Ytsaurus.ApplyMemberChanges)
	require.Equal(t, 1*time.Second, cfg.Ytsaurus.Timeout)
	require.Equal(t, "DEBUG", cfg.Ytsaurus.LogLevel)

	require.Equal(t, "WARN", cfg.Logging.Level)
	require.Equal(t, true, cfg.Logging.IsProduction)

	logger, err := configureLogger(&cfg.Logging)
	require.NoError(t, err)
	logger.Debugw("test logging message", "key", "val")
}

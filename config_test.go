package main

import (
	"embed"
	"testing"
	"time"

	"go.ytsaurus.tech/library/go/ptr"

	"github.com/stretchr/testify/require"
)

//go:embed azure_config.example.yaml ldap_config.example.yaml
var _ embed.FS

func TestAzureConfig(t *testing.T) {
	configPath := "azure_config.example.yaml"

	cfg, err := loadConfig(configPath)
	require.NoError(t, err)

	require.Equal(t, 5*time.Minute, cfg.App.SyncInterval)
	require.Equal(t, []ReplacementPair{
		{From: "@acme.com", To: ""},
		{From: "@", To: ":"},
	}, cfg.App.UsernameReplacements)
	require.Equal(t, []ReplacementPair{
		{From: "|all", To: ""},
	}, cfg.App.GroupnameReplacements)
	require.Equal(t, 10, cfg.App.RemoveLimit)
	require.Equal(t, 7*24*time.Hour, cfg.App.BanBeforeRemoveDuration)

	require.True(t, cfg.Ldap == nil)

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

func TestLdapConfig(t *testing.T) {
	configPath := "ldap_config.example.yaml"

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

	require.True(t, cfg.Azure == nil)

	require.Equal(t, "dc=example,dc=org", cfg.Ldap.BaseDN)
	require.Equal(t, "cn=admin,dc=example,dc=org", cfg.Ldap.BindDN)
	require.Equal(t, "localhost:10210", cfg.Ldap.Address)
	require.Equal(t, "LDAP_PASSWORD", cfg.Ldap.BindPasswordEnvVar)

	require.Equal(t, "(&(objectClass=posixAccount)(ou=People))", cfg.Ldap.Users.Filter)
	require.Equal(t, "cn", cfg.Ldap.Users.UsernameAttributeType)
	require.Equal(t, "uid", cfg.Ldap.Users.UIDAttributeType)
	require.Equal(t, ptr.String("givenName"), cfg.Ldap.Users.FirstNameAttributeType)

	require.Equal(t, "(objectClass=posixGroup)", cfg.Ldap.Groups.Filter)
	require.Equal(t, "cn", cfg.Ldap.Groups.GroupnameAttributeType)
	require.Equal(t, "memberUid", cfg.Ldap.Groups.MemberUIDAttributeType)

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

package main

import (
	"time"
)

type Config struct {
	App      *AppConfig      `yaml:"app"`
	Azure    *AzureConfig    `yaml:"azure"`
	Ytsaurus *YtsaurusConfig `yaml:"ytsaurus"`
	Logging  *LoggingConfig  `yaml:"logging"`
}

type AppConfig struct {
	// SyncInterval is the interval between full synchronizations.
	// Zero value means that auto-sync disabled (sync can be invoked only manually).
	SyncInterval time.Duration `yaml:"sync_interval"`

	// UsernameReplacements is a list of replaces which will be applied to the userPrincipalName Azure field before
	// using as username in Ytsaurus.
	// For example, you may use it to strip off characters like @ which are not recommended for use
	// in usernames as they are required to be escaped in YPath.
	UsernameReplacements  []ReplacementPair `yaml:"username_replacements"`
	GroupnameReplacements []ReplacementPair `yaml:"groupname_replacements"`

	// If count users or groups for planned delete in on sync cycle reaches RemoveLimit
	// app will fail that sync cycle.
	RemoveLimit int `yaml:"remove_limit"`

	// BanBeforeRemoveDuration is a duration of a graceful ban before finally removing the user from YTsaurus.
	// Default value is 0s, which means remove straight after user was found to be missing from Azure,.
	BanBeforeRemoveDuration time.Duration `yaml:"ban_before_remove_duration"`
}

type ReplacementPair struct {
	From string `yaml:"from"`
	To   string `yaml:"to"`
}

type AzureConfig struct {
	Tenant             string `yaml:"tenant"`
	ClientID           string `yaml:"client_id"`
	ClientSecretEnvVar string `yaml:"client_secret_env_var"` // default: "AZURE_CLIENT_SECRET"
	// UsersFilter is MS Graph $filter value used for user fetching requests.
	// See https://learn.microsoft.com/en-us/graph/api/user-list?#optional-query-parameters
	UsersFilter string `yaml:"users_filter"`
	// GroupsFilter is MS Graph $filter value used for group fetching requests.
	// See https://learn.microsoft.com/en-us/graph/api/group-list
	GroupsFilter string `yaml:"groups_filter"`
	// GroupsDisplayNameSuffixPostFilter applied to the fetched groups display names.
	GroupsDisplayNameSuffixPostFilter string        `yaml:"groups_display_name_suffix_post_filter"`
	Timeout                           time.Duration `yaml:"timeout"`

	// DebugAzureIDs is a list of ids for which app will print more debug info in logs.
	DebugAzureIDs []string `yaml:"debug_azure_ids"`
}

type YtsaurusConfig struct {
	Proxy string `yaml:"proxy"`
	// SecretEnvVar is a name of env variable with YTsaurus token. Default: "YT_TOKEN".
	SecretEnvVar string `yaml:"secret_env_var"`
	// ApplyUserChanges = false means dry-run (no writes will be executed) for users updates.
	ApplyUserChanges bool `yaml:"apply_user_changes"`
	// ApplyGroupChanges = false means dry-run (no writes will be executed) for groups updates.
	ApplyGroupChanges bool `yaml:"apply_group_changes"`
	// ApplyMemberChanges = false means dry-run (no writes will be executed) for membership updates.
	ApplyMemberChanges bool `yaml:"apply_member_changes"`

	Timeout  time.Duration `yaml:"timeout"`
	LogLevel string        `yaml:"log_level"`

	// DebugUsernames is a list of YTsaurus usernames for which app will print more debug info in logs.
	DebugUsernames []string `yaml:"debug_usernames"`
	// DebugGroupnames is a list of YTsaurus groupnames for which app will print more debug info in logs.
	DebugGroupnames []string `yaml:"debug_groupnames"`
}

type LoggingConfig struct {
	Level        string `yaml:"level"`
	IsProduction bool   `yaml:"is_production"`
}

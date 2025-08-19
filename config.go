package main

import (
	"time"
)

type Config struct {
	App      AppConfig      `yaml:"app"`
	Ytsaurus YtsaurusConfig `yaml:"ytsaurus"`
	Logging  LoggingConfig  `yaml:"logging"`

	// One of them should be specified.
	Azure *AzureConfig `yaml:"azure,omitempty"`
	Ldap  *LdapConfig  `yaml:"ldap,omitempty"`
}

type AppConfig struct {
	// SyncInterval is the interval between full synchronizations.
	// If it is not speciied or value is zero than auto-sync disabled (sync can be invoked only manually).
	SyncInterval time.Duration `yaml:"sync_interval"`

	// UsernameReplacements is a list of replaces which will be applied to a username for source (Azure or Ldap).
	// For example, you may use it to strip off characters like @ which are not recommended for use
	// in usernames as they are required to be escaped in YPath.
	UsernameReplacements  []ReplacementPair `yaml:"username_replacements"`
	GroupnameReplacements []ReplacementPair `yaml:"groupname_replacements"`

	// If count users or groups for planned delete in on sync cycle reaches RemoveLimit
	// app will fail that sync cycle.
	// No limit if it is not specified.
	RemoveLimit int `yaml:"remove_limit,omitempty"`

	// BanBeforeRemoveDuration is a duration of a graceful ban before finally removing the user from YTsaurus.
	// If it is not specified, user will be removed straight after user was found to be missing from source (Azure or Ldap).
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

	// We sync 3 entities independently: users, groups, and memberships.
	//
	// USERS are filtered using TWO filters applied sequentially:
	// 1. `users_filter` - MS Graph $filter for user requests (e.g., accountEnabled eq true)
	// 2. `user_groups_filter` - MS Graph $filter for group requests to get groups whose members will be synced as users
	//    This is needed because MS Graph user API doesn't support filtering by group membership.
	//    Only users who match BOTH filters will be synced (users_filter AND membership in user_groups_filter groups).
	//
	// GROUPS are filtered independently using:
	// - `groups_filter` - MS Graph $filter for group requests
	//   This works independently from `user_groups_filter` because the set of groups you want to sync
	//   may be different from the groups whose members you want to sync as users.
	//
	// MEMBERSHIPS are synced from Azure, excluding memberships for users and groups that didn't match their respective filters.
	//
	// All filter formats follow MS Graph $filter OData syntax.
	// See https://learn.microsoft.com/en-us/graph/api/user-list?#optional-query-parameters
	UsersFilter      string `yaml:"users_filter"`      // Filter for MS Graph users API
	UserGroupsFilter string `yaml:"user_groups_filter"` // Filter for MS Graph groups API to determine which users to sync
	GroupsFilter     string `yaml:"groups_filter"`      // Filter for MS Graph groups API to determine which groups to sync

	// TODO(nadya73): support for ldap also, but with other name.
	// GroupsDisplayNameSuffixPostFilter is deprecated: use GroupsDisplayNameRegexPostFilter instead.
	GroupsDisplayNameSuffixPostFilter string `yaml:"groups_display_name_suffix_post_filter"`
	// GroupsDisplayNameRegexPostFilter applied to the fetched groups display names.
	GroupsDisplayNameRegexPostFilter string        `yaml:"groups_display_name_regex_post_filter"`
	Timeout                          time.Duration `yaml:"timeout"`

	// TODO(nadya73): support for ldap also, but with other name.
	// DebugAzureIDs is a list of ids for which app will print more debug info in logs.
	DebugAzureIDs []string `yaml:"debug_azure_ids"`
}

type LdapUsersConfig struct {
	// A filter for getting users.
	// For example, `(objectClass=account)`.
	Filter string `yaml:"filter"`
	// An attribute type which will be used as @name attribute.
	// For example, `cn`.
	UsernameAttributeType string `yaml:"username_attribute_type"`
	// For example, `uid`.
	UIDAttributeType       string  `yaml:"uid_attribute_type"`
	FirstNameAttributeType *string `yaml:"first_name_attribute_type"`
	// A list of usernames for which app will print more debug info in logs.
	DebugUsernames []string `yaml:"debug_usernames"`
}

type LdapGroupsConfig struct {
	// A filter for getting groups.
	// For example, `(objectClass=posixGroup)`.
	Filter string `yaml:"filter"`
	// An attribute type which will be used as @name attribute.
	// For example, `cn`.
	GroupnameAttributeType string `yaml:"groupname_attribute_type"`
	// An attribute type which will be used for getting group members.
	// For example, `memberUid`.
	MemberUIDAttributeType string `yaml:"member_uid_attribute_type"`

	// A list of groupnames for which app will print more debug info in logs.
	DebugGroupnames []string `yaml:"debug_groupnames"`
}

type LdapConfig struct {
	Address            string           `yaml:"address"`
	BindDN             string           `yaml:"bind_dn"`
	BindPasswordEnvVar string           `yaml:"bind_password_env_var"`
	Users              LdapUsersConfig  `yaml:"users"`
	Groups             LdapGroupsConfig `yaml:"groups"`
	BaseDN             string           `yaml:"base_dn"`
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
	// The attribute name of user/group object in YTsaurus.
	SourceAttributeName string `yaml:"source_attribute_name"`
}

type LoggingConfig struct {
	Level        string `yaml:"level"`
	IsProduction bool   `yaml:"is_production"`
}

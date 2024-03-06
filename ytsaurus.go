package main

import (
	"context"
	"os"
	"time"

	"go.ytsaurus.tech/library/go/ptr"

	"github.com/pkg/errors"
	"k8s.io/utils/clock"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	defaultYtsaurusTimeout      = 3 * time.Second
	defaultYtsaurusSecretEnvVar = "YT_TOKEN"
)

type Ytsaurus struct {
	client yt.Client

	logger  appLoggerType
	timeout time.Duration
	clock   clock.PassiveClock

	dryRunUsers   bool
	dryRunGroups  bool
	dryRunMembers bool

	debugUsernames  []string
	debugGroupnames []string

	sourceAttributeName string
}

func NewYtsaurus(cfg *YtsaurusConfig, logger appLoggerType, clock clock.PassiveClock) (*Ytsaurus, error) {
	if cfg.LogLevel != "" {
		err := os.Setenv("YT_LOG_LEVEL", cfg.LogLevel)
		if err != nil {
			return nil, err
		}
	}

	if cfg.SecretEnvVar == "" {
		cfg.SecretEnvVar = defaultYtsaurusSecretEnvVar
	}
	secret := os.Getenv(cfg.SecretEnvVar)
	if secret == "" {
		return nil, errors.Errorf("YTsaurus secret in %s env var shouldn't be empty", cfg.SecretEnvVar)
	}
	client, err := ythttp.NewClient(&yt.Config{
		Proxy: cfg.Proxy,
		Credentials: &yt.TokenCredentials{
			Token: secret,
		},
	})
	if err != nil {
		return nil, err
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaultYtsaurusTimeout
	}
	if cfg.SourceAttributeName == nil {
		cfg.SourceAttributeName = ptr.String("source")
	}
	return &Ytsaurus{
		client:        client,
		dryRunUsers:   !cfg.ApplyUserChanges,
		dryRunGroups:  !cfg.ApplyGroupChanges,
		dryRunMembers: !cfg.ApplyMemberChanges,

		logger:  logger,
		timeout: cfg.Timeout,
		clock:   clock,

		debugUsernames:      cfg.DebugUsernames,
		debugGroupnames:     cfg.DebugGroupnames,
		sourceAttributeName: *cfg.SourceAttributeName,
	}, nil
}

func (y *Ytsaurus) GetUsers(sourceType SourceType) ([]YtsaurusUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	users, err := doGetAllYtsaurusUsers(ctx, y.client, y.sourceAttributeName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ytsaurus users")
	}
	var managedUsers []YtsaurusUser
	for _, user := range users {
		y.maybePrintExtraLogs(user.Username, "get_user", "user", user)
		if user.IsManuallyManaged(sourceType) {
			continue
		}
		managedUsers = append(managedUsers, user)
	}
	y.logger.Infow("Fetched all users from YTsaurus",
		"total", len(users),
		"managed", len(managedUsers),
	)
	return managedUsers, nil
}

func (y *Ytsaurus) CreateUser(user YtsaurusUser) error {
	if y.dryRunUsers {
		y.logger.Debugw("[DRY-RUN] Going to create user", "user", user)
		return nil
	}
	y.logger.Debugw("Going to create user", "user", user)

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(user.Username, "create_user", "user", user)

	return doCreateYtsaurusUser(
		ctx,
		y.client,
		user.Username,
		map[string]any{
			y.sourceAttributeName: user.SourceRaw,
			"source_type":         user.SourceType,
		},
	)
}

// UpdateUser handles YTsaurus user attributes update.
// In particular @name also may be changed, in that case username should be current user name.
func (y *Ytsaurus) UpdateUser(username string, user YtsaurusUser) error {
	if err := y.ensureUserManaged(username); err != nil {
		return err
	}

	logger := y.logger.With("username", username, "user", user)
	if y.dryRunUsers {
		logger.Debugw("[DRY-RUN] Going to update user")
		return nil
	}
	logger.Debugw("Going to update user")

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(username, "update_user", "username", username, "user", user)
	y.maybePrintExtraLogs(user.Username, "update_user", "username", username, "user", user)
	return doSetAttributesForYtsaurusUser(
		ctx,
		y.client,
		username,
		buildUserAttributes(user, y.sourceAttributeName),
	)
}

func (y *Ytsaurus) RemoveUser(username string) error {
	if err := y.ensureUserManaged(username); err != nil {
		return err
	}
	logger := y.logger.With("username", username)
	if y.dryRunUsers {
		logger.Debugw("[DRY-RUN] Going to remove user")
		return nil
	}
	logger.Debugw("Going to remove user")

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(username, "remove_user", "username", username)
	return y.client.RemoveNode(
		ctx,
		ypath.Path("//sys/users/"+username),
		nil,
	)
}

func (y *Ytsaurus) BanUser(username string) error {
	if err := y.ensureUserManaged(username); err != nil {
		return err
	}
	logger := y.logger.With("username", username)
	if y.dryRunUsers {
		logger.Debugw("[DRY-RUN] Going to ban user")
		return nil
	}
	logger.Debugw("Going to ban user")

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(username, "ban_user", "username", username)
	return doSetAttributesForYtsaurusUser(
		ctx,
		y.client,
		username,
		map[string]any{
			"banned":       true,
			"banned_since": y.clock.Now().UTC().Format(appTimeFormat),
		},
	)
}

func (y *Ytsaurus) GetGroupsWithMembers(sourceType SourceType) ([]YtsaurusGroupWithMembers, error) {
	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	groups, err := doGetAllYtsaurusGroupsWithMembers(ctx, y.client, y.sourceAttributeName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get ytsaurus groups")
	}
	var managedGroups []YtsaurusGroupWithMembers
	for _, group := range groups {
		y.maybePrintExtraLogs(group.Name, "get_group", "group", group)
		if group.IsManuallyManaged(sourceType) {
			continue
		}
		managedGroups = append(managedGroups, group)
	}
	y.logger.Infow("Fetched all groups from YTsaurus",
		"total", len(groups),
		"managed", len(managedGroups),
	)
	return managedGroups, nil
}

func (y *Ytsaurus) CreateGroup(group YtsaurusGroup) error {
	if y.dryRunGroups {
		y.logger.Debugw("[DRY-RUN] Going to create group", "name", group.Name)
		return nil
	}
	y.logger.Debugw("Going to create group", "name", group.Name)

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(group.Name, "create_group", "group", group)
	return doCreateYtsaurusGroup(
		ctx,
		y.client,
		group.Name,
		map[string]any{
			"source_type":         group.SourceType,
			y.sourceAttributeName: group.SourceRaw,
		},
	)
}

// UpdateGroup handles YTsaurus group attributes update.
// In particular @name also may be changed, in that case groupname should be current group name.
func (y *Ytsaurus) UpdateGroup(groupname string, group YtsaurusGroup) error {
	logger := y.logger.With("groupname", group.Name, "group", group)
	if y.dryRunGroups {
		logger.Debugw("[DRY-RUN] Going to update group")
		return nil
	}
	if err := y.ensureGroupManaged(groupname); err != nil {
		return err
	}
	logger.Debugw("Going to create group")

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(groupname, "update_group", "groupname", groupname, "group", groupname)
	y.maybePrintExtraLogs(group.Name, "update_group", "groupname", groupname, "group", group)
	return doSetAttributesForYtsaurusGroup(
		ctx,
		y.client,
		groupname,
		buildGroupAttributes(group, y.sourceAttributeName),
	)
}

func (y *Ytsaurus) RemoveGroup(groupname string) error {
	logger := y.logger.With("groupname", groupname)
	if y.dryRunGroups {
		logger.Debugw("[DRY-RUN] Going to remove group")
		return nil
	}
	if err := y.ensureGroupManaged(groupname); err != nil {
		return err
	}
	logger.Debugw("Going to remove group")

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(groupname, "remove_group", "groupname", groupname)
	return y.client.RemoveNode(
		ctx,
		ypath.Path("//sys/groups/"+groupname),
		nil,
	)
}

func (y *Ytsaurus) AddMember(username, groupname string) error {
	if y.dryRunMembers {
		y.logger.Debugw("[DRY-RUN] Going to add member", "username", username, "groupname", groupname)
		return nil
	}
	if err := y.ensureUserManaged(username); err != nil {
		return err
	}
	if err := y.ensureGroupManaged(groupname); err != nil {
		return err
	}
	y.logger.Debugw("Going to add member", "username", username, "groupname", groupname)

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(groupname, "add_member", "username", username, "groupname", groupname)
	y.maybePrintExtraLogs(username, "add_member", "username", username, "groupname", groupname)
	return doAddMemberYtsaurusGroup(ctx, y.client, username, groupname)
}

func (y *Ytsaurus) RemoveMember(username, groupname string) error {
	if y.dryRunMembers {
		y.logger.Debugw("[DRY-RUN] Going to remove member", "username", username, "groupname", groupname)
		return nil
	}
	if err := y.ensureUserManaged(username); err != nil {
		return err
	}
	if err := y.ensureGroupManaged(groupname); err != nil {
		return err
	}
	y.logger.Debugw("Going to remove member", "username", username, "groupname", groupname)

	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	y.maybePrintExtraLogs(groupname, "remove_username", "username", username, "groupname", groupname)
	y.maybePrintExtraLogs(username, "remove_username", "username", username, "groupname", groupname)
	return doRemoveMemberYtsaurusGroup(ctx, y.client, username, groupname)
}

func (y *Ytsaurus) isUserManaged(username string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	attrAzureExists, err := y.client.NodeExists(
		ctx,
		ypath.Path("//sys/users/"+username+"/@azure"),
		nil,
	)
	if err != nil {
		return false, err
	}
	attrSourceExists, err := y.client.NodeExists(
		ctx,
		ypath.Path("//sys/users/"+username+"/@source"),
		nil,
	)
	if err != nil {
		return false, err
	}
	return attrAzureExists || attrSourceExists, nil
}

func (y *Ytsaurus) ensureUserManaged(username string) error {
	isManaged, err := y.isUserManaged(username)
	if err != nil {
		return errors.Wrap(err, "Failed to check if user is managed")
	}
	if !isManaged {
		return errors.New("Prevented attempt to change manual managed user")
	}
	return nil
}

func (y *Ytsaurus) isGroupManaged(name string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), y.timeout)
	defer cancel()

	attrAzureExists, err := y.client.NodeExists(
		ctx,
		ypath.Path("//sys/groups/"+name+"/@azure"),
		nil,
	)
	if err != nil {
		return false, err
	}
	attrSourceExists, err := y.client.NodeExists(
		ctx,
		ypath.Path("//sys/groups/"+name+"/@source"),
		nil,
	)
	if err != nil {
		return false, err
	}

	return attrAzureExists || attrSourceExists, nil
}

func (y *Ytsaurus) ensureGroupManaged(groupname string) error {
	isManaged, err := y.isGroupManaged(groupname)
	if err != nil {
		return errors.Wrapf(err, "Failed to check if group %s is managed", groupname)
	}
	if !isManaged {
		return errors.New("Prevented attempt to change manual managed group" + groupname)
	}
	return nil
}

func (y *Ytsaurus) maybePrintExtraLogs(name string, event string, args ...any) {
	args = append([]any{"debug_name", name, "event", event}, args...)
	for _, debugID := range y.debugUsernames {
		if name == debugID {
			y.logger.Infow("Debug info", args...)
		}
	}
	for _, debugID := range y.debugGroupnames {
		if name == debugID {
			y.logger.Infow("Debug info", args...)
		}
	}
}

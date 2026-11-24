package app

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.ytsaurus.tech/yt/go/yson"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (a *App) syncOnce() error {
	if !a.syncMutex.TryLock() {
		return ErrSyncInProgress
	}
	defer a.syncMutex.Unlock()
	return a.syncOnceLocked()
}

func (a *App) syncOnceLocked() error {
	metrics := a.metricsRecorder()
	startedAt := time.Now()
	defer func() {
		metrics.ObserveSyncDuration(time.Since(startedAt))
	}()
	metrics.ResetYTMutationErrors()
	metrics.SyncStarted()
	a.logger.Info("Start syncing")
	defer a.logger.Info("Finish syncing")

	var partialErrors []error
	actualYtsaurusUserMap, usersErr := a.syncUsers()
	if usersErr != nil && !errors.Is(usersErr, ErrSyncPartial) {
		metrics.SyncFailed(syncStageUsers)
		return errors.Wrap(usersErr, "user sync failed")
	}
	if usersErr != nil {
		partialErrors = append(partialErrors, usersErr)
	}
	groupsErr := a.syncGroups(actualYtsaurusUserMap)
	if groupsErr != nil && !errors.Is(groupsErr, ErrSyncPartial) {
		metrics.SyncFailed(syncStageGroups)
		return errors.Wrap(groupsErr, "group sync failed")
	}
	if groupsErr != nil {
		partialErrors = append(partialErrors, groupsErr)
	}
	completedAt := time.Now()
	metrics.SetLastCompletedTimestamp(completedAt)
	if len(partialErrors) > 0 {
		metrics.SyncPartiallySucceeded()
		return newPartialSyncError(partialErrors...)
	}
	metrics.SyncSucceeded()
	metrics.SetLastSuccessTimestamp(completedAt)
	return nil
}

func (a *App) isRemoveLimitReached(objectsCount int) bool {
	if a.removeLimit <= 0 {
		return false
	}
	return objectsCount >= a.removeLimit
}

// mutationFailure holds a single failed YTsaurus mutation: the error itself and the
// zap key-value pairs describing the object which failed to sync.
type mutationFailure struct {
	err    error
	fields []any
}

// logMutationFailures reports failed mutations: a single summary message with the number
// of objects which failed to sync at error level, and one message per object with the full
// object description at debug level. This keeps the default log level free of complete
// user/group structures.
func (a *App) logMutationFailures(summary, detail string, failures []mutationFailure) {
	if len(failures) == 0 {
		return
	}
	a.logger.Errorw(summary, "count", len(failures))
	for _, failure := range failures {
		a.logger.Debugw(detail, append([]any{zap.Error(failure.err)}, failure.fields...)...)
	}
}

// syncUsers syncs AD users with YTsaurus cluster and returns the actual users map
// after applying changes.
func (a *App) syncUsers() (map[source.ObjectID]internalyt.YtsaurusUser, error) {
	a.logger.Info("Start syncing users")
	var err error
	var sourceUsers []source.SourceUser

	sourceUsers, err = a.source.GetUsers()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get Source users")
	}

	ytUsers, err := a.ytsaurus.GetUsers()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get YTsaurus users")
	}

	diff, err := a.diffUsers(sourceUsers, ytUsers)
	if err != nil {
		return nil, errors.Wrap(err, "failed to calculate users diff")
	}
	if a.isRemoveLimitReached(len(diff.remove)) {
		a.metricsRecorder().RemoveLimitExceeded(objectTypeUser)
		for _, user := range diff.remove {
			a.logger.Debugw("User planned for removal", "user", user)
		}
		return nil, fmt.Errorf("delete limit in one cycle reached: %d users planned for removal", len(diff.remove))
	}

	var bannedCount, removedCount int
	var createFailures, updateFailures, banOrRemoveFailures []mutationFailure
	metrics := a.metricsRecorder()
	for _, user := range diff.remove {
		wasBanned, wasRemoved, removeErr := a.banOrRemoveUser(user)
		if removeErr != nil {
			banOrRemoveFailures = append(banOrRemoveFailures, mutationFailure{err: removeErr, fields: []any{"user", user}})
		}
		if wasBanned {
			bannedCount++
		}
		if wasRemoved {
			removedCount++
		}
	}
	a.logMutationFailures("failed to ban or remove users", "failed to ban or remove user", banOrRemoveFailures)
	for _, user := range diff.create {
		err = a.ytsaurus.CreateUser(user)
		if err != nil {
			createFailures = append(createFailures, mutationFailure{err: err, fields: []any{"user", user}})
			metrics.YTMutationFailed(objectTypeUser, mutationCreate)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeUser, mutationCreate)
	}
	a.logMutationFailures("failed to create users", "failed to create user", createFailures)
	for _, updatedUser := range diff.update {
		err = a.ytsaurus.UpdateUser(updatedUser.OldUsername, updatedUser.YtsaurusUser)
		if err != nil {
			updateFailures = append(updateFailures, mutationFailure{err: err, fields: []any{"user", updatedUser}})
			metrics.YTMutationFailed(objectTypeUser, mutationUpdate)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeUser, mutationUpdate)
	}
	a.logMutationFailures("failed to update users", "failed to update user", updateFailures)
	a.logger.Infow("Finish syncing users",
		"created", len(diff.create)-len(createFailures),
		"create_errors", len(createFailures),
		"updated", len(diff.update)-len(updateFailures),
		"update_errors", len(updateFailures),
		"removed", removedCount,
		"banned", bannedCount,
		"ban_or_remove_errors", len(banOrRemoveFailures),
	)
	mutationErrCount := len(createFailures) + len(updateFailures) + len(banOrRemoveFailures)
	if mutationErrCount > 0 {
		return diff.result, newPartialSyncError(errors.Errorf("user sync: %d YTsaurus mutations failed", mutationErrCount))
	}
	return diff.result, nil
}

func (a *App) syncGroups(usersMap map[source.ObjectID]internalyt.YtsaurusUser) error {
	a.logger.Info("Start syncing groups")
	sourceGroups, err := a.source.GetGroupsWithMembers()
	if err != nil {
		return errors.Wrap(err, "failed to get source groups")
	}
	ytGroups, err := a.ytsaurus.GetGroupsWithMembers()
	if err != nil {
		return errors.Wrap(err, "failed to get YTsaurus groups")
	}

	diff, err := a.diffGroups(sourceGroups, ytGroups, usersMap)
	if err != nil {
		return errors.Wrap(err, "failed to calculate groups diff")
	}
	if a.isRemoveLimitReached(len(diff.groupsToRemove)) {
		a.metricsRecorder().RemoveLimitExceeded(objectTypeGroup)
		for _, group := range diff.groupsToRemove {
			a.logger.Debugw("Group planned for removal", "group", group)
		}
		return fmt.Errorf("delete limit in one cycle reached: %d groups planned for removal", len(diff.groupsToRemove))
	}

	var createFailures, updateFailures, removeFailures []mutationFailure
	metrics := a.metricsRecorder()
	for _, group := range diff.groupsToRemove {
		err = a.ytsaurus.RemoveGroup(group.Name)
		if err != nil {
			removeFailures = append(removeFailures, mutationFailure{err: err, fields: []any{"group", group}})
			metrics.YTMutationFailed(objectTypeGroup, mutationRemove)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeGroup, mutationRemove)
	}
	a.logMutationFailures("failed to remove groups", "failed to remove group", removeFailures)
	for _, group := range diff.groupsToCreate {
		err = a.ytsaurus.CreateGroup(group)
		if err != nil {
			createFailures = append(createFailures, mutationFailure{err: err, fields: []any{"group", group}})
			metrics.YTMutationFailed(objectTypeGroup, mutationCreate)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeGroup, mutationCreate)
	}
	a.logMutationFailures("failed to create groups", "failed to create group", createFailures)
	for _, updatedGroup := range diff.groupsToUpdate {
		err = a.ytsaurus.UpdateGroup(updatedGroup.OldName, updatedGroup.YtsaurusGroup)
		if err != nil {
			updateFailures = append(updateFailures, mutationFailure{err: err, fields: []any{"group", updatedGroup}})
			metrics.YTMutationFailed(objectTypeGroup, mutationUpdate)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeGroup, mutationUpdate)
	}
	a.logMutationFailures("failed to update groups", "failed to update group", updateFailures)
	a.logger.Infow("Finish syncing groups",
		"created", len(diff.groupsToCreate)-len(createFailures),
		"create_errors", len(createFailures),
		"updated", len(diff.groupsToUpdate)-len(updateFailures),
		"update_errors", len(updateFailures),
		"removed", len(diff.groupsToRemove)-len(removeFailures),
		"remove_errors", len(removeFailures),
	)
	mutationErrCount := len(createFailures) + len(updateFailures) + len(removeFailures)
	var partialErrors []error
	if mutationErrCount > 0 {
		partialErrors = append(partialErrors, errors.Errorf("group sync: %d YTsaurus mutations failed", mutationErrCount))
	}

	a.logger.Info("Start syncing group memberships")
	var addMemberFailures, removeMemberFailures []mutationFailure
	for _, membership := range diff.membersToRemove {
		err = a.ytsaurus.RemoveMember(membership.Username, membership.GroupName)
		if err != nil {
			removeMemberFailures = append(removeMemberFailures, mutationFailure{
				err:    err,
				fields: []any{"user", membership.Username, "group", membership.GroupName},
			})
			metrics.YTMutationFailed(objectTypeMembership, mutationRemove)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeMembership, mutationRemove)
	}
	a.logMutationFailures("failed to remove members", "failed to remove member", removeMemberFailures)
	for _, membership := range diff.membersToAdd {
		err = a.ytsaurus.AddMember(membership.Username, membership.GroupName)
		if err != nil {
			addMemberFailures = append(addMemberFailures, mutationFailure{
				err:    err,
				fields: []any{"user", membership.Username, "group", membership.GroupName},
			})
			metrics.YTMutationFailed(objectTypeMembership, mutationAdd)
			continue
		}
		metrics.YTMutationSucceeded(objectTypeMembership, mutationAdd)
	}
	a.logMutationFailures("failed to add members", "failed to add member", addMemberFailures)

	a.logger.Infow("Finish syncing group memberships",
		"added", len(diff.membersToAdd)-len(addMemberFailures),
		"add_errors", len(addMemberFailures),
		"removed", len(diff.membersToRemove)-len(removeMemberFailures),
		"remove_errors", len(removeMemberFailures),
	)
	mutationErrCount = len(addMemberFailures) + len(removeMemberFailures)
	if mutationErrCount > 0 {
		partialErrors = append(partialErrors, errors.Errorf("membership sync: %d YTsaurus mutations failed", mutationErrCount))
	}
	if len(partialErrors) > 0 {
		return newPartialSyncError(partialErrors...)
	}
	return nil
}

type groupDiff struct {
	groupsToCreate  []internalyt.YtsaurusGroup
	groupsToRemove  []internalyt.YtsaurusGroup
	groupsToUpdate  []UpdatedYtsaurusGroup
	membersToAdd    []internalyt.YtsaurusMembership
	membersToRemove []internalyt.YtsaurusMembership
}

func (a *App) diffGroups(
	sourceGroups []source.SourceGroupWithMembers,
	ytGroups []internalyt.YtsaurusGroupWithMembers,
	usersMap map[source.ObjectID]internalyt.YtsaurusUser,
) (*groupDiff, error) {
	var groupsToCreate, groupsToRemove []internalyt.YtsaurusGroup
	var groupsToUpdate []UpdatedYtsaurusGroup
	var membersToAdd, membersToRemove []internalyt.YtsaurusMembership

	sourceGroupsWithMembersMap := make(map[source.ObjectID]source.SourceGroupWithMembers)
	for _, group := range sourceGroups {
		sourceGroupsWithMembersMap[group.SourceGroup.GetID()] = group
	}

	ytGroupsWithMembersMap := make(map[source.ObjectID]internalyt.YtsaurusGroupWithMembers)
	for _, group := range ytGroups {
		sourceGroup, err := a.buildSourceGroup(&group)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create group from source")
		}
		ytGroupsWithMembersMap[sourceGroup.GetID()] = group
	}

	// Collecting groups to create (the ones that exist in Source but not in YTsaurus).
	for objectID, sourceGroupWithMembers := range sourceGroupsWithMembersMap {
		if _, ok := ytGroupsWithMembersMap[objectID]; !ok {
			newYtsaurusGroup, err := a.buildYtsaurusGroup(sourceGroupWithMembers.SourceGroup)
			if err != nil {
				return nil, errors.Wrap(err, "failed to build Ytsaurus group")
			}
			groupsToCreate = append(groupsToCreate, newYtsaurusGroup)
			for username := range a.buildYtsaurusGroupMembers(sourceGroupWithMembers, usersMap).Iter() {
				membersToAdd = append(membersToAdd, internalyt.YtsaurusMembership{
					GroupName: newYtsaurusGroup.Name,
					Username:  username,
				})
			}
		}
	}

	for objectID, ytGroupWithMembers := range ytGroupsWithMembersMap {
		// Collecting groups to remove (the ones that exist in YTsaurus and not in the source).
		sourceGroupWithMembers, ok := sourceGroupsWithMembersMap[objectID]
		if !ok {
			groupsToRemove = append(groupsToRemove, ytGroupWithMembers.YtsaurusGroup)
			continue
		}

		// Collecting groups with changed Source fields (actually we have only displayName for now which
		// shouldn't change, though we still handle that just in case).
		groupChanged, updatedYtGroup, err := a.isGroupChanged(sourceGroupWithMembers.SourceGroup, ytGroupWithMembers.YtsaurusGroup)
		if err != nil {
			return nil, errors.Wrap(err, "failed to check if group is changed")
		}
		// Group name can change after update, so we ensure that correct one is used for membership updates.
		actualGroupname := ytGroupWithMembers.Name
		if groupChanged {
			// This shouldn't happen until we add more fields in YTsaurus' group source attribute.
			a.logger.Warnw(
				"Detected group fields update (we handling that correctly, though case is not expected)",
				"group", ytGroupWithMembers.YtsaurusGroup.Name,
			)
			a.logger.Debugw(
				"Detected group fields update",
				"source_group", sourceGroupWithMembers.SourceGroup,
				"ytsaurus_group", ytGroupWithMembers.YtsaurusGroup,
				"updated_group", updatedYtGroup,
			)
			groupsToUpdate = append(groupsToUpdate, updatedYtGroup)
			actualGroupname = updatedYtGroup.Name
		}

		membersCreate, membersRemove := a.isGroupMembersChanged(sourceGroupWithMembers, ytGroupWithMembers, usersMap)
		for _, username := range membersCreate {
			membersToAdd = append(membersToAdd, internalyt.YtsaurusMembership{
				GroupName: actualGroupname,
				Username:  username,
			})
		}
		for _, username := range membersRemove {
			membersToRemove = append(membersToRemove, internalyt.YtsaurusMembership{
				GroupName: actualGroupname,
				Username:  username,
			})
		}
	}

	return &groupDiff{
		groupsToCreate:  groupsToCreate,
		groupsToUpdate:  groupsToUpdate,
		groupsToRemove:  groupsToRemove,
		membersToAdd:    membersToAdd,
		membersToRemove: membersToRemove,
	}, nil
}

type usersDiff struct {
	create []internalyt.YtsaurusUser
	update []UpdatedYtsaurusUser
	remove []internalyt.YtsaurusUser
	result map[source.ObjectID]internalyt.YtsaurusUser
}

func (a *App) diffUsers(
	sourceUsers []source.SourceUser,
	ytUsers []internalyt.YtsaurusUser,
) (*usersDiff, error) {
	sourceUsersMap := make(map[source.ObjectID]source.SourceUser)
	for _, user := range sourceUsers {
		if a.allowUserIDRebinding {
			if _, duplicate := sourceUsersMap[user.GetID()]; duplicate || user.GetID() == "" {
				return nil, errors.Errorf("cannot rebind users: empty or duplicate source user ID %q", user.GetID())
			}
		}
		sourceUsersMap[user.GetID()] = user
	}

	ytUsersMap := make(map[source.ObjectID]internalyt.YtsaurusUser)
	for _, user := range ytUsers {
		sourceUser, err := a.buildSourceUser(&user)
		if err != nil {
			return nil, errors.Wrap(err, "failed to build source user")
		}
		if a.allowUserIDRebinding {
			if _, duplicate := ytUsersMap[sourceUser.GetID()]; duplicate || sourceUser.GetID() == "" {
				return nil, errors.Errorf("cannot rebind users: empty or duplicate YTsaurus source user ID %q", sourceUser.GetID())
			}
		}
		ytUsersMap[sourceUser.GetID()] = user
	}

	if a.allowUserIDRebinding {
		if err := a.rebindUsersByName(sourceUsersMap, ytUsersMap); err != nil {
			return nil, err
		}
	}
	resultUsersMap := make(map[source.ObjectID]internalyt.YtsaurusUser)
	for objectID, user := range ytUsersMap {
		resultUsersMap[objectID] = user
	}

	var create, remove []internalyt.YtsaurusUser
	var update []UpdatedYtsaurusUser

	for objectID, sourceUser := range sourceUsersMap {
		if _, ok := ytUsersMap[objectID]; !ok {
			ytUser, err := a.buildYtsaurusUser(sourceUser)
			if err != nil {
				return nil, errors.Wrap(err, "failed to create Ytsaurus user from source user")
			}
			create = append(create, ytUser)
			resultUsersMap[objectID] = ytUser
		}
	}

	for objectID, ytUser := range ytUsersMap {
		sourceUser, ok := sourceUsersMap[objectID]
		if !ok {
			remove = append(remove, ytUser)
			delete(resultUsersMap, objectID)
			continue
		}
		newYtUser, err := a.buildYtsaurusUser(sourceUser)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create Ytsaurus user from source user")
		}
		userChanged, updatedYtUser, err := a.isUserChanged(newYtUser, ytUser)
		if err != nil {
			return nil, errors.Wrap(err, "failed to check if user was changed")
		}
		if !userChanged {
			continue
		}
		update = append(update, updatedYtUser)
		resultUsersMap[objectID] = updatedYtUser.YtsaurusUser
	}
	return &usersDiff{
		create: create,
		update: update,
		remove: remove,
		result: resultUsersMap,
	}, nil
}

// rebindUsersByName matches new source IDs to managed users with the same username and profile.
// It changes only the map keys: diffUsers will update the stored attributes and clear the ban.
func (a *App) rebindUsersByName(sourceUsers map[source.ObjectID]source.SourceUser, ytUsers map[source.ObjectID]internalyt.YtsaurusUser) error {
	sourceIDsByName := make(map[string]source.ObjectID)
	for objectID, user := range sourceUsers {
		username := a.buildUsername(user)
		if previousID, duplicate := sourceIDsByName[username]; duplicate {
			return errors.Errorf("cannot rebind users: source IDs %q and %q map to username %q", previousID, objectID, username)
		}
		sourceIDsByName[username] = objectID
	}
	ytIDsByName := make(map[string]source.ObjectID)
	for objectID, user := range ytUsers {
		if previousID, duplicate := ytIDsByName[user.Username]; duplicate {
			return errors.Errorf("cannot rebind users: YTsaurus source IDs %q and %q have username %q", previousID, objectID, user.Username)
		}
		ytIDsByName[user.Username] = objectID
	}

	for username, newID := range sourceIDsByName {
		// An existing match by ID always takes precedence over a match by name.
		if _, exists := ytUsers[newID]; exists {
			continue
		}
		oldID, exists := ytIDsByName[username]
		if !exists {
			continue
		}
		if _, stillPresent := sourceUsers[oldID]; stillPresent {
			return errors.Errorf("cannot rebind user %q from ID %q to %q: previous ID is still present in source", username, oldID, newID)
		}
		for field, newValue := range sourceUsers[newID].GetRebindingAttributes() {
			oldValue, oldOK := ytUsers[oldID].SourceRaw[field].(string)
			if !oldOK || newValue != oldValue {
				return errors.Errorf("cannot rebind user %q from ID %q to %q: %s is missing or does not match", username, oldID, newID, field)
			}
		}
		ytUsers[newID] = ytUsers[oldID]
		delete(ytUsers, oldID)
		a.logger.Infow("Planned user ID rebinding", "username", username, "old_source_id", oldID, "new_source_id", newID)
	}
	return nil
}

func (a *App) buildUsername(sourceUser source.SourceUser) string {
	username := sourceUser.GetName()
	if a.usernameReplaces != nil {
		for _, replace := range a.usernameReplaces {
			username = strings.ReplaceAll(username, replace.From, replace.To)
		}
	}
	username = strings.ToLower(username)
	return username
}

func (a *App) buildGroupName(sourceGroup source.SourceGroup) string {
	name := sourceGroup.GetName()
	if a.groupnameReplaces != nil {
		for _, replace := range a.groupnameReplaces {
			name = strings.ReplaceAll(name, replace.From, replace.To)
		}
	}
	name = strings.ToLower(name)
	return name
}

func (a *App) buildSourceUser(ytUser *internalyt.YtsaurusUser) (source.SourceUser, error) {
	if ytUser.IsManuallyManaged() {
		return nil, errors.New("user is manually managed and can't be converted to source user")
	}
	return a.source.CreateUserFromRaw(ytUser.SourceRaw)
}

func (a *App) buildSourceGroup(ytGroup *internalyt.YtsaurusGroupWithMembers) (source.SourceGroup, error) {
	if ytGroup.IsManuallyManaged() {
		return nil, errors.New("group is manually managed and can't be converted to source group")
	}
	return a.source.CreateGroupFromRaw(ytGroup.SourceRaw)
}

func (a *App) buildYtsaurusUser(sourceUser source.SourceUser) (internalyt.YtsaurusUser, error) {
	sourceRaw, err := sourceUser.GetRaw()
	if err != nil {
		return internalyt.YtsaurusUser{}, err
	}
	return internalyt.YtsaurusUser{
		Username:  a.buildUsername(sourceUser),
		SourceRaw: sourceRaw,
		// If we have Source user —> he is not banned.
		BannedSince: time.Time{},
	}, nil
}

func (a *App) buildYtsaurusGroup(sourceGroup source.SourceGroup) (internalyt.YtsaurusGroup, error) {
	sourceRaw, err := sourceGroup.GetRaw()
	if err != nil {
		return internalyt.YtsaurusGroup{}, err
	}

	return internalyt.YtsaurusGroup{
		Name:      a.buildGroupName(sourceGroup),
		SourceRaw: sourceRaw,
	}, nil
}

func (a *App) buildYtsaurusGroupMembers(sourceGroupWithMembers source.SourceGroupWithMembers, usersMap map[source.ObjectID]internalyt.YtsaurusUser) mapset.Set[string] {
	members := NewStringSet()
	for sourceID := range sourceGroupWithMembers.Members.Iter() {
		ytUser, ok := usersMap[sourceID]
		if !ok {
			// User is unknown to the YTsaurus (can be accountEnabled=false).
			continue
		}
		members.Add(ytUser.Username)
	}
	return members
}

// UpdatedYtsaurusUser is a wrapper for a YTsaurus user, because it is handy to store old username for update,
// because usernames can be changed.
type UpdatedYtsaurusUser struct {
	internalyt.YtsaurusUser
	OldUsername string
}

// If isUserChanged detects that user is changed, it returns UpdatedYtsaurusUser.
func (a *App) isUserChanged(newYtUser internalyt.YtsaurusUser, ytUser internalyt.YtsaurusUser) (bool, UpdatedYtsaurusUser, error) {
	newSourceRaw, err := yson.Marshal(newYtUser.SourceRaw)
	if err != nil {
		return false, UpdatedYtsaurusUser{}, err
	}
	oldSourceRaw, err := yson.Marshal(ytUser.SourceRaw)
	if err != nil {
		return false, UpdatedYtsaurusUser{}, err
	}
	if newYtUser.Username == ytUser.Username && bytes.Equal(newSourceRaw, oldSourceRaw) && newYtUser.BannedSince.Equal(ytUser.BannedSince) {
		return false, UpdatedYtsaurusUser{}, nil
	}
	return true, UpdatedYtsaurusUser{YtsaurusUser: newYtUser, OldUsername: ytUser.Username}, nil
}

// UpdatedYtsaurusGroup is a wrapper for a YTsaurus group, because it is handy to store old groupname for update,
// because groupnames can be changed.
type UpdatedYtsaurusGroup struct {
	internalyt.YtsaurusGroup
	OldName string
}

// If isGroupChanged detects that group itself (not members) is changed, it returns UpdatedYtsaurusGroup.
func (a *App) isGroupChanged(sourceGroup source.SourceGroup, ytGroup internalyt.YtsaurusGroup) (bool, UpdatedYtsaurusGroup, error) {
	newGroup, err := a.buildYtsaurusGroup(sourceGroup)
	if err != nil {
		return false, UpdatedYtsaurusGroup{}, errors.Wrap(err, "failed to build Ytsaurus group")
	}
	newSourceRaw, err := yson.Marshal(newGroup.SourceRaw)
	if err != nil {
		return false, UpdatedYtsaurusGroup{}, err
	}
	oldSourceRaw, err := yson.Marshal(ytGroup.SourceRaw)
	if err != nil {
		return false, UpdatedYtsaurusGroup{}, err
	}
	if bytes.Equal(newSourceRaw, oldSourceRaw) {
		return false, UpdatedYtsaurusGroup{}, nil
	}
	a.logger.Debugw(
		"Group is changed",
		"sourceGroup", sourceGroup,
		"ytGroup", ytGroup,
		"newGroup", newGroup,
		"newSourceRaw", string(newSourceRaw),
		"oldSourceRaw", string(oldSourceRaw),
	)
	return true, UpdatedYtsaurusGroup{YtsaurusGroup: newGroup, OldName: ytGroup.Name}, nil
}

// If isGroupMembersChanged detects that group members are changed, it returns lists of usernames to create and remove.
func (a *App) isGroupMembersChanged(sourceGroup source.SourceGroupWithMembers, ytGroup internalyt.YtsaurusGroupWithMembers, usersMap map[source.ObjectID]internalyt.YtsaurusUser) (create, remove []string) {
	newMembers := a.buildYtsaurusGroupMembers(sourceGroup, usersMap)
	oldMembers := ytGroup.Members

	create = newMembers.Difference(oldMembers).ToSlice()
	remove = oldMembers.Difference(newMembers).ToSlice()
	return
}

func (a *App) banOrRemoveUser(user internalyt.YtsaurusUser) (wasBanned, wasRemoved bool, err error) {
	// Ban settings is disabled.
	if a.banDuration == 0 {
		err = a.ytsaurus.RemoveUser(user.Username)
		if err != nil {
			a.metricsRecorder().YTMutationFailed(objectTypeUser, mutationRemove)
			return false, false, err
		}
		a.metricsRecorder().YTMutationSucceeded(objectTypeUser, mutationRemove)
		return false, true, nil
	}
	// If user is not already banned we should do it.
	if !user.IsBanned() {
		err = a.ytsaurus.BanUser(user.Username)
		if err != nil {
			a.metricsRecorder().YTMutationFailed(objectTypeUser, mutationBan)
			return false, false, err
		}
		a.metricsRecorder().YTMutationSucceeded(objectTypeUser, mutationBan)
		return true, false, nil
	}
	// If user was banned longer than setting permits, we remove it.
	if user.IsBanned() && time.Since(user.BannedSince) > a.banDuration {
		err = a.ytsaurus.RemoveUser(user.Username)
		if err != nil {
			a.metricsRecorder().YTMutationFailed(objectTypeUser, mutationRemove)
			return false, false, err
		}
		a.metricsRecorder().YTMutationSucceeded(objectTypeUser, mutationRemove)
		return false, true, nil
	}
	a.logger.Debugw("user is banned, but not yet removed", "user", user.Username, "since", user.BannedSince)
	return false, false, nil
}

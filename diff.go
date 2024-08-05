package main

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"go.ytsaurus.tech/yt/go/yson"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type SourceUser interface {
	GetID() ObjectID
	GetName() string
	GetRaw() (map[string]any, error)
}

type SourceGroup interface {
	GetID() ObjectID
	GetName() string
	GetRaw() (map[string]any, error)
}

type SourceGroupWithMembers struct {
	SourceGroup SourceGroup
	// Members is a set of strings, representing users' ObjectID.
	Members StringSet
}

func (a *App) syncOnce() {
	a.logger.Info("Start syncing")
	defer a.logger.Info("Finish syncing")

	actualYtsaurusUserMap, err := a.syncUsers()
	if err != nil {
		a.logger.Error("user sync failed", zap.Error(err))
		return
	}
	err = a.syncGroups(actualYtsaurusUserMap)
	if err != nil {
		a.logger.Error("group sync failed", zap.Error(err))
	}
}

func (a *App) isRemoveLimitReached(objectsCount int) bool {
	if a.removeLimit <= 0 {
		return false
	}
	return objectsCount >= a.removeLimit
}

// syncUsers syncs AD users with YTsaurus cluster and returns /actual/ map[ObjectID]YtsaurusUser
// after applying changes.
func (a *App) syncUsers() (map[ObjectID]YtsaurusUser, error) {
	a.logger.Info("Start syncing users")
	var err error
	var sourceUsers []SourceUser

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
		return nil, fmt.Errorf("delete limit in one cycle reached: %d %v", len(diff.remove), diff.remove)
	}

	var bannedCount, removedCount int
	var createErrCount, updateErrCount, banOrremoveErrCount int
	for _, user := range diff.remove {
		wasBanned, wasRemoved, removeErr := a.banOrRemoveUser(user)
		if removeErr != nil {
			banOrremoveErrCount++
			a.logger.Errorw("failed to ban or remove user", zap.Error(err), "user", user)
		}
		if wasBanned {
			bannedCount++
		}
		if wasRemoved {
			removedCount++
		}
	}
	for _, user := range diff.create {
		err = a.ytsaurus.CreateUser(user)
		if err != nil {
			createErrCount++
			a.logger.Errorw("failed to create user", zap.Error(err), "user", user)
		}
	}
	for _, updatedUser := range diff.update {
		err = a.ytsaurus.UpdateUser(updatedUser.OldUsername, updatedUser.YtsaurusUser)
		if err != nil {
			updateErrCount++
			a.logger.Errorw("failed to update user", zap.Error(err), "user", updatedUser)
		}
	}
	a.logger.Infow("Finish syncing users",
		"created", len(diff.create)-createErrCount,
		"create_errors", createErrCount,
		"updated", len(diff.update)-updateErrCount,
		"update_errors", updateErrCount,
		"removed", removedCount,
		"banned", bannedCount,
		"ban_or_remove_errors", banOrremoveErrCount,
	)
	return diff.result, nil
}

func (a *App) syncGroups(usersMap map[ObjectID]YtsaurusUser) error {
	a.logger.Info("Start syncing groups")
	azureGroups, err := a.source.GetGroupsWithMembers()
	if err != nil {
		return errors.Wrap(err, "failed to get Source groups")
	}
	ytGroups, err := a.ytsaurus.GetGroupsWithMembers()
	if err != nil {
		return errors.Wrap(err, "failed to get YTsaurus groups")
	}

	diff, err := a.diffGroups(azureGroups, ytGroups, usersMap)
	if err != nil {
		return errors.Wrap(err, "failed to calculate groups diff")
	}
	if a.isRemoveLimitReached(len(diff.groupsToRemove)) {
		return fmt.Errorf("delete limit in one cycle reached: %d %v", len(diff.groupsToRemove), diff)
	}

	var createErrCount, updateErrCount, removeErrCount int
	for _, group := range diff.groupsToRemove {
		err = a.ytsaurus.RemoveGroup(group.Name)
		if err != nil {
			removeErrCount++
			a.logger.Errorw("failed to remove group", zap.Error(err), "group", group)
		}
	}
	for _, group := range diff.groupsToCreate {
		err = a.ytsaurus.CreateGroup(group)
		if err != nil {
			createErrCount++
			a.logger.Errorw("failed to create group", zap.Error(err), "group", group)
		}
	}
	for _, updatedGroup := range diff.groupsToUpdate {
		err = a.ytsaurus.UpdateGroup(updatedGroup.OldName, updatedGroup.YtsaurusGroup)
		if err != nil {
			updateErrCount++
			a.logger.Errorw("failed to update group", zap.Error(err), "group", updatedGroup)
		}
	}
	a.logger.Infow("Finish syncing groups",
		"created", len(diff.groupsToCreate)-createErrCount,
		"create_errors", createErrCount,
		"updated", len(diff.groupsToUpdate)-updateErrCount,
		"update_errors", updateErrCount,
		"removed", len(diff.groupsToRemove)-removeErrCount,
		"remove_errors", removeErrCount,
	)

	a.logger.Info("Start syncing group memberships")
	var addMemberErrCount, removeMemberErrCount int
	for _, membership := range diff.membersToRemove {
		err = a.ytsaurus.RemoveMember(membership.Username, membership.GroupName)
		if err != nil {
			removeMemberErrCount++
			a.logger.Errorw("failed to remove member", zap.Error(err), "user", membership.Username, "group", membership.GroupName)
			// TODO: alerts
		}
	}
	for _, membership := range diff.membersToAdd {
		err = a.ytsaurus.AddMember(membership.Username, membership.GroupName)
		if err != nil {
			addMemberErrCount++
			a.logger.Errorw("failed to add member", zap.Error(err), "user", membership.Username, "group", membership.GroupName)
			// TODO: alerts
		}
	}

	a.logger.Infow("Finish syncing group memberships",
		"added", len(diff.membersToAdd)-addMemberErrCount,
		"add_errors", addMemberErrCount,
		"removed", len(diff.membersToRemove)-removeMemberErrCount,
		"remove_errors", removeMemberErrCount,
	)
	return nil
}

type groupDiff struct {
	groupsToCreate  []YtsaurusGroup
	groupsToRemove  []YtsaurusGroup
	groupsToUpdate  []UpdatedYtsaurusGroup
	membersToAdd    []YtsaurusMembership
	membersToRemove []YtsaurusMembership
}

func (a *App) diffGroups(
	sourceGroups []SourceGroupWithMembers,
	ytGroups []YtsaurusGroupWithMembers,
	usersMap map[ObjectID]YtsaurusUser,
) (*groupDiff, error) {
	var groupsToCreate, groupsToRemove []YtsaurusGroup
	var groupsToUpdate []UpdatedYtsaurusGroup
	var membersToAdd, membersToRemove []YtsaurusMembership

	sourceGroupsWithMembersMap := make(map[ObjectID]SourceGroupWithMembers)
	for _, group := range sourceGroups {
		sourceGroupsWithMembersMap[group.SourceGroup.GetID()] = group
	}

	ytGroupsWithMembersMap := make(map[ObjectID]YtsaurusGroupWithMembers)
	for _, group := range ytGroups {
		sourceGroup, err := a.buildSourceGroup(&group)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create azure group from source")
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
				membersToAdd = append(membersToAdd, YtsaurusMembership{
					GroupName: newYtsaurusGroup.Name,
					Username:  username,
				})
			}
		}
	}

	for objectID, ytGroupWithMembers := range ytGroupsWithMembersMap {
		// Collecting groups to remove (the ones that exist in YTsaurus and not in Azure).
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
		actualGroupname := ytGroupWithMembers.YtsaurusGroup.Name
		if groupChanged {
			// This shouldn't happen until we add more fields in YTsaurus' group @azure attribute.
			a.logger.Warnw(
				"Detected group fields update (we handling that correctly, though case is not expected)",
				"source_group", sourceGroupWithMembers.SourceGroup,
				"ytsaurus_group", ytGroupWithMembers.YtsaurusGroup,
				"updated_group", updatedYtGroup,
			)
			groupsToUpdate = append(groupsToUpdate, updatedYtGroup)
			actualGroupname = updatedYtGroup.YtsaurusGroup.Name
		}

		membersCreate, membersRemove := a.isGroupMembersChanged(sourceGroupWithMembers, ytGroupWithMembers, usersMap)
		for _, username := range membersCreate {
			membersToAdd = append(membersToAdd, YtsaurusMembership{
				GroupName: actualGroupname,
				Username:  username,
			})
		}
		for _, username := range membersRemove {
			membersToRemove = append(membersToRemove, YtsaurusMembership{
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
	create []YtsaurusUser
	update []UpdatedYtsaurusUser
	remove []YtsaurusUser
	result map[ObjectID]YtsaurusUser
}

func (a *App) diffUsers(
	sourceUsers []SourceUser,
	ytUsers []YtsaurusUser,
) (*usersDiff, error) {
	sourceUsersMap := make(map[ObjectID]SourceUser)
	for _, user := range sourceUsers {
		sourceUsersMap[user.GetID()] = user
	}

	ytUsersMap := make(map[ObjectID]YtsaurusUser)
	resultUsersMap := make(map[ObjectID]YtsaurusUser)
	for _, user := range ytUsers {
		sourceUser, err := a.buildSourceUser(&user)
		if err != nil {
			return nil, errors.Wrap(err, "failed to build source user")
		}
		ytUsersMap[sourceUser.GetID()] = user
		resultUsersMap[sourceUser.GetID()] = user
	}

	var create, remove []YtsaurusUser
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

func (a *App) buildUsername(sourceUser SourceUser) string {
	username := sourceUser.GetName()
	if a.usernameReplaces != nil {
		for _, replace := range a.usernameReplaces {
			username = strings.Replace(username, replace.From, replace.To, -1)
		}
	}
	username = strings.ToLower(username)
	return username
}

func (a *App) buildGroupName(sourceGroup SourceGroup) string {
	name := sourceGroup.GetName()
	if a.groupnameReplaces != nil {
		for _, replace := range a.groupnameReplaces {
			name = strings.Replace(name, replace.From, replace.To, -1)
		}
	}
	name = strings.ToLower(name)
	return name
}

func (a *App) buildSourceUser(ytUser *YtsaurusUser) (SourceUser, error) {
	if ytUser.IsManuallyManaged() {
		return nil, errors.New("user is manually managed and can't be converted to source user")
	}
	return a.source.CreateUserFromRaw(ytUser.SourceRaw)
}

func (a *App) buildSourceGroup(ytGroup *YtsaurusGroupWithMembers) (SourceUser, error) {
	if ytGroup.IsManuallyManaged() {
		return nil, errors.New("user is manually managed and can't be converted to source user")
	}
	return a.source.CreateGroupFromRaw(ytGroup.SourceRaw)
}

func (a *App) buildYtsaurusUser(sourceUser SourceUser) (YtsaurusUser, error) {
	sourceRaw, err := sourceUser.GetRaw()
	if err != nil {
		return YtsaurusUser{}, err
	}
	return YtsaurusUser{
		Username:  a.buildUsername(sourceUser),
		SourceRaw: sourceRaw,
		// If we have Source user —> he is not banned.
		BannedSince: time.Time{},
	}, nil
}

func (a *App) buildYtsaurusGroup(sourceGroup SourceGroup) (YtsaurusGroup, error) {
	sourceRaw, err := sourceGroup.GetRaw()
	if err != nil {
		return YtsaurusGroup{}, err
	}

	return YtsaurusGroup{
		Name:      a.buildGroupName(sourceGroup),
		SourceRaw: sourceRaw,
	}, nil
}

func (a *App) buildYtsaurusGroupMembers(sourceGroupWithMembers SourceGroupWithMembers, usersMap map[ObjectID]YtsaurusUser) StringSet {
	members := NewStringSet()
	for azureID := range sourceGroupWithMembers.Members.Iter() {
		ytUser, ok := usersMap[azureID]
		if !ok {
			// User is unknown to the YTsaurus (can be accountEnabled=false).
			continue
		}
		members.Add(ytUser.Username)
	}
	return members
}

// UpdatedYtsaurusUser is a wrapper for YtsaurusUser, because it is handy to store old username for update,
// because usernames can be changed.
type UpdatedYtsaurusUser struct {
	YtsaurusUser
	OldUsername string
}

// If isUserChanged detects that user is changed, it returns UpdatedYtsaurusUser.
func (a *App) isUserChanged(newYtUser YtsaurusUser, ytUser YtsaurusUser) (bool, UpdatedYtsaurusUser, error) {
	newSourceRaw, err := yson.Marshal(newYtUser.SourceRaw)
	if err != nil {
		return false, UpdatedYtsaurusUser{}, err
	}
	oldSourceRaw, err := yson.Marshal(ytUser.SourceRaw)
	if err != nil {
		return false, UpdatedYtsaurusUser{}, err
	}
	if newYtUser.Username == ytUser.Username && bytes.Equal(newSourceRaw, oldSourceRaw) && newYtUser.BannedSince == ytUser.BannedSince {
		return false, UpdatedYtsaurusUser{}, nil
	}
	return true, UpdatedYtsaurusUser{YtsaurusUser: newYtUser, OldUsername: ytUser.Username}, nil
}

// UpdatedYtsaurusGroup is a wrapper for YtsaurusGroup, because it is handy to store old groupname for update,
// because groupnames can be changed.
type UpdatedYtsaurusGroup struct {
	YtsaurusGroup
	OldName string
}

// If isGroupChanged detects that group itself (not members) is changed, it returns UpdatedYtsaurusGroup.
func (a *App) isGroupChanged(sourceGroup SourceGroup, ytGroup YtsaurusGroup) (bool, UpdatedYtsaurusGroup, error) {
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
func (a *App) isGroupMembersChanged(sourceGroup SourceGroupWithMembers, ytGroup YtsaurusGroupWithMembers, usersMap map[ObjectID]YtsaurusUser) (create, remove []string) {
	newMembers := a.buildYtsaurusGroupMembers(sourceGroup, usersMap)
	oldMembers := ytGroup.Members

	create = newMembers.Difference(oldMembers).ToSlice()
	remove = oldMembers.Difference(newMembers).ToSlice()
	return
}

func (a *App) banOrRemoveUser(user YtsaurusUser) (wasBanned, wasRemoved bool, err error) {
	// Ban settings is disabled.
	if a.banDuration == 0 {
		return false, true, a.ytsaurus.RemoveUser(user.Username)
	}
	// If user is not already banned we should do it.
	if !user.IsBanned() {
		return true, false, a.ytsaurus.BanUser(user.Username)
	}
	// If user was banned longer than setting permits, we remove it.
	if user.IsBanned() && time.Since(user.BannedSince) > a.banDuration {
		return false, true, a.ytsaurus.RemoveUser(user.Username)
	}
	a.logger.Debugw("user is banned, but not yet removed", "user", user.Username, "since", user.BannedSince)
	return false, false, nil
}

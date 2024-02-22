package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

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
	if a.removeLimit == nil || *a.removeLimit <= 0 {
		return false
	}
	return objectsCount >= *a.removeLimit
}

// syncUsers syncs AD users with YTsaurus cluster and returns /actual/ map[ObjectID]YtsaurusUser
// after applying changes.
func (a *App) syncUsers() (map[ObjectID]YtsaurusUser, error) {
	a.logger.Info("Start syncing users")
	var err error
	var sourceUsers []SourceUser

	if a.source != nil {
		sourceUsers, err = a.source.GetUsers()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get Source users")
		}
	}

	ytUsers, err := a.ytsaurus.GetUsers(a.source.GetSourceType())
	if err != nil {
		return nil, errors.Wrap(err, "failed to get YTsaurus users")
	}

	sourceUsersMap := make(map[ObjectID]SourceUser)
	ytUsersMap := make(map[ObjectID]YtsaurusUser)

	for _, user := range sourceUsers {
		sourceUsersMap[user.GetID()] = user
	}
	for _, user := range ytUsers {
		ytUsersMap[user.SourceUser.GetID()] = user
	}

	diff := a.diffUsers(sourceUsersMap, ytUsersMap)
	if a.isRemoveLimitReached(len(diff.remove)) {
		return nil, fmt.Errorf("delete limit in one cycle reached: %d %v", len(diff.remove), diff)
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
		// Actualizing user map for group sync later.
		delete(ytUsersMap, user.SourceUser.GetID())
	}
	for _, user := range diff.create {
		err = a.ytsaurus.CreateUser(user)
		if err != nil {
			createErrCount++
			a.logger.Errorw("failed to create user", zap.Error(err), "user", user)
		}
		// Actualizing user map for group sync later.
		ytUsersMap[user.SourceUser.GetID()] = user
	}
	for _, updatedUser := range diff.update {
		err = a.ytsaurus.UpdateUser(updatedUser.OldUsername, updatedUser.YtsaurusUser)
		if err != nil {
			updateErrCount++
			a.logger.Errorw("failed to update user", zap.Error(err), "user", updatedUser)
		}
		// Actualizing user map for group sync later.
		ytUsersMap[updatedUser.SourceUser.GetID()] = updatedUser.YtsaurusUser
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
	return ytUsersMap, nil
}

func (a *App) syncGroups(usersMap map[ObjectID]YtsaurusUser) error {
	a.logger.Info("Start syncing groups")
	azureGroups, err := a.source.GetGroupsWithMembers()
	if err != nil {
		return errors.Wrap(err, "failed to get Source groups")
	}
	ytGroups, err := a.ytsaurus.GetGroupsWithMembers(a.source.GetSourceType())
	if err != nil {
		return errors.Wrap(err, "failed to get YTsaurus groups")
	}

	diff := a.diffGroups(azureGroups, ytGroups, usersMap)
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
) groupDiff {
	var groupsToCreate, groupsToRemove []YtsaurusGroup
	var groupsToUpdate []UpdatedYtsaurusGroup
	var membersToAdd, membersToRemove []YtsaurusMembership

	sourceGroupsWithMembersMap := make(map[ObjectID]SourceGroupWithMembers)
	ytGroupsWithMembersMap := make(map[ObjectID]YtsaurusGroupWithMembers)

	for _, group := range sourceGroups {
		sourceGroupsWithMembersMap[group.SourceGroup.GetID()] = group
	}
	for _, group := range ytGroups {
		ytGroupsWithMembersMap[group.SourceGroup.GetID()] = group
	}

	// Collecting groups to create (the ones that exist in Source but not in YTsaurus).
	for objectID, sourceGroupWithMembers := range sourceGroupsWithMembersMap {
		if _, ok := ytGroupsWithMembersMap[objectID]; !ok {
			newYtsaurusGroup := a.buildYtsaurusGroup(sourceGroupWithMembers.SourceGroup)
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
		// Collecting groups to remove (the ones that exist in YTsaurus and not in Source).
		sourceGroupWithMembers, ok := sourceGroupsWithMembersMap[objectID]
		if !ok {
			groupsToRemove = append(groupsToRemove, ytGroupWithMembers.YtsaurusGroup)
			continue
		}

		// Collecting groups with changed Source fields (actually we have only displayName for now which
		// should change, though we still handle that just in case).
		groupChanged, updatedYtGroup := a.isGroupChanged(sourceGroupWithMembers.SourceGroup, ytGroupWithMembers.YtsaurusGroup)
		// Group name can change after update, so we ensure that correct one is used for membership updates.
		actualGroupname := ytGroupWithMembers.YtsaurusGroup.Name
		if groupChanged {
			// This shouldn't happen until we add more fields in YTsaurus' group @azure attribute.
			a.logger.Warnw(
				"Detected group fields update (we handling that correctly, though case is not expected)",
				"group", updatedYtGroup,
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

	return groupDiff{
		groupsToCreate:  groupsToCreate,
		groupsToUpdate:  groupsToUpdate,
		groupsToRemove:  groupsToRemove,
		membersToAdd:    membersToAdd,
		membersToRemove: membersToRemove,
	}
}

type usersDiff struct {
	create []YtsaurusUser
	update []UpdatedYtsaurusUser
	remove []YtsaurusUser
}

func (a *App) diffUsers(
	sourceUsersMap map[ObjectID]SourceUser,
	ytUsersMap map[ObjectID]YtsaurusUser,
) usersDiff {
	var create, remove []YtsaurusUser
	var update []UpdatedYtsaurusUser

	for objectID, sourceUser := range sourceUsersMap {
		if _, ok := ytUsersMap[objectID]; !ok {
			create = append(create, a.buildYtsaurusUser(sourceUser))
		}
	}
	for objectID, ytUser := range ytUsersMap {
		sourceUser, ok := sourceUsersMap[objectID]
		if !ok {
			remove = append(remove, ytUser)
			continue
		}
		userChanged, updatedYtUser := a.isUserChanged(sourceUser, ytUser)
		if !userChanged {
			continue
		}
		update = append(update, updatedYtUser)
	}
	return usersDiff{
		create: create,
		update: update,
		remove: remove,
	}
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

func (a *App) buildYtsaurusUser(sourceUser SourceUser) YtsaurusUser {
	return YtsaurusUser{
		Username:   a.buildUsername(sourceUser),
		SourceUser: sourceUser,
		// If we have Source user —> he is not banned.
		BannedSince: time.Time{},
	}
}

func (a *App) buildYtsaurusGroup(sourceGroup SourceGroup) YtsaurusGroup {
	return YtsaurusGroup{
		Name:        a.buildGroupName(sourceGroup),
		SourceGroup: sourceGroup,
	}
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
func (a *App) isUserChanged(sourceUser SourceUser, ytUser YtsaurusUser) (bool, UpdatedYtsaurusUser) {
	newUser := a.buildYtsaurusUser(sourceUser)
	if newUser == ytUser {
		return false, UpdatedYtsaurusUser{}
	}
	return true, UpdatedYtsaurusUser{YtsaurusUser: newUser, OldUsername: ytUser.Username}
}

// UpdatedYtsaurusGroup is a wrapper for YtsaurusGroup, because it is handy to store old groupname for update,
// because groupnames can be changed.
type UpdatedYtsaurusGroup struct {
	YtsaurusGroup
	OldName string
}

// If isGroupChanged detects that group itself (not members) is changed, it returns UpdatedYtsaurusGroup.
func (a *App) isGroupChanged(sourceGroup SourceGroup, ytGroup YtsaurusGroup) (bool, UpdatedYtsaurusGroup) {
	newGroup := a.buildYtsaurusGroup(sourceGroup)
	if newGroup == ytGroup {
		return false, UpdatedYtsaurusGroup{}
	}
	return true, UpdatedYtsaurusGroup{YtsaurusGroup: newGroup, OldName: ytGroup.Name}
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
	if a.banDuration == nil {
		return false, true, a.ytsaurus.RemoveUser(user.Username)
	}
	// If user is not already banned we should do it.
	if !user.IsBanned() && *a.banDuration != 0 {
		return true, false, a.ytsaurus.BanUser(user.Username)
	}
	// If user was banned longer than setting permits, we remove it.
	if user.IsBanned() && time.Since(user.BannedSince) > *a.banDuration {
		return false, true, a.ytsaurus.RemoveUser(user.Username)
	}
	a.logger.Debugw("user is banned, but not yet removed", "user", user.Username, "since", user.BannedSince)
	return false, false, nil
}

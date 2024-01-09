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
	if a.removeLimit <= 0 {
		return false
	}
	return objectsCount >= a.removeLimit
}

// syncUsers syncs AD users with YTsaurus cluster and returns /actual/ map[AzureID]YtsaurusUser
// after applying changes.
func (a *App) syncUsers() (map[AzureID]YtsaurusUser, error) {
	a.logger.Info("Start syncing users")
	azureUsers, err := a.azure.GetUsers()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get Azure users")
	}
	ytUsers, err := a.ytsaurus.GetUsers()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get YTsaurus users")
	}

	azureUsersMap := make(map[AzureID]AzureUser)
	ytUsersMap := make(map[AzureID]YtsaurusUser)

	for _, user := range azureUsers {
		azureUsersMap[user.AzureID] = user
	}
	for _, user := range ytUsers {
		ytUsersMap[user.AzureID] = user
	}

	diff := a.diffUsers(azureUsersMap, ytUsersMap)
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
		delete(ytUsersMap, user.AzureID)
	}
	for _, user := range diff.create {
		err = a.ytsaurus.CreateUser(user)
		if err != nil {
			createErrCount++
			a.logger.Errorw("failed to create user", zap.Error(err), "user", user)
		}
		// Actualizing user map for group sync later.
		ytUsersMap[user.AzureID] = user
	}
	for _, updatedUser := range diff.update {
		err = a.ytsaurus.UpdateUser(updatedUser.OldUsername, updatedUser.YtsaurusUser)
		if err != nil {
			updateErrCount++
			a.logger.Errorw("failed to update user", zap.Error(err), "user", updatedUser)
		}
		// Actualizing user map for group sync later.
		ytUsersMap[updatedUser.AzureID] = updatedUser.YtsaurusUser
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

func (a *App) syncGroups(usersMap map[AzureID]YtsaurusUser) error {
	a.logger.Info("Start syncing groups")
	azureGroups, err := a.azure.GetGroupsWithMembers()
	if err != nil {
		return errors.Wrap(err, "failed to get Azure groups")
	}
	ytGroups, err := a.ytsaurus.GetGroupsWithMembers()
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
	azureGroups []AzureGroupWithMembers,
	ytGroups []YtsaurusGroupWithMembers,
	usersMap map[AzureID]YtsaurusUser,
) groupDiff {
	var groupsToCreate, groupsToRemove []YtsaurusGroup
	var groupsToUpdate []UpdatedYtsaurusGroup
	var membersToAdd, membersToRemove []YtsaurusMembership

	azureGroupsWithMembersMap := make(map[AzureID]AzureGroupWithMembers)
	ytGroupsWithMembersMap := make(map[AzureID]YtsaurusGroupWithMembers)

	for _, group := range azureGroups {
		azureGroupsWithMembersMap[group.AzureID] = group
	}
	for _, group := range ytGroups {
		ytGroupsWithMembersMap[group.AzureID] = group
	}

	// Collecting groups to create (the ones that exist in Azure but not in YTsaurus).
	for azureID, azureGroupWithMembers := range azureGroupsWithMembersMap {
		if _, ok := ytGroupsWithMembersMap[azureID]; !ok {
			newYtsaurusGroup := a.buildYtsaurusGroup(azureGroupWithMembers.AzureGroup)
			groupsToCreate = append(groupsToCreate, newYtsaurusGroup)
			for username := range a.buildYtsaurusGroupMembers(azureGroupWithMembers, usersMap).Iter() {
				membersToAdd = append(membersToAdd, YtsaurusMembership{
					GroupName: newYtsaurusGroup.Name,
					Username:  username,
				})
			}
		}
	}

	for azureID, ytGroupWithMembers := range ytGroupsWithMembersMap {
		// Collecting groups to remove (the ones that exist in YTsaurus and not in Azure).
		azureGroupWithMembers, ok := azureGroupsWithMembersMap[azureID]
		if !ok {
			groupsToRemove = append(groupsToRemove, ytGroupWithMembers.YtsaurusGroup)
			continue
		}

		// Collecting groups with changed Azure fields (actually we have only displayName for now which
		// should change, though we still handle that just in case).
		groupChanged, updatedYtGroup := a.isGroupChanged(azureGroupWithMembers.AzureGroup, ytGroupWithMembers.YtsaurusGroup)
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

		membersCreate, membersRemove := a.isGroupMembersChanged(azureGroupWithMembers, ytGroupWithMembers, usersMap)
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
	azureUsersMap map[AzureID]AzureUser,
	ytUsersMap map[AzureID]YtsaurusUser,
) usersDiff {
	var create, remove []YtsaurusUser
	var update []UpdatedYtsaurusUser

	for azureID, azureUser := range azureUsersMap {
		if _, ok := ytUsersMap[azureID]; !ok {
			create = append(create, a.buildYtsaurusUser(azureUser))
		}
	}
	for azureID, ytUser := range ytUsersMap {
		azureUser, ok := azureUsersMap[azureID]
		if !ok {
			remove = append(remove, ytUser)
			continue
		}
		userChanged, updatedYtUser := a.isUserChanged(azureUser, ytUser)
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

func (a *App) buildUsername(azureUser AzureUser) string {
	username := azureUser.PrincipalName
	for _, replace := range a.usernameReplaces {
		username = strings.Replace(username, replace.From, replace.To, -1)
	}
	username = strings.ToLower(username)
	return username
}

func (a *App) buildGroupName(azureGroup AzureGroup) string {
	name := azureGroup.Identity
	for _, replace := range a.groupnameReplaces {
		name = strings.Replace(name, replace.From, replace.To, -1)
	}
	name = strings.ToLower(name)
	return name
}

func (a *App) buildYtsaurusUser(azureUser AzureUser) YtsaurusUser {
	return YtsaurusUser{
		Username:      a.buildUsername(azureUser),
		AzureID:       azureUser.AzureID,
		PrincipalName: azureUser.PrincipalName,
		Email:         azureUser.Email,
		FirstName:     azureUser.FirstName,
		LastName:      azureUser.LastName,
		DisplayName:   azureUser.DisplayName,
		// If we have Azure user —> he is not banned.
		BannedSince: time.Time{},
	}
}

func (a *App) buildYtsaurusGroup(azureGroup AzureGroup) YtsaurusGroup {
	return YtsaurusGroup{
		Name:        a.buildGroupName(azureGroup),
		AzureID:     azureGroup.AzureID,
		DisplayName: azureGroup.DisplayName,
	}
}

func (a *App) buildYtsaurusGroupMembers(azureGroupWithMembers AzureGroupWithMembers, usersMap map[AzureID]YtsaurusUser) StringSet {
	members := NewStringSet()
	for azureID := range azureGroupWithMembers.Members.Iter() {
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
func (a *App) isUserChanged(azureUser AzureUser, ytUser YtsaurusUser) (bool, UpdatedYtsaurusUser) {
	newUser := a.buildYtsaurusUser(azureUser)
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
func (a *App) isGroupChanged(azureGroup AzureGroup, ytGroup YtsaurusGroup) (bool, UpdatedYtsaurusGroup) {
	newGroup := a.buildYtsaurusGroup(azureGroup)
	if newGroup.Name == ytGroup.Name && newGroup.DisplayName == ytGroup.DisplayName {
		return false, UpdatedYtsaurusGroup{}
	}
	return true, UpdatedYtsaurusGroup{YtsaurusGroup: newGroup, OldName: ytGroup.Name}
}

// If isGroupMembersChanged detects that group members are changed, it returns lists of usernames to create and remove.
func (a *App) isGroupMembersChanged(azureGroup AzureGroupWithMembers, ytGroup YtsaurusGroupWithMembers, usersMap map[AzureID]YtsaurusUser) (create, remove []string) {
	newMembers := a.buildYtsaurusGroupMembers(azureGroup, usersMap)
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
	if !user.IsBanned() && a.banDuration != 0 {
		return true, false, a.ytsaurus.BanUser(user.Username)
	}
	// If user was banned longer than setting permits, we remove it.
	if user.IsBanned() && time.Since(user.BannedSince) > a.banDuration {
		return false, true, a.ytsaurus.RemoveUser(user.Username)
	}
	a.logger.Debugw("user is banned, but not yet removed", "user", user.Username, "since", user.BannedSince)
	return false, false, nil
}

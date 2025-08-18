package main

type AzureFake struct {
	users            []SourceUser
	groups           []SourceGroupWithMembers
	userGroupsFilter string
}

func NewAzureFake() *AzureFake {
	return &AzureFake{}
}

func NewAzureFakeWithConfig(cfg *AzureConfig) *AzureFake {
	return &AzureFake{
		userGroupsFilter: cfg.UserGroupsFilter,
	}
}

func (a *AzureFake) setUsers(users []SourceUser) {
	a.users = users
}

func (a *AzureFake) setGroups(groups []SourceGroupWithMembers) {
	a.groups = groups
}

func (a *AzureFake) CreateUserFromRaw(raw map[string]any) (SourceUser, error) {
	return NewAzureUser(raw)
}

func (a *AzureFake) CreateGroupFromRaw(raw map[string]any) (SourceGroup, error) {
	return NewAzureGroup(raw)
}

func (a *AzureFake) GetUsers() ([]SourceUser, error) {
	users := a.users
	
	// Apply user groups filter if specified
	if a.userGroupsFilter != "" {
		users = a.filterUsersByGroupMembership(users)
	}
	
	return users, nil
}

func (a *AzureFake) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	return a.groups, nil
}

func (a *AzureFake) filterUsersByGroupMembership(users []SourceUser) []SourceUser {
	if a.userGroupsFilter == "" {
		return users
	}

	// Collect all member IDs from groups matching the filter
	allowedUserIDs := NewStringSet()
	for _, group := range a.groups {
		groupID := group.SourceGroup.GetID()
		displayName := group.SourceGroup.GetName()
		
		// Simple filter matching - in real implementation this would be MS Graph filter
		// For testing, we'll match by display name containing the filter string
		if a.matchesFilter(groupID, displayName, a.userGroupsFilter) {
			for _, memberID := range group.Members.ToSlice() {
				allowedUserIDs.Add(memberID)
			}
		}
	}

	// Filter users to only include those who are members of matching groups
	var filteredUsers []SourceUser
	for _, user := range users {
		userID := user.GetID()
		if allowedUserIDs.Contains(userID) {
			filteredUsers = append(filteredUsers, user)
		}
	}

	return filteredUsers
}

func (a *AzureFake) matchesFilter(groupID, displayName, filter string) bool {
	// Simple filter implementation for testing
	// In real implementation this would be MS Graph OData filter
	// For simplicity, match if filter contains the display name or ID
	return groupID == filter || displayName == filter || 
		   (filter != "" && (groupID == filter || displayName == filter))
}

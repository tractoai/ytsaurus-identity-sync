package main

type AzureFake struct {
	users  []SourceUser
	groups []SourceGroupWithMembers
}

func NewAzureFake() *AzureFake {
	return &AzureFake{}
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
	return a.users, nil
}

func (a *AzureFake) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	return a.groups, nil
}

// GetUsersByGroups returns users that belong to the specified groups
func (a *AzureFake) GetUsersByGroups(groups []SourceGroupWithMembers) ([]SourceUser, error) {
	// Extract all unique user IDs from the groups
	userIDs := NewStringSet()
	for _, group := range groups {
		for userID := range group.Members.Iter() {
			userIDs.Add(userID)
		}
	}

	// Filter users that are in the groups
	var filteredUsers []SourceUser
	for _, user := range a.users {
		if userIDs.Contains(user.GetID()) {
			filteredUsers = append(filteredUsers, user)
		}
	}

	return filteredUsers, nil
}

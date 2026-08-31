package azure

import "github.com/tractoai/ytsaurus-identity-sync/pkg/source"

type AzureFake struct {
	users  []source.SourceUser
	groups []source.SourceGroupWithMembers
}

func NewAzureFake() *AzureFake {
	return &AzureFake{}
}

func (a *AzureFake) SetUsers(users []source.SourceUser) {
	a.users = users
}

func (a *AzureFake) SetGroups(groups []source.SourceGroupWithMembers) {
	a.groups = groups
}

func (a *AzureFake) CreateUserFromRaw(raw map[string]any) (source.SourceUser, error) {
	return NewAzureUser(raw)
}

func (a *AzureFake) CreateGroupFromRaw(raw map[string]any) (source.SourceGroup, error) {
	return NewAzureGroup(raw)
}

func (a *AzureFake) GetUsers() ([]source.SourceUser, error) {
	return a.users, nil
}

func (a *AzureFake) GetGroupsWithMembers() ([]source.SourceGroupWithMembers, error) {
	return a.groups, nil
}

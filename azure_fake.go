package main

type AzureFake struct {
	users  []AzureUser
	groups []AzureGroupWithMembers
}

func NewAzureFake() *AzureFake {
	return &AzureFake{}
}

func (a *AzureFake) setUsers(users []AzureUser) {
	a.users = users
}

func (a *AzureFake) setGroups(groups []AzureGroupWithMembers) {
	a.groups = groups
}

func (a *AzureFake) GetUsers() ([]AzureUser, error) {
	return a.users, nil
}

func (a *AzureFake) GetGroupsWithMembers() ([]AzureGroupWithMembers, error) {
	return a.groups, nil
}

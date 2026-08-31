package app

import (
	"fmt"

	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
)

type fakeSource struct {
	users  []source.SourceUser
	groups []source.SourceGroupWithMembers
}

func (f *fakeSource) GetUsers() ([]source.SourceUser, error) {
	return f.users, nil
}

func (f *fakeSource) GetGroupsWithMembers() ([]source.SourceGroupWithMembers, error) {
	return f.groups, nil
}

func (f *fakeSource) CreateUserFromRaw(raw map[string]any) (source.SourceUser, error) {
	id, idOK := raw["id"].(string)
	name, nameOK := raw["name"].(string)
	if !idOK || !nameOK {
		return nil, fmt.Errorf("invalid source user raw value: %v", raw)
	}
	return fakeUser{
		ID:        id,
		Name:      name,
		Email:     stringValue(raw, "email"),
		FirstName: stringValue(raw, "first_name"),
	}, nil
}

func (f *fakeSource) CreateGroupFromRaw(raw map[string]any) (source.SourceGroup, error) {
	id, idOK := raw["id"].(string)
	name, nameOK := raw["name"].(string)
	if !idOK || !nameOK {
		return nil, fmt.Errorf("invalid source group raw value: %v", raw)
	}
	return fakeGroup{ID: id, Name: name}, nil
}

type fakeUser struct {
	ID        source.ObjectID
	Name      string
	Email     string
	FirstName string
}

func (u fakeUser) GetID() source.ObjectID {
	return u.ID
}

func (u fakeUser) GetName() string {
	return u.Name
}

func (u fakeUser) GetRaw() (map[string]any, error) {
	return map[string]any{
		"id":         u.ID,
		"name":       u.Name,
		"email":      u.Email,
		"first_name": u.FirstName,
	}, nil
}

type fakeGroup struct {
	ID   source.ObjectID
	Name string
}

func (g fakeGroup) GetID() source.ObjectID {
	return g.ID
}

func (g fakeGroup) GetName() string {
	return g.Name
}

func (g fakeGroup) GetRaw() (map[string]any, error) {
	return map[string]any{
		"id":   g.ID,
		"name": g.Name,
	}, nil
}

func stringValue(raw map[string]any, key string) string {
	value, _ := raw[key].(string)
	return value
}

func sourceUsers(users ...fakeUser) []source.SourceUser {
	result := make([]source.SourceUser, 0, len(users))
	for _, user := range users {
		result = append(result, user)
	}
	return result
}

func sourceGroup(group fakeGroup, members ...source.ObjectID) source.SourceGroupWithMembers {
	return source.SourceGroupWithMembers{
		SourceGroup: group,
		Members:     NewStringSetFromItems(members...),
	}
}

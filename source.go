package main

import (
	"fmt"

	"go.ytsaurus.tech/yt/go/yson"
)

type ObjectID = string
type SourceType string

const (
	LdapSourceType  SourceType = "ldap"
	AzureSourceType SourceType = "azure"
)

type Source interface {
	GetUsers() ([]SourceUser, error)
	GetGroupsWithMembers() ([]SourceGroupWithMembers, error)
	GetSourceType() SourceType
}

type SourceUser interface {
	GetID() ObjectID
	GetName() string
	GetSourceType() SourceType
}

func NewSourceUser(sourceType SourceType, attributes map[string]any) (SourceUser, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	switch sourceType {
	case LdapSourceType:
		var ldapUser LdapUser
		err = yson.Unmarshal(bytes, &ldapUser)
		if err != nil {
			return nil, err
		}
		return ldapUser, nil
	case AzureSourceType:
		var azureUser AzureUser
		err = yson.Unmarshal(bytes, &azureUser)
		if err != nil {
			return nil, err
		}
		return azureUser, nil
	}
	return nil, fmt.Errorf("unknown source type: %v", sourceType)
}

type AzureUser struct {
	// PrincipalName is unique human-readable Azure user field, used (possibly with changes)
	// for the corresponding YTsaurus user's `name` attribute.
	PrincipalName string `yson:"principal_name"`

	AzureID     ObjectID `yson:"id"`
	Email       string   `yson:"email"`
	FirstName   string   `yson:"first_name"`
	LastName    string   `yson:"last_name"`
	DisplayName string   `yson:"display_name"`
}

func (user AzureUser) GetID() ObjectID {
	return user.AzureID
}

func (user AzureUser) GetName() string {
	return user.PrincipalName
}

func (user AzureUser) GetSourceType() SourceType {
	return AzureSourceType
}

type LdapUser struct {
	Username  string `yson:"username"`
	UID       string `yson:"uid"`
	FirstName string `yson:"first_name"`
	// TODO(nadya73): Add more fields.
}

func (user LdapUser) GetID() ObjectID {
	return user.UID
}

func (user LdapUser) GetName() string {
	return user.Username
}

func (user LdapUser) GetSourceType() SourceType {
	return LdapSourceType
}

type SourceGroup interface {
	GetID() ObjectID
	GetName() string
	GetSourceType() SourceType
}

func NewSourceGroup(sourceType SourceType, attributes map[string]any) (SourceGroup, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	switch sourceType {
	case LdapSourceType:
		var ldapGroup LdapGroup
		err = yson.Unmarshal(bytes, &ldapGroup)
		if err != nil {
			return nil, err
		}
		return ldapGroup, nil
	case AzureSourceType:
		var azureGroup AzureGroup
		err = yson.Unmarshal(bytes, &azureGroup)
		if err != nil {
			return nil, err
		}
		return azureGroup, nil
	}
	return nil, fmt.Errorf("unknown source type: %v", sourceType)
}

type AzureGroup struct {
	// Identity is unique human-readable Source user field, used (possibly with changes)
	// for the corresponding YTsaurus user's `name` attribute.
	Identity string `yson:"identity"`

	AzureID     ObjectID `yson:"id"`
	DisplayName string   `yson:"display_name"`
}

func (ag AzureGroup) GetID() ObjectID {
	return ag.AzureID
}

func (ag AzureGroup) GetName() string {
	return ag.Identity
}

func (ag AzureGroup) GetSourceType() SourceType {
	return AzureSourceType
}

type LdapGroup struct {
	Groupname string `yson:"groupname"`
}

func (lg LdapGroup) GetID() ObjectID {
	return lg.Groupname
}

func (lg LdapGroup) GetName() string {
	return lg.Groupname
}

func (lg LdapGroup) GetSourceType() SourceType {
	return LdapSourceType
}

type SourceGroupWithMembers struct {
	SourceGroup SourceGroup
	// Members is a set of strings, representing users' ObjectID.
	Members StringSet
}

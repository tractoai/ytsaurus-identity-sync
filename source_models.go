package main

import (
	"fmt"
	"github.com/pkg/errors"
	"go.ytsaurus.tech/yt/go/yson"
)

type ObjectID = string
type SourceType string

const (
	LdapSourceType  SourceType = "ldap"
	AzureSourceType SourceType = "azure"
)

type SourceUser interface {
	GetId() ObjectID
	GetName() string
	GetSourceType() SourceType
}

func NewSourceUser(attributes map[string]any) (SourceUser, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	sourceType := attributes["source_type"]
	if sourceType == string(LdapSourceType) {
		var ldapUser LdapUser
		err = yson.Unmarshal(bytes, &ldapUser)
		if err != nil {
			return nil, err
		}
		return ldapUser, nil
	} else if sourceType == string(AzureSourceType) {
		var azureUser AzureUser
		err = yson.Unmarshal(bytes, &azureUser)
		if err != nil {
			return nil, err
		}
		return azureUser, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Unknown source type: %v", sourceType))
	}
}

type BasicSourceUser struct {
	SourceType SourceType `yson:"source_type"`
}

type AzureUser struct {
	BasicSourceUser
	// PrincipalName is unique human-readable Azure user field, used (possibly with changes)
	// for the corresponding YTsaurus user's `name` attribute.
	PrincipalName string `yson:"principal_name"`

	AzureID     ObjectID `yson:"id"`
	Email       string   `yson:"email"`
	FirstName   string   `yson:"first_name"`
	LastName    string   `yson:"last_name"`
	DisplayName string   `yson:"display_name"`
}

func (user AzureUser) GetId() ObjectID {
	return user.AzureID
}

func (user AzureUser) GetName() string {
	return user.PrincipalName
}

func (user AzureUser) GetSourceType() SourceType {
	return AzureSourceType
}

type LdapUser struct {
	BasicSourceUser
	Username  string `yson:"username"`
	Uid       string `yson:"uid"`
	FirstName string `yson:"first_name"`
	// TODO(nadya73): Add more fields.
}

func (user LdapUser) GetId() ObjectID {
	return user.Uid
}

func (user LdapUser) GetName() string {
	return user.Username
}

func (user LdapUser) GetSourceType() SourceType {
	return LdapSourceType
}

type SourceGroup interface {
	GetId() ObjectID
	GetName() string
	GetSourceType() SourceType
}

type BasicSourceGroup struct {
	SourceType SourceType `yson:"source_type"`
}

func NewSourceGroup(attributes map[string]any) (SourceGroup, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	sourceType := attributes["source_type"]
	if sourceType == string(LdapSourceType) {
		var ldapGroup LdapGroup
		err = yson.Unmarshal(bytes, &ldapGroup)
		if err != nil {
			return nil, err
		}
		return ldapGroup, nil
	} else if sourceType == string(AzureSourceType) {
		var azureGroup AzureGroup
		err = yson.Unmarshal(bytes, &azureGroup)
		if err != nil {
			return nil, err
		}
		return azureGroup, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Unknown source type: %v", sourceType))
	}
}

type AzureGroup struct {
	BasicSourceGroup
	// Identity is unique human-readable Source user field, used (possibly with changes)
	// for the corresponding YTsaurus user's `name` attribute.
	Identity string `yson:"identity"`

	AzureID     ObjectID `yson:"id"`
	DisplayName string   `yson:"display_name"`
}

func (ag AzureGroup) GetId() ObjectID {
	return ag.AzureID
}

func (ag AzureGroup) GetName() string {
	return ag.Identity
}

func (ag AzureGroup) GetSourceType() SourceType {
	return AzureSourceType
}

type LdapGroup struct {
	BasicSourceGroup
	Groupname string `yson:"groupname"`
}

func (lg LdapGroup) GetId() ObjectID {
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

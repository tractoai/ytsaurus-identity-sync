package main

import "go.ytsaurus.tech/yt/go/yson"

type LdapUser struct {
	Username  string `yson:"username"`
	UID       string `yson:"uid"`
	FirstName string `yson:"first_name"`
	// TODO(nadya73): Add more fields.
}

func NewLdapUser(attributes map[string]any) (*LdapUser, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	var user LdapUser
	err = yson.Unmarshal(bytes, &user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

func (lu LdapUser) GetID() ObjectID {
	return lu.UID
}

func (lu LdapUser) GetName() string {
	return lu.Username
}

func (lu LdapUser) GetRaw() (map[string]any, error) {
	bytes, err := yson.Marshal(lu)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]any)
	err = yson.Unmarshal(bytes, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

type LdapGroup struct {
	Groupname string `yson:"groupname"`
}

func NewLdapGroup(attributes map[string]any) (*LdapGroup, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	var group LdapGroup
	err = yson.Unmarshal(bytes, &group)
	if err != nil {
		return nil, err
	}
	return &group, nil
}

func (lg LdapGroup) GetID() ObjectID {
	return lg.Groupname
}

func (lg LdapGroup) GetName() string {
	return lg.Groupname
}

func (lg LdapGroup) GetRaw() (map[string]any, error) {
	bytes, err := yson.Marshal(lg)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]any)
	err = yson.Unmarshal(bytes, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

package main

import (
	"log"

	"github.com/go-ldap/ldap/v3"
	"k8s.io/utils/env"
)

type Ldap struct {
	Connection *ldap.Conn
	Config     *LdapConfig
}

func NewLdap(cfg *LdapConfig, logger appLoggerType) (*Ldap, error) {
	conn, err := ldap.DialURL(cfg.Address)
	if err != nil {
		log.Fatalf("Failed to connect: %s\n", err)
		return nil, err
	}

	_, err = conn.SimpleBind(&ldap.SimpleBindRequest{
		Username: cfg.BindDN,
		Password: env.GetString(cfg.BindPasswordEnvVar, "adminpassword"),
	})
	if err != nil {
		log.Fatalf("Failed to bind: %s\n", err)
		return nil, err
	}
	return &Ldap{
		Connection: conn,
		Config:     cfg,
	}, nil
}

func (source *Ldap) GetSourceType() SourceType {
	return LdapSourceType
}

func (source *Ldap) GetUsers() ([]SourceUser, error) {
	res, err := source.Connection.Search(&ldap.SearchRequest{
		BaseDN:     source.Config.BaseDN,
		Filter:     source.Config.Users.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var users []SourceUser
	for _, entry := range res.Entries {
		username := entry.GetAttributeValue(source.Config.Users.UsernameAttributeType)
		uid := entry.GetAttributeValue(source.Config.Users.UIDAttributeType)
		var firstName string
		if source.Config.Users.FirstNameAttributeType != nil {
			firstName = entry.GetAttributeValue(*source.Config.Users.FirstNameAttributeType)
		}
		users = append(users, LdapUser{
			Username:  username,
			UID:       uid,
			FirstName: firstName})
	}
	return users, nil
}

func (source *Ldap) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	res, err := source.Connection.Search(&ldap.SearchRequest{
		BaseDN:     source.Config.BaseDN,
		Filter:     source.Config.Groups.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var groups []SourceGroupWithMembers
	for _, entry := range res.Entries {
		groupname := entry.GetAttributeValue(source.Config.Groups.GroupnameAttributeType)
		members := entry.GetAttributeValues(source.Config.Groups.MemberUIDAttributeType)
		groups = append(groups, SourceGroupWithMembers{
			SourceGroup: LdapGroup{
				Groupname: groupname,
			},
			Members: NewStringSetFromItems(members...),
		})
	}
	return groups, nil
}

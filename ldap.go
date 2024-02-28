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

func (l *Ldap) GetSourceType() SourceType {
	return LdapSourceType
}

func (l *Ldap) GetUsers() ([]SourceUser, error) {
	res, err := l.Connection.Search(&ldap.SearchRequest{
		BaseDN:     l.Config.BaseDN,
		Filter:     l.Config.Users.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var users []SourceUser
	for _, entry := range res.Entries {
		username := entry.GetAttributeValue(l.Config.Users.UsernameAttributeType)
		uid := entry.GetAttributeValue(l.Config.Users.UIDAttributeType)
		var firstName string
		if l.Config.Users.FirstNameAttributeType != nil {
			firstName = entry.GetAttributeValue(*l.Config.Users.FirstNameAttributeType)
		}
		users = append(users, LdapUser{
			Username:  username,
			UID:       uid,
			FirstName: firstName})
	}
	return users, nil
}

func (l *Ldap) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	res, err := l.Connection.Search(&ldap.SearchRequest{
		BaseDN:     l.Config.BaseDN,
		Filter:     l.Config.Groups.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var groups []SourceGroupWithMembers
	for _, entry := range res.Entries {
		groupname := entry.GetAttributeValue(l.Config.Groups.GroupnameAttributeType)
		members := entry.GetAttributeValues(l.Config.Groups.MemberUIDAttributeType)
		groups = append(groups, SourceGroupWithMembers{
			SourceGroup: LdapGroup{
				Groupname: groupname,
			},
			Members: NewStringSetFromItems(members...),
		})
	}
	return groups, nil
}

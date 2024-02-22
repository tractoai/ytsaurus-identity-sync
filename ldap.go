package main

import (
	"github.com/go-ldap/ldap/v3"
	"k8s.io/utils/env"
	"log"
)

type Ldap struct {
	Connection *ldap.Conn
	Config     *LdapConfig
}

func NewLdap(cfg *LdapConfig, logger appLoggerType) (*Ldap, error) {
	conn, err := ldap.DialURL(cfg.Address)
	if err != nil {
		log.Fatalf("Failed to connect: %s\n", err)
	}

	_, err = conn.SimpleBind(&ldap.SimpleBindRequest{
		Username: cfg.BindDN,
		Password: env.GetString(cfg.BindPasswordEnvVar, "adminpassword"),
	})
	return &Ldap{
		Connection: conn,
		Config:     cfg,
	}, nil
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
		uid := entry.GetAttributeValue(source.Config.Users.UidAttributeType)
		var firstName string
		if source.Config.Users.FirstNameAttributeType != nil {
			firstName = entry.GetAttributeValue(*source.Config.Users.FirstNameAttributeType)
		}
		users = append(users, LdapUser{
			BasicSourceUser: BasicSourceUser{SourceType: LdapSourceType},
			Username:        username,
			Uid:             uid,
			FirstName:       firstName})
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
		members := entry.GetAttributeValues(source.Config.Groups.MemberUidAttributeType)
		groups = append(groups, SourceGroupWithMembers{
			SourceGroup: LdapGroup{
				BasicSourceGroup: BasicSourceGroup{SourceType: LdapSourceType},
				Groupname:        groupname,
			},
			Members: NewStringSetFromItems(members...),
		})
	}
	return groups, nil
}

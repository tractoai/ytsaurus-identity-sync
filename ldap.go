package main

import (
	"errors"

	"github.com/go-ldap/ldap/v3"
	"k8s.io/utils/env"
)

type Ldap struct {
	connection *ldap.Conn
	config     *LdapConfig
	logger     appLoggerType
}

func NewLdap(cfg *LdapConfig, logger appLoggerType) (*Ldap, error) {
	conn, err := ldap.DialURL(cfg.Address)
	if err != nil {
		logger.Fatalf("Failed to connect: %s\n", err)
		return nil, err
	}

	_, err = conn.SimpleBind(&ldap.SimpleBindRequest{
		Username: cfg.BindDN,
		Password: env.GetString(cfg.BindPasswordEnvVar, "adminpassword"),
	})
	if err != nil {
		logger.Fatalf("Failed to bind: %s\n", err)
		return nil, err
	}
	return &Ldap{
		connection: conn,
		config:     cfg,
		logger:     logger,
	}, nil
}

func (l *Ldap) CreateUserFromRaw(raw map[string]any) (SourceUser, error) {
	return NewLdapUser(raw)
}

func (l *Ldap) CreateGroupFromRaw(raw map[string]any) (SourceGroup, error) {
	return NewLdapGroup(raw)
}

func (l *Ldap) GetUsers() ([]SourceUser, error) {
	res, err := l.connection.Search(&ldap.SearchRequest{
		BaseDN:     l.config.BaseDN,
		Filter:     l.config.Users.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var users []SourceUser
	for _, entry := range res.Entries {
		username := entry.GetAttributeValue(l.config.Users.UsernameAttributeType)
		uid := entry.GetAttributeValue(l.config.Users.UIDAttributeType)
		var firstName string
		if l.config.Users.FirstNameAttributeType != nil {
			firstName = entry.GetAttributeValue(*l.config.Users.FirstNameAttributeType)
		}
		users = append(users, LdapUser{
			Username:  username,
			UID:       uid,
			FirstName: firstName})
	}
	return users, nil
}

func (l *Ldap) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	res, err := l.connection.Search(&ldap.SearchRequest{
		BaseDN:     l.config.BaseDN,
		Filter:     l.config.Groups.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var groups []SourceGroupWithMembers
	for _, entry := range res.Entries {
		groupname := entry.GetAttributeValue(l.config.Groups.GroupnameAttributeType)
		members := entry.GetAttributeValues(l.config.Groups.MemberUIDAttributeType)
		groups = append(groups, SourceGroupWithMembers{
			SourceGroup: LdapGroup{
				Groupname: groupname,
			},
			Members: NewStringSetFromItems(members...),
		})
	}
	return groups, nil
}

// GetUsersByGroups is not supported for LDAP source
func (l *Ldap) GetUsersByGroups(groups []SourceGroupWithMembers) ([]SourceUser, error) {
	return nil, errors.New("groups-first sync strategy is not supported for LDAP source")
}


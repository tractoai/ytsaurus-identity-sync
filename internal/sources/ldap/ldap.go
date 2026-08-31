package ldap

import (
	"github.com/go-ldap/ldap/v3"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"
	"k8s.io/utils/env"
)

type Ldap struct {
	connection *ldap.Conn
	config     *config.LdapConfig
	logger     *zap.SugaredLogger
}

func NewLdap(cfg *config.LdapConfig, logger *zap.SugaredLogger) (*Ldap, error) {
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

func (l *Ldap) CreateUserFromRaw(raw map[string]any) (source.SourceUser, error) {
	return NewLdapUser(raw)
}

func (l *Ldap) CreateGroupFromRaw(raw map[string]any) (source.SourceGroup, error) {
	return NewLdapGroup(raw)
}

func (l *Ldap) GetUsers() ([]source.SourceUser, error) {
	res, err := l.connection.Search(&ldap.SearchRequest{
		BaseDN:     l.config.BaseDN,
		Filter:     l.config.Users.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var users []source.SourceUser
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

func (l *Ldap) GetGroupsWithMembers() ([]source.SourceGroupWithMembers, error) {
	res, err := l.connection.Search(&ldap.SearchRequest{
		BaseDN:     l.config.BaseDN,
		Filter:     l.config.Groups.Filter,
		Attributes: []string{"*"},
		Scope:      ldap.ScopeWholeSubtree,
	})
	if err != nil {
		return nil, err
	}

	var groups []source.SourceGroupWithMembers
	for _, entry := range res.Entries {
		groupname := entry.GetAttributeValue(l.config.Groups.GroupnameAttributeType)
		members := entry.GetAttributeValues(l.config.Groups.MemberUIDAttributeType)
		groups = append(groups, source.SourceGroupWithMembers{
			SourceGroup: LdapGroup{
				Groupname: groupname,
			},
			Members: NewStringSetFromItems(members...),
		})
	}
	return groups, nil
}

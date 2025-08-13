package main

import (
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

// GetUsersByGroups returns users that belong to the specified groups
func (l *Ldap) GetUsersByGroups(groups []SourceGroupWithMembers) ([]SourceUser, error) {
	// Extract all unique user UIDs from the groups
	userUIDs := NewStringSet()
	for _, group := range groups {
		for userUID := range group.Members.Iter() {
			userUIDs.Add(userUID)
		}
	}

	if userUIDs.Cardinality() == 0 {
		l.logger.Info("No users found in the provided groups")
		return []SourceUser{}, nil
	}

	// Convert to slice and build filter
	userUIDSlice := userUIDs.ToSlice()
	l.logger.Infow("Fetching users from groups", "user_count", len(userUIDSlice))

	// Build LDAP filter to get users with specific UIDs
	// Example: (|(uid=user1)(uid=user2)(uid=user3))
	var filterParts []string
	for _, uid := range userUIDSlice {
		filterParts = append(filterParts, "("+l.config.Users.UIDAttributeType+"="+uid+")")
	}
	
	var filter string
	if len(filterParts) == 1 {
		filter = filterParts[0]
	} else {
		filter = "(|" + joinStrings(filterParts, "") + ")"
	}

	// Also combine with the existing user filter if present
	if l.config.Users.Filter != "" {
		filter = "(&" + l.config.Users.Filter + filter + ")"
	}

	res, err := l.connection.Search(&ldap.SearchRequest{
		BaseDN:     l.config.BaseDN,
		Filter:     filter,
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
			FirstName: firstName,
		})
	}

	l.logger.Infow("Fetched users by groups", "fetched", len(users))
	return users, nil
}

// Helper function to join strings
func joinStrings(parts []string, separator string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += separator + parts[i]
	}
	return result
}

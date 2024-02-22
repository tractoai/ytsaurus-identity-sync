package main

import (
	"time"
)

type YtsaurusUser struct {
	// Username is a unique @name attribute of a user.
	Username    string
	SourceUser  SourceUser
	BannedSince time.Time
}

func (u YtsaurusUser) GetSourceAttributeName() string {
	switch u.SourceUser.GetSourceType() {
	case AzureSourceType:
		return "azure"
	case LdapSourceType:
		return "source"
	}
	return "source"
}

// IsManuallyManaged true if user doesn't have @azure attribute (system or manually created user).
func (u YtsaurusUser) IsManuallyManaged() bool {
	return u.SourceUser == nil
}

func (u YtsaurusUser) IsBanned() bool {
	return !u.BannedSince.IsZero()
}

func (u YtsaurusUser) BannedSinceString() string {
	if u.BannedSince.IsZero() {
		return ""
	}
	return u.BannedSince.Format(appTimeFormat)
}

type YtsaurusGroup struct {
	// Name is a unique @name attribute of a group.
	Name        string
	SourceGroup SourceGroup
}

func (g YtsaurusGroup) GetSourceAttributeName() string {
	switch g.SourceGroup.GetSourceType() {
	case AzureSourceType:
		return "azure"
	case LdapSourceType:
		return "source"
	}
	return "source"
}

type YtsaurusGroupWithMembers struct {
	YtsaurusGroup
	// Members is a set of group members' @name attribute.
	Members StringSet
}

func NewEmptyYtsaurusGroupWithMembers(group YtsaurusGroup) YtsaurusGroupWithMembers {
	return YtsaurusGroupWithMembers{YtsaurusGroup: group, Members: NewStringSet()}
}

type YtsaurusMembership struct {
	GroupName string
	Username  string
}

// IsManuallyManaged true if group doesn't have @azure attribute (system or manually created group).
func (u YtsaurusGroup) IsManuallyManaged() bool {
	return u.SourceGroup == nil
}

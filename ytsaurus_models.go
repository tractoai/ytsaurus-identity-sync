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
	}
	return "source"
}

// IsManuallyManaged true if user doesn't have @azure attribute (system or manually created user).
func (u YtsaurusUser) IsManuallyManaged(sourceType SourceType) bool {
	return u.SourceUser == nil || u.SourceUser.GetSourceType() != sourceType
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
	}
	return "source"
}

// IsManuallyManaged true if group doesn't have @azure attribute (system or manually created group).
func (g YtsaurusGroup) IsManuallyManaged(sourceType SourceType) bool {
	return g.SourceGroup == nil || g.SourceGroup.GetSourceType() != sourceType
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

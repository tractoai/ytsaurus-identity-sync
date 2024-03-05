package main

import (
	"time"
)

type SourceUser interface {
	GetID() ObjectID
	GetName() string
	GetSourceType() SourceType
	GetRaw() (map[string]any, error)
}

type SourceGroup interface {
	GetID() ObjectID
	GetName() string
	GetSourceType() SourceType
	GetRaw() (map[string]any, error)
}

type SourceGroupWithMembers struct {
	SourceGroup SourceGroup
	// Members is a set of strings, representing users' ObjectID.
	Members StringSet
}

type YtsaurusUser struct {
	// Username is a unique @name attribute of a user.
	Username    string
	SourceType  *string
	SourceRaw   map[string]any
	BannedSince time.Time
}

// IsManuallyManaged true if user doesn't have @azure attribute (system or manually created user).
func (u YtsaurusUser) IsManuallyManaged(sourceType SourceType) bool {
	return u.SourceRaw == nil || u.SourceType == nil || *u.SourceType != string(sourceType)
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
	Name       string
	SourceType *string
	SourceRaw  map[string]any
}

// IsManuallyManaged true if group doesn't have @azure attribute (system or manually created group).
func (g YtsaurusGroup) IsManuallyManaged(sourceType SourceType) bool {
	return g.SourceRaw == nil || g.SourceType == nil || *g.SourceType != string(sourceType)
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

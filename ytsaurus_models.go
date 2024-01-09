package main

import (
	"time"
)

type YtsaurusUser struct {
	// Username is a unique @name attribute of a user.
	Username string
	// AzureID is non-human readable string like 2cd8a70c-9044-4488-b06a-c8461c39b296.
	AzureID string
	// PrincipalName is a unique human-readable login.
	// It could be in form of email, but doesn't give guarantee that such email exists.
	PrincipalName string
	// Email is filled if Azure user has email.
	Email       string
	FirstName   string
	LastName    string
	DisplayName string
	BannedSince time.Time
}

// IsManuallyManaged true if user doesn't have @azure attribute (system or manually created user).
func (u YtsaurusUser) IsManuallyManaged() bool {
	return u.AzureID == ""
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
	AzureID     string
	DisplayName string
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
	return u.AzureID == ""
}

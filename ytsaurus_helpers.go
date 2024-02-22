package main

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// Lower level functions for reusing in tests.

func doGetAllYtsaurusUsers(ctx context.Context, client yt.Client) ([]YtsaurusUser, error) {
	type YtsaurusUserResponse struct {
		Name        string         `yson:",value"`
		Azure       *AzureUser     `yson:"azure,attr"`
		Source      map[string]any `yson:"source,attr"`
		Banned      bool           `yson:"banned,attr"`
		BannedSince string         `yson:"banned_since,attr"`
	}

	var response []YtsaurusUserResponse
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/users"),
		&response,
		&yt.ListNodeOptions{
			Attributes: []string{
				"azure",
				"banned",
				"banned_since",
				"source",
			},
		},
	)
	if err != nil {
		return nil, err
	}

	var users []YtsaurusUser
	for _, ytUser := range response {
		var bannedSince time.Time
		if ytUser.BannedSince != "" {
			bannedSince, err = time.Parse(appTimeFormat, ytUser.BannedSince)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse @banned_since. %v", ytUser)
			}
		}
		var sourceUser SourceUser
		if ytUser.Azure != nil {
			sourceUser = *ytUser.Azure
		} else if ytUser.Source != nil {
			sourceUser, err = NewSourceUser(ytUser.Source)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create source user. %v", ytUser)
			}
		}

		users = append(users, YtsaurusUser{
			Username:    ytUser.Name,
			SourceUser:  sourceUser,
			BannedSince: bannedSince,
		})
	}
	return users, nil
}

func doGetAllYtsaurusGroupsWithMembers(ctx context.Context, client yt.Client) ([]YtsaurusGroupWithMembers, error) {
	type YtsaurusGroupReponse struct {
		Name    string         `yson:",value"`
		Azure   *AzureGroup    `yson:"azure,attr"`
		Source  map[string]any `yson:"source,attr"`
		Members []string       `yson:"members,attr"`
	}

	var response []YtsaurusGroupReponse
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/groups"),
		&response,
		&yt.ListNodeOptions{
			Attributes: []string{"members", "azure", "source"},
		},
	)
	if err != nil {
		return nil, err
	}

	var groups []YtsaurusGroupWithMembers
	for _, ytGroup := range response {
		members := NewStringSet()
		for _, m := range ytGroup.Members {
			members.Add(m)
		}

		var sourceGroup SourceGroup
		if ytGroup.Azure != nil {
			sourceGroup = *ytGroup.Azure
		} else if ytGroup.Source != nil {
			sourceGroup, err = NewSourceGroup(ytGroup.Source)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to create source group. %v", ytGroup)
			}
		}

		groups = append(groups, YtsaurusGroupWithMembers{
			YtsaurusGroup: YtsaurusGroup{
				Name:        ytGroup.Name,
				SourceGroup: sourceGroup,
			},
			Members: members,
		})
	}
	return groups, nil
}

func doCreateYtsaurusUser(ctx context.Context, client yt.Client, username string, attrs map[string]any) error {
	if attrs == nil {
		attrs = make(map[string]any)
	}
	attrs["name"] = username
	_, err := client.CreateObject(
		ctx,
		yt.NodeUser,
		&yt.CreateObjectOptions{Attributes: attrs},
	)
	return err
}

func doCreateYtsaurusGroup(ctx context.Context, client yt.Client, name string, attrs map[string]any) error {
	if attrs == nil {
		attrs = make(map[string]any)
	}
	attrs["name"] = name
	_, err := client.CreateObject(
		ctx,
		yt.NodeGroup,
		&yt.CreateObjectOptions{Attributes: attrs},
	)
	return err
}

func doAddMemberYtsaurusGroup(ctx context.Context, client yt.Client, username, groupname string) error {
	return client.AddMember(
		ctx,
		groupname,
		username,
		nil,
	)
}

func doRemoveMemberYtsaurusGroup(ctx context.Context, client yt.Client, username, groupname string) error {
	return client.RemoveMember(
		ctx,
		groupname,
		username,
		nil,
	)
}

func buildUserAttributes(user YtsaurusUser) map[string]any {
	return map[string]any{
		"name":                        user.Username,
		"banned_since":                user.BannedSinceString(),
		"banned":                      user.IsBanned(),
		user.GetSourceAttributeName(): user.SourceUser,
	}
}

func buildGroupAttributes(group YtsaurusGroup) map[string]any {
	return map[string]any{
		group.GetSourceAttributeName(): group.SourceGroup,
		"name":                         group.Name,
	}
}

// nolint: unused
func doSetAzureAttributeForYtsaurusUser(ctx context.Context, client yt.Client, username string, attrName string, attrValue any) error {
	return client.SetNode(
		ctx,
		ypath.Path("//sys/users/"+username+"/@"+attrName),
		attrValue,
		nil,
	)
}

func doSetAttributesForYtsaurusUser(ctx context.Context, client yt.Client, username string, attrs map[string]any) error {
	attrsCopy := make(map[string]any)
	for key, value := range attrs {
		if key == "name" && value == username {
			// multiset_attributes returns an error:
			// `setting builtin attribute "name" ... user ... already exists`
			// on attempt to set same name for the existing user.
			continue
		}
		attrsCopy[key] = value
	}

	return client.MultisetAttributes(
		ctx,
		ypath.Path("//sys/users/"+username+"/@"),
		attrsCopy,
		nil,
	)
}

// nolint: unused
func doSetAzureAttributeForYtsaurusGroup(
	ctx context.Context,
	client yt.Client,
	groupname string,
	attrName string,
	attrValue map[string]string,
) error {
	return client.SetNode(
		ctx,
		ypath.Path("//sys/groups/"+groupname+"/@"+attrName),
		attrValue,
		nil,
	)
}

func doSetAttributesForYtsaurusGroup(ctx context.Context, client yt.Client, groupname string, attrs map[string]any) error {
	return client.MultisetAttributes(
		ctx,
		ypath.Path("//sys/groups/"+groupname+"/@"),
		attrs,
		nil,
	)
}

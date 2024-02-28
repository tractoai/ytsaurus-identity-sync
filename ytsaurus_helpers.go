package main

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

// Lower level functions for reusing in tests.

const (
	bannedSinceAttributeName = "banned_since"
	bannedAttributeName      = "banned"
	sourceTypeAttributeName  = "source_type"
)

func doGetAllYtsaurusUsers(ctx context.Context, client yt.Client, sourceAttributeName string) ([]YtsaurusUser, error) {
	type YtsaurusUserResponse struct {
		Name  string         `yson:",value"`
		Attrs map[string]any `yson:",attrs"`
	}

	var response []YtsaurusUserResponse
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/users"),
		&response,
		&yt.ListNodeOptions{
			Attributes: []string{
				bannedAttributeName,
				bannedSinceAttributeName,
				sourceTypeAttributeName,
				sourceAttributeName,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	var users []YtsaurusUser
	for _, ytUser := range response {
		var bannedSince time.Time
		if bannedSinceRaw, ok := ytUser.Attrs[bannedSinceAttributeName]; ok && bannedSinceRaw != "" {
			bannedSince, err = time.Parse(appTimeFormat, bannedSinceRaw.(string))
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse @banned_since. %v", ytUser)
			}
		}
		var sourceUser SourceUser
		if sourceRaw, ok := ytUser.Attrs[sourceAttributeName]; ok {
			sourceType := AzureSourceType
			if sourceTypeRaw, ok := ytUser.Attrs[sourceTypeAttributeName]; ok {
				sourceType = SourceType(sourceTypeRaw.(string))
			}
			sourceUser, err = NewSourceUser(sourceType, sourceRaw.(map[string]any))
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
		Name       string         `yson:",value"`
		SourceType *SourceType    `yson:"source_type,attr"`
		Azure      *AzureGroup    `yson:"azure,attr"`
		Source     map[string]any `yson:"source,attr"`
		Members    []string       `yson:"members,attr"`
	}

	var response []YtsaurusGroupReponse
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/groups"),
		&response,
		&yt.ListNodeOptions{
			Attributes: []string{
				"members",
				"azure",
				"source",
				"source_type",
			},
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
		} else if ytGroup.SourceType != nil && ytGroup.Source != nil {
			sourceGroup, err = NewSourceGroup(*ytGroup.SourceType, ytGroup.Source)
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
		"source_type":                 user.SourceUser.GetSourceType(),
	}
}

func buildGroupAttributes(group YtsaurusGroup) map[string]any {
	return map[string]any{
		group.GetSourceAttributeName(): group.SourceGroup,
		"name":                         group.Name,
		"source_type":                  group.SourceGroup.GetSourceType(),
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

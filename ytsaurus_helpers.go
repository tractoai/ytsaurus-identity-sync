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
	membersAttributeName     = "members"
	nameAttributeName        = "name"
)

var (
	listMaxSize = int64(65535)
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
			MaxSize: &listMaxSize,
			Attributes: []string{
				bannedAttributeName,
				bannedSinceAttributeName,
				sourceAttributeName,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	var users []YtsaurusUser
	for _, ytUser := range response {
		user := YtsaurusUser{
			Username: ytUser.Name,
		}

		if ytUser.Attrs != nil {
			if bannedSinceRaw, ok := ytUser.Attrs[bannedSinceAttributeName]; ok && bannedSinceRaw != "" {
				user.BannedSince, err = time.Parse(appTimeFormat, bannedSinceRaw.(string))
				if err != nil {
					return nil, errors.Wrapf(err, "failed to parse @banned_since. %v", ytUser)
				}
			}
			if sourceRaw, ok := ytUser.Attrs[sourceAttributeName]; ok {
				user.SourceRaw = sourceRaw.(map[string]any)
			}
		}

		users = append(users, user)
	}
	return users, nil
}

func doGetAllYtsaurusGroupsWithMembers(ctx context.Context, client yt.Client, sourceAttributeName string) ([]YtsaurusGroupWithMembers, error) {
	type YtsaurusGroupReponse struct {
		Name  string         `yson:",value"`
		Attrs map[string]any `yson:",attrs"`
	}

	var response []YtsaurusGroupReponse
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/groups"),
		&response,
		&yt.ListNodeOptions{
			MaxSize: &listMaxSize,
			Attributes: []string{
				membersAttributeName,
				sourceAttributeName,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	var groups []YtsaurusGroupWithMembers
	for _, ytGroup := range response {
		members := NewStringSet()

		group := YtsaurusGroup{Name: ytGroup.Name}

		if ytGroup.Attrs != nil {
			if membersRaw, ok := ytGroup.Attrs[membersAttributeName]; ok {
				for _, m := range membersRaw.([]interface{}) {
					members.Add(m.(string))
				}
			}

			if sourceRaw, ok := ytGroup.Attrs[sourceAttributeName]; ok {
				group.SourceRaw = sourceRaw.(map[string]any)
			}
		}

		groups = append(groups, YtsaurusGroupWithMembers{
			YtsaurusGroup: group,
			Members:       members,
		})
	}
	return groups, nil
}

func doCreateYtsaurusUser(ctx context.Context, client yt.Client, username string, attrs map[string]any) error {
	if attrs == nil {
		attrs = make(map[string]any)
	}
	attrs[nameAttributeName] = username
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
	attrs[nameAttributeName] = name
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

func buildUserAttributes(user YtsaurusUser, sourceAttributeName string) map[string]any {
	return map[string]any{
		nameAttributeName:        user.Username,
		bannedSinceAttributeName: user.BannedSinceString(),
		bannedAttributeName:      user.IsBanned(),
		sourceAttributeName:      user.SourceRaw,
	}
}

func buildGroupAttributes(group YtsaurusGroup, sourceAttributeName string) map[string]any {
	return map[string]any{
		sourceAttributeName: group.SourceRaw,
		nameAttributeName:   group.Name,
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
		if key == nameAttributeName && value == username {
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
	attrValue map[string]any,
) error {
	return client.SetNode(
		ctx,
		ypath.Path("//sys/groups/"+groupname+"/@"+attrName),
		attrValue,
		nil,
	)
}

func doSetAttributesForYtsaurusGroupUpdate(ctx context.Context, client yt.Client, groupname string, attrs map[string]any) error {
	if groupname == attrs[nameAttributeName] {
		// otherwise we'll got
		// method: "multiset_attributes"
		// error setting builtin attribute "name"
		delete(attrs, nameAttributeName)
	}
	return client.MultisetAttributes(
		ctx,
		ypath.Path("//sys/groups/"+groupname+"/@"),
		attrs,
		nil,
	)
}

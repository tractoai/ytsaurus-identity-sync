package main

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

// Lower level functions for reusing in tests.

func doGetAllYtsaurusUsers(ctx context.Context, client yt.Client) ([]YtsaurusUser, error) {
	var response []yson.ValueWithAttrs
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/users"),
		&response,
		&yt.ListNodeOptions{
			Attributes: []string{
				"azure",
				"banned_since",
			},
		},
	)
	if err != nil {
		return nil, err
	}

	handleNilString := func(val any) string {
		if val == nil {
			return ""
		}
		return val.(string)
	}

	var users []YtsaurusUser
	for _, ytUser := range response {
		name := ytUser.Value.(string)

		var userData map[string]any
		if ytUser.Attrs["azure"] != nil {
			userData = ytUser.Attrs["azure"].(map[string]any)
		} else {
			userData = make(map[string]any)
		}

		var bannedSinceRaw = handleNilString(ytUser.Attrs["banned_since"])
		var bannedSince time.Time
		if bannedSinceRaw != "" {
			bannedSince, err = time.Parse(appTimeFormat, bannedSinceRaw)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse @banned_since. %v", ytUser)
			}
		}
		users = append(users, YtsaurusUser{
			Username:      name,
			AzureID:       handleNilString(userData["id"]),
			PrincipalName: handleNilString(userData["principal_name"]),
			Email:         handleNilString(userData["email"]),
			FirstName:     handleNilString(userData["first_name"]),
			LastName:      handleNilString(userData["last_name"]),
			DisplayName:   handleNilString(userData["display_name"]),
			BannedSince:   bannedSince,
		})
	}
	return users, nil
}

func doGetAllYtsaurusGroupsWithMembers(ctx context.Context, client yt.Client) ([]YtsaurusGroupWithMembers, error) {
	type YtsaurusGroupReponse struct {
		Name    string            `yson:",value"`
		Azure   map[string]string `yson:"azure,attr"`
		Members []string          `yson:"members,attr"`
	}

	var response []YtsaurusGroupReponse
	err := client.ListNode(
		ctx,
		ypath.Path("//sys/groups"),
		&response,
		&yt.ListNodeOptions{
			Attributes: []string{"members", "azure"},
		},
	)
	if err != nil {
		return nil, err
	}

	var users []YtsaurusGroupWithMembers
	for _, ytGroup := range response {
		members := NewStringSet()
		for _, m := range ytGroup.Members {
			members.Add(m)
		}
		users = append(users, YtsaurusGroupWithMembers{
			YtsaurusGroup: YtsaurusGroup{
				Name:        ytGroup.Name,
				AzureID:     ytGroup.Azure["id"],
				DisplayName: ytGroup.Azure["display_name"],
			},
			Members: members,
		})
	}
	return users, nil
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

func buildUserAzureAttributeValue(user YtsaurusUser) map[string]string {
	return map[string]string{
		"id":             user.AzureID,
		"email":          user.Email,
		"principal_name": user.PrincipalName,
		"first_name":     user.FirstName,
		"last_name":      user.LastName,
		"display_name":   user.DisplayName,
	}
}

func buildUserAttributes(user YtsaurusUser) map[string]any {
	return map[string]any{
		"azure":        buildUserAzureAttributeValue(user),
		"name":         user.Username,
		"banned_since": user.BannedSinceString(),
		"banned":       user.IsBanned(),
	}
}

func buildGroupAzureAttributeValue(group YtsaurusGroup) map[string]string {
	return map[string]string{
		"id":           group.AzureID,
		"display_name": group.DisplayName,
	}
}

func buildGroupAttributes(group YtsaurusGroup) map[string]any {
	return map[string]any{
		"azure": map[string]string{
			"id":           group.AzureID,
			"display_name": group.DisplayName,
		},
		"name": group.Name,
	}
}

// nolint: unused
func doSetAzureAttributeForYtsaurusUser(ctx context.Context, client yt.Client, username string, attrValue map[string]string) error {
	return client.SetNode(
		ctx,
		ypath.Path("//sys/users/"+username+"/@azure"),
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
func doSetAzureAttributeForYtsaurusGroup(ctx context.Context, client yt.Client, groupname string, attrValue map[string]string) error {
	return client.SetNode(
		ctx,
		ypath.Path("//sys/groups/"+groupname+"/@azure"),
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

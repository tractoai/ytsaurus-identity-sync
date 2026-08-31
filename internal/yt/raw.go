package yt

import (
	"context"

	ytclient "go.ytsaurus.tech/yt/go/yt"
)

// Client returns the underlying YTsaurus client for low-level maintenance operations.
func (y *Ytsaurus) Client() ytclient.Client {
	return y.client
}

func GetAllUsers(ctx context.Context, client ytclient.Client, sourceAttributeName string) ([]YtsaurusUser, error) {
	return doGetAllYtsaurusUsers(ctx, client, sourceAttributeName)
}

func GetAllGroupsWithMembers(ctx context.Context, client ytclient.Client, sourceAttributeName string) ([]YtsaurusGroupWithMembers, error) {
	return doGetAllYtsaurusGroupsWithMembers(ctx, client, sourceAttributeName)
}

func CreateUser(ctx context.Context, client ytclient.Client, username string, attrs map[string]any) error {
	return doCreateYtsaurusUser(ctx, client, username, attrs)
}

func CreateGroup(ctx context.Context, client ytclient.Client, name string, attrs map[string]any) error {
	return doCreateYtsaurusGroup(ctx, client, name, attrs)
}

func AddMember(ctx context.Context, client ytclient.Client, username, groupname string) error {
	return doAddMemberYtsaurusGroup(ctx, client, username, groupname)
}

func BuildUserAttributes(user YtsaurusUser, sourceAttributeName string) map[string]any {
	return buildUserAttributes(user, sourceAttributeName)
}

func BuildGroupAttributes(group YtsaurusGroup, sourceAttributeName string) map[string]any {
	return buildGroupAttributes(group, sourceAttributeName)
}

func SetUserSourceAttribute(ctx context.Context, client ytclient.Client, username, attrName string, attrValue any) error {
	return doSetAzureAttributeForYtsaurusUser(ctx, client, username, attrName, attrValue)
}

func SetGroupSourceAttribute(ctx context.Context, client ytclient.Client, groupname, attrName string, attrValue map[string]any) error {
	return doSetAzureAttributeForYtsaurusGroup(ctx, client, groupname, attrName, attrValue)
}

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestLocalYtsaurus(t *testing.T) {
	ytLocal := NewYtsaurusLocal()
	defer func() { require.NoError(t, ytLocal.Stop()) }()
	require.NoError(t, ytLocal.Start())

	ytClient, err := ytLocal.GetClient()
	require.NoError(t, err)

	newUserName := "oleg"
	usernamesBefore := getUsers(t, ytClient)
	require.NotContains(t, usernamesBefore, newUserName)
	createUser(t, ytClient, newUserName)
	usernamesAfter := getUsers(t, ytClient)
	require.Contains(t, usernamesAfter, newUserName)
}

func getUsers(t *testing.T, client yt.Client) []string {
	var usernames []string
	err := client.ListNode(
		context.Background(),
		ypath.Path("//sys/users"),
		&usernames,
		nil,
	)
	require.NoError(t, err)
	return usernames
}

func createUser(t *testing.T, client yt.Client, name string) {
	_, err := client.CreateObject(
		context.Background(),
		yt.NodeUser,
		&yt.CreateObjectOptions{
			Attributes: map[string]any{
				"name": name,
			},
		},
	)
	require.NoError(t, err)
}

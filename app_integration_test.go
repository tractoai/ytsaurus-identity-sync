//go:build integration

package main

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	ytcontainer "github.com/tractoai/testcontainers-ytsaurus"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
)

const (
	// If runLocalYtsaurus false — it is up to you to run local_yt manually.
	// For example: yt/docker/local/run_local_cluster.sh --proxy-port 10110
	runLocalYtsaurus = false
)

// TestAppIntegration checks sync with real Azure API and local yt
// It requires AZURE_CLIENT_SECRET to be set.
func TestAppIntegration(t *testing.T) {
	require.NoError(t, os.Setenv(defaultYtsaurusSecretEnvVar, ytDevToken))
	cfg, err := loadConfig("config.local.yaml")
	require.NoError(t, err)

	ctx := context.Background()
	var ytClient yt.Client
	if runLocalYtsaurus {
		var ytLocal *ytcontainer.YTsaurusContainer
		ytLocal, err = ytcontainer.RunContainer(ctx)
		require.NoError(t, err)
		defer func() { require.NoError(t, ytLocal.Terminate(ctx)) }()
		ytClient, err = ytLocal.NewClient(ctx)
		require.NoError(t, err)
	} else {
		ytClient, err = ythttp.NewClient(&yt.Config{
			Proxy: cfg.Ytsaurus.Proxy,
		})
		require.NoError(t, err)
	}

	logger, err := configureLogger(&cfg.Logging)
	require.NoError(t, err)
	app, err := NewApp(cfg, logger)
	require.NoError(t, err)

	usersBefore, err := doGetAllYtsaurusUsers(context.Background(), ytClient, cfg.Ytsaurus.SourceAttributeName)
	require.NoError(t, err)
	t.Log("usersBefore", len(usersBefore), usersBefore)
	groupsBefore, err := doGetAllYtsaurusGroupsWithMembers(context.Background(), ytClient, cfg.Ytsaurus.SourceAttributeName)
	t.Log("groupsBefore", len(groupsBefore), groupsBefore)
	require.NoError(t, err)

	app.syncOnce()

	usersAfter, err := doGetAllYtsaurusUsers(context.Background(), ytClient, cfg.Ytsaurus.SourceAttributeName)
	require.NoError(t, err)
	t.Log("usersAfter", len(usersAfter), usersAfter)
	groupsAfter, err := doGetAllYtsaurusGroupsWithMembers(context.Background(), ytClient, cfg.Ytsaurus.SourceAttributeName)
	t.Log("groupsAfter", len(groupsAfter), groupsAfter)
	require.NoError(t, err)

	require.LessOrEqual(t, len(usersBefore), len(usersAfter))
	require.LessOrEqual(t, len(groupsBefore), len(groupsAfter))
}

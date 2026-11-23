package app

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/sources/azure"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"
	"go.ytsaurus.tech/yt/go/ypath"
	"k8s.io/utils/clock"
)

// Azure decoding is real; only fetching users is replaced to avoid external API calls.
type rebindingAzureSource struct {
	*azure.AzureReal
	users []source.SourceUser
}

func (s *rebindingAzureSource) GetUsers() ([]source.SourceUser, error) {
	return s.users, nil
}

func (suite *AppTestSuite) TestAzureUserIDRebindingPreservesObject() {
	t := suite.T()
	defer suite.clear()
	oldUser := azure.AzureUser{AzureID: "old-azure-id", PrincipalName: "alice@acme.com"}
	newUser := oldUser
	newUser.AzureID = "new-azure-id"
	oldRaw, err := oldUser.GetRaw()
	require.NoError(t, err)
	since := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
	require.NoError(t, internalyt.CreateUser(suite.ctx, suite.ytsaurusClient, "alice", map[string]any{
		"azure": oldRaw, "banned": true, "banned_since": since, "rebinding_test_marker": "preserved",
	}))
	require.NoError(t, internalyt.CreateGroup(suite.ctx, suite.ytsaurusClient, "manual-rebinding-group", nil))
	require.NoError(t, internalyt.AddMember(suite.ctx, suite.ytsaurusClient, "alice", "manual-rebinding-group"))
	var objectID string
	require.NoError(t, suite.ytsaurusClient.GetNode(suite.ctx, ypath.Path("//sys/users/alice/@id"), &objectID, nil))
	proxy, err := suite.ytsaurusLocal.GetProxy(suite.ctx)
	require.NoError(t, err)

	for _, applyChanges := range []bool{false, true} {
		name := "dry run"
		if applyChanges {
			name = "apply"
		}
		t.Run(name, func(t *testing.T) {
			logger := zap.NewNop().Sugar()
			cfg := &config.Config{
				App: config.AppConfig{
					UsernameReplacements: defaultUsernameReplacements, BanBeforeRemoveDuration: time.Hour, RemoveLimit: 1,
				},
				Azure: &config.AzureConfig{AllowUserIDRebinding: true},
				Ytsaurus: config.YtsaurusConfig{
					Proxy: proxy, SourceAttributeName: "azure", ApplyUserChanges: applyChanges, Timeout: 10 * time.Second,
				},
			}
			ytsaurus, err := internalyt.NewYtsaurus(&cfg.Ytsaurus, logger, clock.RealClock{})
			require.NoError(t, err)
			src := &rebindingAzureSource{AzureReal: &azure.AzureReal{}, users: []source.SourceUser{newUser}}
			application := NewApp(cfg, logger, src, ytsaurus, nil)
			t.Cleanup(application.Stop)
			_, err = application.syncUsers()
			require.NoError(t, err)

			var attrs struct {
				ID          string          `yson:"id"`
				Azure       azure.AzureUser `yson:"azure"`
				Banned      bool            `yson:"banned"`
				BannedSince string          `yson:"banned_since"`
				Marker      string          `yson:"rebinding_test_marker"`
				MemberOf    []string        `yson:"member_of"`
			}
			require.NoError(t, suite.ytsaurusClient.GetNode(suite.ctx, ypath.Path("//sys/users/alice/@"), &attrs, nil))
			require.Equal(t, objectID, attrs.ID)
			require.Equal(t, "preserved", attrs.Marker)
			require.Contains(t, attrs.MemberOf, "manual-rebinding-group")
			if applyChanges {
				require.Equal(t, newUser, attrs.Azure)
				require.False(t, attrs.Banned)
				require.Empty(t, attrs.BannedSince)
			} else {
				require.Equal(t, oldUser, attrs.Azure)
				require.True(t, attrs.Banned)
				require.Equal(t, since, attrs.BannedSince)
			}
		})
	}
}

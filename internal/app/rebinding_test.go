package app

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/internal/config"
	"github.com/tractoai/ytsaurus-identity-sync/internal/sources/azure"
	"github.com/tractoai/ytsaurus-identity-sync/internal/sources/ldap"
	internalyt "github.com/tractoai/ytsaurus-identity-sync/internal/yt"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.uber.org/zap"
)

// Provider decoding is real; only fetching users is replaced to avoid external API calls.
type rebindingSource struct {
	source.Source
	users []source.SourceUser
}

func (s *rebindingSource) GetUsers() ([]source.SourceUser, error) {
	return s.users, nil
}

func newRebindingTestApp(t *testing.T, allowRebinding bool) *App {
	t.Helper()
	application := NewApp(&config.Config{
		App: config.AppConfig{
			UsernameReplacements:    defaultUsernameReplacements,
			GroupnameReplacements:   defaultGroupnameReplacements,
			BanBeforeRemoveDuration: 24 * time.Hour,
			RemoveLimit:             1,
			AllowUserIDRebinding:    allowRebinding,
		},
	}, zap.NewNop().Sugar(), &fakeSource{}, &metricsYtsaurus{}, newRecordingMetrics())
	t.Cleanup(application.Stop)
	return application
}

func TestUserIDRebinding(t *testing.T) {
	newUser := aliceSource
	newUser.ID = "new-alice-id"
	newUser.Name = "ALICE@acme.com"
	newYTUser := ytsaurusUserFromSource(newUser)
	for _, tc := range []struct {
		name           string
		allowRebinding bool
		oldUser        internalyt.YtsaurusUser
		wantRebind     bool
	}{
		{name: "disabled by default", oldUser: aliceYT},
		{name: "active user", allowRebinding: true, oldUser: aliceYT, wantRebind: true},
		{
			name: "banned user", allowRebinding: true,
			oldUser: bannedUser(aliceYT, time.Now()), wantRebind: true,
		},
		{
			name: "expired ban", allowRebinding: true,
			oldUser: bannedUser(aliceYT, time.Now().Add(-48*time.Hour)), wantRebind: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			application := newRebindingTestApp(t, tc.allowRebinding)
			diff, err := application.diffUsers(sourceUsers(newUser), []internalyt.YtsaurusUser{tc.oldUser})
			require.NoError(t, err)
			require.Equal(t, map[source.ObjectID]internalyt.YtsaurusUser{newUser.ID: newYTUser}, diff.result)
			if !tc.wantRebind {
				require.Equal(t, []internalyt.YtsaurusUser{newYTUser}, diff.create)
				require.Equal(t, []internalyt.YtsaurusUser{tc.oldUser}, diff.remove)
				require.Empty(t, diff.update)
				return
			}
			require.Empty(t, diff.create)
			require.Empty(t, diff.remove)
			require.Equal(t, []UpdatedYtsaurusUser{{OldUsername: aliceYT.Username, YtsaurusUser: newYTUser}}, diff.update)
			// Matching only changes the diff, not the fetched object's source attributes.
			require.Equal(t, aliceSource.ID, tc.oldUser.SourceRaw["id"])

			groupDiff, err := application.diffGroups([]source.SourceGroupWithMembers{
				sourceGroup(devsSourceGroup, newUser.ID),
				sourceGroup(qaSourceGroup, aliceSource.ID),
			}, []internalyt.YtsaurusGroupWithMembers{
				ytsaurusGroup(devsYTGroup),
				ytsaurusGroup(qaYTGroup, aliceYT.Username),
			}, diff.result)
			require.NoError(t, err)
			require.Equal(t, []internalyt.YtsaurusMembership{{Username: aliceYT.Username, GroupName: devsYTGroup.Name}}, groupDiff.membersToAdd)
			require.Equal(t, []internalyt.YtsaurusMembership{{Username: aliceYT.Username, GroupName: qaYTGroup.Name}}, groupDiff.membersToRemove)
		})
	}
}

func TestUserIDRebindingPrefersExistingID(t *testing.T) {
	application := newRebindingTestApp(t, true)
	renamedAlice := aliceSource
	renamedAlice.Name = bobSource.Name
	renamedAlice.FirstName = "Alicia"
	renamedAlice.LastName = "Jones"
	renamedAlice.Email = "alicia@example.com"
	diff, err := application.diffUsers(sourceUsers(renamedAlice), []internalyt.YtsaurusUser{aliceYT, bobYT})
	require.NoError(t, err)
	require.Empty(t, diff.create)
	require.Equal(t, []internalyt.YtsaurusUser{bobYT}, diff.remove)
	require.Equal(t, []UpdatedYtsaurusUser{{OldUsername: aliceYT.Username, YtsaurusUser: ytsaurusUserFromSource(renamedAlice)}}, diff.update)
}

func TestUserIDRebindingChecksProfile(t *testing.T) {
	for _, provider := range []struct {
		name     string
		source   source.Source
		oldUser  source.SourceUser
		newAttrs map[string]any
		fields   []string
	}{
		{
			name: "azure", source: &azure.AzureReal{},
			oldUser: azure.AzureUser{
				AzureID: "old-azure-id", PrincipalName: "alice@acme.com",
				FirstName: "Alice", LastName: "Smith", Email: "alice@acme.com", DisplayName: "Alice Smith",
			},
			newAttrs: map[string]any{"id": "new-azure-id", "principal_name": "ALICE@acme.com", "display_name": "Smith, Alice"},
			fields:   []string{"first_name", "last_name", "email"},
		},
		{
			name: "ldap", source: &ldap.Ldap{},
			oldUser:  ldap.LdapUser{UID: "old-ldap-id", Username: "alice@acme.com", FirstName: "Alice"},
			newAttrs: map[string]any{"uid": "new-ldap-id", "username": "ALICE@acme.com"},
			fields:   []string{"first_name"},
		},
	} {
		for _, field := range provider.fields {
			for _, tc := range []struct {
				name       string
				change     func(oldRaw, newRaw map[string]any)
				wantRebind bool
			}{
				{name: "matching", wantRebind: true},
				{
					name: "different",
					change: func(_, newRaw map[string]any) {
						newRaw[field] = "different"
					},
				},
				{
					name: "cleared",
					change: func(_, newRaw map[string]any) {
						newRaw[field] = ""
					},
				},
				{
					name: "added",
					change: func(oldRaw, _ map[string]any) {
						oldRaw[field] = ""
					},
				},
				{
					name: "both empty", wantRebind: true,
					change: func(oldRaw, newRaw map[string]any) {
						oldRaw[field], newRaw[field] = "", ""
					},
				},
				{
					name: "missing stored attribute",
					change: func(oldRaw, newRaw map[string]any) {
						delete(oldRaw, field)
						newRaw[field] = ""
					},
				},
			} {
				t.Run(provider.name+"/"+field+"/"+tc.name, func(t *testing.T) {
					oldRaw, err := provider.oldUser.GetRaw()
					require.NoError(t, err)
					newRaw, err := provider.oldUser.GetRaw()
					require.NoError(t, err)
					for key, value := range provider.newAttrs {
						newRaw[key] = value
					}
					if tc.change != nil {
						tc.change(oldRaw, newRaw)
					}
					newUser, err := provider.source.CreateUserFromRaw(newRaw)
					require.NoError(t, err)
					storedUser := bannedUser(internalyt.YtsaurusUser{Username: "alice", SourceRaw: oldRaw}, time.Now())
					ytsaurus := &rebindingYtsaurus{metricsYtsaurus: metricsYtsaurus{users: []internalyt.YtsaurusUser{storedUser}}}
					application := newRebindingTestApp(t, true)
					application.source = &rebindingSource{Source: provider.source, users: []source.SourceUser{newUser}}
					application.ytsaurus = ytsaurus
					result, err := application.syncUsers()
					if !tc.wantRebind {
						require.ErrorContains(t, err, field+" is missing or does not match")
						require.Nil(t, result)
						require.Empty(t, ytsaurus.updates)
						require.Equal(t, []internalyt.YtsaurusUser{storedUser}, ytsaurus.users)
						require.Empty(t, application.metrics.(*recordingMetrics).ytMutationSuccess)
						require.Empty(t, application.metrics.(*recordingMetrics).ytMutationErrors)
						return
					}
					require.NoError(t, err)
					wantUser := internalyt.YtsaurusUser{Username: "alice", SourceRaw: newRaw}
					require.Equal(t, []UpdatedYtsaurusUser{{OldUsername: "alice", YtsaurusUser: wantUser}}, ytsaurus.updates)
					require.Equal(t, map[source.ObjectID]internalyt.YtsaurusUser{newUser.GetID(): wantUser}, result)
				})
			}
		}
	}
}

func TestUserIDRebindingCreatesUnmatchedUser(t *testing.T) {
	application := newRebindingTestApp(t, true)
	diff, err := application.diffUsers(sourceUsers(bobSource), []internalyt.YtsaurusUser{aliceYT})
	require.NoError(t, err)
	require.Equal(t, []internalyt.YtsaurusUser{bobYT}, diff.create)
	require.Equal(t, []internalyt.YtsaurusUser{aliceYT}, diff.remove)
	require.Empty(t, diff.update)
}

func TestUserIDRebindingRejectsAmbiguityBeforeMutations(t *testing.T) {
	newAlice := fakeUser{ID: "new-alice-id", Name: "ALICE@acme.com"}
	renamedAlice := aliceSource
	renamedAlice.Name = "alice-renamed@acme.com"
	for _, tc := range []struct {
		name    string
		users   []source.SourceUser
		ytUsers []internalyt.YtsaurusUser
		wantErr string
	}{
		{
			name: "previous ID still in source", users: sourceUsers(newAlice, renamedAlice),
			ytUsers: []internalyt.YtsaurusUser{aliceYT}, wantErr: "previous ID is still present in source",
		},
		{
			name: "duplicate normalized username", users: sourceUsers(aliceSource, newAlice),
			ytUsers: []internalyt.YtsaurusUser{aliceYT}, wantErr: `map to username "alice"`,
		},
		{
			name: "two new IDs for one username", users: sourceUsers(newAlice, fakeUser{ID: "another-id", Name: "alice@acme.com"}),
			ytUsers: []internalyt.YtsaurusUser{aliceYT}, wantErr: `map to username "alice"`,
		},
		{
			name: "duplicate source ID", users: sourceUsers(aliceSource, renamedAlice),
			ytUsers: []internalyt.YtsaurusUser{aliceYT}, wantErr: "duplicate source user ID",
		},
		{
			name: "duplicate stored source ID", users: sourceUsers(newAlice),
			ytUsers: []internalyt.YtsaurusUser{aliceYT, ytsaurusUserFromSource(renamedAlice)}, wantErr: "duplicate YTsaurus source user ID",
		},
		{
			name: "duplicate stored username", users: sourceUsers(newAlice),
			ytUsers: []internalyt.YtsaurusUser{aliceYT, ytsaurusUserFromSource(newAlice)}, wantErr: `have username "alice"`,
		},
		{
			name: "empty source ID", users: sourceUsers(fakeUser{Name: aliceSource.Name}),
			ytUsers: []internalyt.YtsaurusUser{aliceYT}, wantErr: "empty or duplicate source user ID",
		},
		{
			name: "empty stored source ID", users: sourceUsers(newAlice),
			ytUsers: []internalyt.YtsaurusUser{ytsaurusUserFromSource(fakeUser{Name: aliceSource.Name})}, wantErr: "empty or duplicate YTsaurus source user ID",
		},
		{
			name: "manually managed user", users: sourceUsers(newAlice),
			ytUsers: []internalyt.YtsaurusUser{{Username: aliceYT.Username}}, wantErr: "user is manually managed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			application := newRebindingTestApp(t, true)
			application.source = &fakeSource{users: tc.users}
			application.ytsaurus = &metricsYtsaurus{users: tc.ytUsers}
			_, err := application.syncUsers()
			require.ErrorContains(t, err, tc.wantErr)
			require.Empty(t, application.metrics.(*recordingMetrics).ytMutationSuccess)
			require.Empty(t, application.metrics.(*recordingMetrics).ytMutationErrors)
		})
	}
}

type rebindingYtsaurus struct {
	metricsYtsaurus
	updates []UpdatedYtsaurusUser
}

func (y *rebindingYtsaurus) UpdateUser(username string, user internalyt.YtsaurusUser) error {
	y.updates = append(y.updates, UpdatedYtsaurusUser{OldUsername: username, YtsaurusUser: user})
	if y.updateUserErr != nil {
		return y.updateUserErr
	}
	for i := range y.users {
		if y.users[i].Username == username {
			y.users[i] = user
			return nil
		}
	}
	return errors.New("user not found")
}

func TestSyncUserIDRebinding(t *testing.T) {
	for _, failFirstUpdate := range []bool{false, true} {
		name := "successful update"
		if failFirstUpdate {
			name = "retry failed update"
		}
		t.Run(name, func(t *testing.T) {
			application := newRebindingTestApp(t, true)
			newAlice := aliceSource
			newAlice.ID = "new-alice-id"
			oldUser := bannedUser(aliceYT, time.Now().Add(-48*time.Hour))
			ytsaurus := &rebindingYtsaurus{metricsYtsaurus: metricsYtsaurus{users: []internalyt.YtsaurusUser{oldUser}}}
			application.source = &fakeSource{users: sourceUsers(newAlice)}
			application.ytsaurus = ytsaurus
			if failFirstUpdate {
				ytsaurus.updateUserErr = errors.New("update failed")
				_, err := application.syncUsers()
				require.ErrorIs(t, err, ErrSyncPartial)
				require.Equal(t, []internalyt.YtsaurusUser{oldUser}, ytsaurus.users)
				require.Equal(t, 1, application.metrics.(*recordingMetrics).ytMutationErrors[objectTypeUser+"/"+mutationUpdate])
				ytsaurus.updateUserErr = nil
			}
			result, err := application.syncUsers()
			require.NoError(t, err)
			newUser := ytsaurusUserFromSource(newAlice)
			require.Equal(t, []internalyt.YtsaurusUser{newUser}, ytsaurus.users)
			require.Equal(t, map[source.ObjectID]internalyt.YtsaurusUser{newAlice.ID: newUser}, result)
			require.Equal(t, map[string]int{objectTypeUser + "/" + mutationUpdate: 1}, application.metrics.(*recordingMetrics).ytMutationSuccess)
			updateCount := len(ytsaurus.updates)
			_, err = application.syncUsers()
			require.NoError(t, err)
			require.Len(t, ytsaurus.updates, updateCount, "the next sync must not update the user again")
		})
	}
}

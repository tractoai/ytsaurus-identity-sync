package azure

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
)

var (
	_ source.SourceUser  = AzureUser{}
	_ source.SourceGroup = AzureGroup{}
	_ source.Source      = (*AzureReal)(nil)
)

// TestAzureUser ensures that raw representation has expected value.
func TestAzureUser(t *testing.T) {
	rawUser, err := AzureUser{
		PrincipalName: "alice@acme.com",
		AzureID:       "fake-az-id-alice",
		Email:         "alice@acme.com",
		FirstName:     "Alice",
		LastName:      "Henderson",
		DisplayName:   "Henderson, Alice (ACME)",
	}.GetRaw()
	require.NoError(t, err)

	require.Equal(
		t,
		map[string]any{
			"principal_name": "alice@acme.com",
			"id":             "fake-az-id-alice",
			"email":          "alice@acme.com",
			"first_name":     "Alice",
			"last_name":      "Henderson",
			"display_name":   "Henderson, Alice (ACME)",
		},
		rawUser,
	)
}

// TestAzureGroup ensures that raw representation has expected value.
func TestAzureGroup(t *testing.T) {
	rawGroup, err := AzureGroup{
		AzureID:     "fake-az-acme.devs",
		DisplayName: "acme.devs|all",
	}.GetRaw()
	require.NoError(t, err)

	require.Equal(
		t,
		map[string]any{
			"id":           "fake-az-acme.devs",
			"display_name": "acme.devs|all",
		},
		rawGroup,
	)
}

func TestCreateFromRaw(t *testing.T) {
	azureSource := &AzureReal{}

	user, err := azureSource.CreateUserFromRaw(map[string]any{
		"principal_name": "alice@acme.com",
		"id":             "alice-id",
		"email":          "alice@acme.com",
		"first_name":     "Alice",
		"last_name":      "Smith",
		"display_name":   "Alice Smith",
	})
	require.NoError(t, err)
	require.Equal(t, AzureUser{
		PrincipalName: "alice@acme.com",
		AzureID:       "alice-id",
		Email:         "alice@acme.com",
		FirstName:     "Alice",
		LastName:      "Smith",
		DisplayName:   "Alice Smith",
	}, *user.(*AzureUser))

	group, err := azureSource.CreateGroupFromRaw(map[string]any{
		"id":           "devs-id",
		"display_name": "acme.devs|all",
	})
	require.NoError(t, err)
	require.Equal(t, AzureGroup{
		AzureID:     "devs-id",
		DisplayName: "acme.devs|all",
	}, *group.(*AzureGroup))
}

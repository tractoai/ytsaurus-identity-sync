package main

import (
	"testing"

	"github.com/stretchr/testify/require"
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

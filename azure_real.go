package main

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	abstractions "github.com/microsoft/kiota-abstractions-go"
	msgraphsdk "github.com/microsoftgraph/msgraph-sdk-go"
	msgraphcore "github.com/microsoftgraph/msgraph-sdk-go-core"
	msgraphgroups "github.com/microsoftgraph/msgraph-sdk-go/groups"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	msgraphusers "github.com/microsoftgraph/msgraph-sdk-go/users"
	"github.com/pkg/errors"
)

const (
	scope                    = "https://graph.microsoft.com/.default"
	msgraphExpandLimit       = 20
	defaultAzureTimeout      = 3 * time.Second
	defaultAzureSecretEnvVar = "AZURE_CLIENT_SECRET"
)

var (
	defaultUserFieldsToSelect = []string{
		"userPrincipalName",
		"id",
		"mail",
		"givenName",
		"surname",
		"displayName",
		"accountEnabled",
	}
	defaultGroupFieldsToSelect = []string{
		"id",
		"displayName",
	}
)

type AzureReal struct {
	graphClient *msgraphsdk.GraphServiceClient

	usersFilter                       string
	groupsFilter                      string
	groupsDisplayNameSuffixPostFilter string

	logger  appLoggerType
	timeout time.Duration

	debugAzureIDs []string
}

func NewAzureReal(cfg *AzureConfig, logger appLoggerType) (*AzureReal, error) {
	// https://github.com/microsoftgraph/msgraph-sdk-go#22-create-an-authenticationprovider-object
	// https://learn.microsoft.com/en-us/graph/sdks/choose-authentication-providers
	if cfg.ClientSecretEnvVar == "" {
		cfg.ClientSecretEnvVar = defaultAzureSecretEnvVar
	}
	secret := os.Getenv(cfg.ClientSecretEnvVar)
	if secret == "" {
		return nil, errors.Errorf("Azure secret in %s env var shouldn't be empty", cfg.ClientSecretEnvVar)
	}
	cred, err := azidentity.NewClientSecretCredential(
		cfg.Tenant,
		cfg.ClientID,
		secret,
		nil,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure secret credentials")
	}

	graphClient, err := msgraphsdk.NewGraphServiceClientWithCredentials(cred, []string{scope})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ms graph client form secret credentials")
	}

	if cfg.Timeout == 0 {
		cfg.Timeout = defaultAzureTimeout
	}
	return &AzureReal{
		usersFilter:                       cfg.UsersFilter,
		groupsFilter:                      cfg.GroupsFilter,
		groupsDisplayNameSuffixPostFilter: cfg.GroupsDisplayNameSuffixPostFilter,

		graphClient:   graphClient,
		logger:        logger,
		timeout:       cfg.Timeout,
		debugAzureIDs: cfg.DebugAzureIDs,
	}, nil
}

func handleNil[T any](s *T) T {
	if s != nil {
		return *s
	}
	var result T
	return result
}

func (a *AzureReal) CreateUserFromRaw(raw map[string]any) (SourceUser, error) {
	return NewAzureUser(raw)
}

func (a *AzureReal) CreateGroupFromRaw(raw map[string]any) (SourceGroup, error) {
	return NewAzureGroup(raw)
}

func (a *AzureReal) GetUsers() ([]SourceUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()

	usersRaw, err := a.getUsersRaw(ctx, defaultUserFieldsToSelect, a.usersFilter)
	if err != nil {
		return nil, err
	}

	usersSkipped := 0
	var users []SourceUser
	for _, user := range usersRaw {
		principalName := handleNil(user.GetUserPrincipalName())
		id := handleNil(user.GetId())
		mail := handleNil(user.GetMail())
		firstName := handleNil(user.GetGivenName())
		lastName := handleNil(user.GetSurname())
		displayName := handleNil(user.GetDisplayName())

		a.maybePrintDebugLogs(
			id,
			"principalName", principalName,
			"mail", mail,
			"firstName", firstName,
			"lastName", lastName,
			"displayName", displayName,
		)

		if principalName == "" {
			a.logger.Debugw("Skipping user with empty principal name", "user", user)
			usersSkipped++
		} else {
			users = append(users,
				AzureUser{
					PrincipalName: principalName,
					AzureID:       id,
					Email:         mail,
					FirstName:     firstName,
					LastName:      lastName,
					DisplayName:   displayName,
				})
		}
	}

	a.logger.Infow("Fetched users from Azure AD", "got", len(usersRaw), "skipped", usersSkipped)
	return users, nil
}

func (a *AzureReal) GetGroupsWithMembers() ([]SourceGroupWithMembers, error) {
	ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
	defer cancel()

	groupsRaw, err := a.getGroupsWithMembersRaw(ctx, defaultGroupFieldsToSelect, a.groupsFilter)
	if err != nil {
		return nil, err
	}

	groupsSkipped := 0
	var groups []SourceGroupWithMembers
	for _, group := range groupsRaw {
		displayName := handleNil(group.GetDisplayName())
		id := handleNil(group.GetId())

		a.maybePrintDebugLogs(id, "displayName", displayName)

		if displayName == "" {
			a.logger.Debugw("Skipping group with empty display name", "group", group)
			groupsSkipped++
			continue
		}

		if a.groupsDisplayNameSuffixPostFilter != "" && !strings.HasSuffix(displayName, a.groupsDisplayNameSuffixPostFilter) {
			continue
		}

		memberIDs := NewStringSet()
		members := group.GetMembers()
		if len(members) == msgraphExpandLimit {
			// By default, $expand returns only 20 members, for those groups we collect all users by group id.
			members, err = a.getGroupMembers(ctx, id)
			if err != nil {
				return nil, errors.Wrap(err, "failed to fetch all members")
			}
		}

		for _, azureMember := range members {
			azureUserID := azureMember.GetId()
			if azureUserID == nil {
				a.logger.Error("Empty group member id", "group", displayName)
				continue
			}
			memberIDs.Add(*azureUserID)
		}
		a.maybePrintDebugLogs(id, "azure_members_count", len(memberIDs.ToSlice()))

		groups = append(groups,
			SourceGroupWithMembers{
				SourceGroup: AzureGroup{
					AzureID:     id,
					DisplayName: displayName,
				},
				Members: memberIDs,
			})
	}

	a.logger.Infow("Fetched groups from Azure AD", "got", len(groupsRaw), "skipped", groupsSkipped)
	return groups, nil
}

func (a *AzureReal) maybePrintDebugLogs(id ObjectID, args ...any) {
	args = append([]any{"id", id}, args...)
	for _, debugID := range a.debugAzureIDs {
		if id == debugID {
			a.logger.Debugw("Debug info", args...)
		}
	}
}

func (a *AzureReal) getUsersRaw(ctx context.Context, fieldsToSelect []string, filter string) ([]models.Userable, error) {
	// https://learn.microsoft.com/en-us/graph/api/user-list
	// https://learn.microsoft.com/en-us/graph/aad-advanced-queries
	headers := abstractions.NewRequestHeaders()
	headers.Add("ConsistencyLevel", "eventual")
	count := true
	requestConfig := &msgraphusers.UsersRequestBuilderGetRequestConfiguration{
		Headers: headers,
		QueryParameters: &msgraphusers.UsersRequestBuilderGetQueryParameters{
			Count:  &count,
			Filter: &filter,
			Select: fieldsToSelect,
		},
	}
	result, err := a.graphClient.Users().Get(ctx, requestConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get users")
	}

	// https://github.com/microsoftgraph/msgraph-sdk-go#41-get-all-the-users-in-an-environment
	var pageIterator *msgraphcore.PageIterator[models.Userable]
	pageIterator, err = msgraphcore.NewPageIterator[models.Userable](
		result,
		a.graphClient.GetAdapter(),
		models.CreateUserCollectionResponseFromDiscriminatorValue,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create users page iterator")
	}

	var rawUsers []models.Userable
	err = pageIterator.Iterate(context.Background(), func(user models.Userable) bool {
		rawUsers = append(rawUsers, user)
		// Return true to continue the iteration.
		return true
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to iterate over Azure users")
	}
	return rawUsers, nil
}

func (a *AzureReal) getGroupsWithMembersRaw(ctx context.Context, fieldsToSelect []string, filter string) ([]models.Groupable, error) {
	// https://learn.microsoft.com/en-us/graph/api/group-list
	headers := abstractions.NewRequestHeaders()
	headers.Add("ConsistencyLevel", "eventual")
	count := true
	requestConfig := &msgraphgroups.GroupsRequestBuilderGetRequestConfiguration{
		Headers: headers,
		QueryParameters: &msgraphgroups.GroupsRequestBuilderGetQueryParameters{
			Count:  &count,
			Filter: &filter,
			Select: fieldsToSelect,
			Expand: []string{"members($select=id)"},
		},
	}
	result, err := a.graphClient.Groups().Get(ctx, requestConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get groups")
	}

	// https://github.com/microsoftgraph/msgraph-sdk-go#41-get-all-the-users-in-an-environment
	pageIterator, err := msgraphcore.NewPageIterator[models.Groupable](
		result,
		a.graphClient.GetAdapter(),
		models.CreateGroupCollectionResponseFromDiscriminatorValue,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create groups page iterator")
	}

	var rawGroups []models.Groupable
	err = pageIterator.Iterate(context.Background(), func(group models.Groupable) bool {
		rawGroups = append(rawGroups, group)
		// Return true to continue the iteration.
		return true
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to iterate over Azure groups")
	}
	return rawGroups, nil
}

func (a *AzureReal) getGroupMembers(ctx context.Context, groupID string) ([]models.DirectoryObjectable, error) {
	headers := abstractions.NewRequestHeaders()
	headers.Add("ConsistencyLevel", "eventual")

	requestParameters := &msgraphgroups.ItemMembersRequestBuilderGetQueryParameters{
		Select: []string{"id"},
	}
	configuration := &msgraphgroups.ItemMembersRequestBuilderGetRequestConfiguration{
		Headers:         headers,
		QueryParameters: requestParameters,
	}

	result, err := a.graphClient.Groups().ByGroupId(groupID).Members().Get(ctx, configuration)
	if err != nil {
		return nil, err
	}

	pageIterator, err := msgraphcore.NewPageIterator[models.DirectoryObjectable](
		result,
		a.graphClient.GetAdapter(),
		models.CreateDirectoryObjectCollectionResponseFromDiscriminatorValue,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create members page iterator")
	}

	var rawMembers []models.DirectoryObjectable
	err = pageIterator.Iterate(context.Background(), func(pageItem models.DirectoryObjectable) bool {
		rawMembers = append(rawMembers, pageItem)
		// Return true to continue the iteration.
		return true
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to iterate over Azure group members")
	}

	return rawMembers, nil

}

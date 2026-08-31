package azure

import (
	"github.com/tractoai/ytsaurus-identity-sync/pkg/source"
	"go.ytsaurus.tech/yt/go/yson"
)

type AzureUser struct {
	// PrincipalName is unique human-readable Azure user field, used (possibly with changes)
	// for the corresponding YTsaurus user's `name` attribute.
	PrincipalName string `yson:"principal_name"`

	AzureID     source.ObjectID `yson:"id"`
	Email       string          `yson:"email"`
	FirstName   string          `yson:"first_name"`
	LastName    string          `yson:"last_name"`
	DisplayName string          `yson:"display_name"`
}

func NewAzureUser(attributes map[string]any) (*AzureUser, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	var azureUser AzureUser
	err = yson.Unmarshal(bytes, &azureUser)
	if err != nil {
		return nil, err
	}
	return &azureUser, nil
}

func (au AzureUser) GetID() source.ObjectID {
	return au.AzureID
}

func (au AzureUser) GetName() string {
	return au.PrincipalName
}

func (au AzureUser) GetRaw() (map[string]any, error) {
	bytes, err := yson.Marshal(au)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]any)
	err = yson.Unmarshal(bytes, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

type AzureGroup struct {
	AzureID     source.ObjectID `yson:"id"`
	DisplayName string          `yson:"display_name"`
}

func NewAzureGroup(attributes map[string]any) (*AzureGroup, error) {
	bytes, err := yson.Marshal(attributes)
	if err != nil {
		return nil, err
	}

	var azureGroup AzureGroup
	err = yson.Unmarshal(bytes, &azureGroup)
	if err != nil {
		return nil, err
	}
	return &azureGroup, nil
}

func (ag AzureGroup) GetID() source.ObjectID {
	return ag.AzureID
}

func (ag AzureGroup) GetName() string {
	return ag.DisplayName
}

func (ag AzureGroup) GetRaw() (map[string]any, error) {
	bytes, err := yson.Marshal(ag)
	if err != nil {
		return nil, err
	}

	raw := make(map[string]any)
	err = yson.Unmarshal(bytes, &raw)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

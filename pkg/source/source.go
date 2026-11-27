package source

import mapset "github.com/deckarep/golang-set/v2"

type ObjectID = string

// Source provides users and groups from an external identity provider.
type Source interface {
	GetUsers() ([]SourceUser, error)
	GetGroupsWithMembers() ([]SourceGroupWithMembers, error)
	CreateUserFromRaw(raw map[string]any) (SourceUser, error)
	CreateGroupFromRaw(raw map[string]any) (SourceGroup, error)
}

type SourceUser interface {
	GetID() ObjectID
	GetName() string
	GetRaw() (map[string]any, error)
	// GetRebindingAttributes returns profile attributes that must match before rebinding
	// a new ID. Keys must match GetRaw, and values are compared exactly with stored strings.
	// Exclude the ID and username: the app handles those separately.
	GetRebindingAttributes() map[string]string
}

type SourceGroup interface {
	GetID() ObjectID
	GetName() string
	GetRaw() (map[string]any, error)
}

type SourceGroupWithMembers struct {
	SourceGroup SourceGroup
	// Members is a set of strings representing user IDs in the source.
	Members mapset.Set[string]
}

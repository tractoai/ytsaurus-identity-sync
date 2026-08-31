package ldap

import mapset "github.com/deckarep/golang-set/v2"

func NewStringSetFromItems(items ...string) mapset.Set[string] {
	return mapset.NewSet(items...)
}

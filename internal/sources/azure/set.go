package azure

import mapset "github.com/deckarep/golang-set/v2"

func NewStringSet() mapset.Set[string] {
	return mapset.NewSet[string]()
}

func NewStringSetFromItems(items ...string) mapset.Set[string] {
	return mapset.NewSet(items...)
}

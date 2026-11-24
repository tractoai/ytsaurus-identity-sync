package app

import mapset "github.com/deckarep/golang-set/v2"

const appTimeFormat = "2006-01-02T15:04:05Z0700"

func NewStringSet() mapset.Set[string] {
	return mapset.NewSet[string]()
}

func NewStringSetFromItems(items ...string) mapset.Set[string] {
	return mapset.NewSet(items...)
}

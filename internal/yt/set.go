package yt

import mapset "github.com/deckarep/golang-set/v2"

type StringSet = mapset.Set[string]

func NewStringSet() StringSet {
	return mapset.NewSet[string]()
}

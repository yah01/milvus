package utils

import "github.com/milvus-io/milvus/internal/querycoordv2/meta"

func FilterReleased[E interface{ GetCollectionID() int64 }](elems []E, collections []int64) []E {
	collMap := make(map[int64]struct{})
	for _, cid := range collections {
		collMap[cid] = struct{}{}
	}
	ret := make([]E, 0, len(elems))
	for _, s := range elems {
		if _, ok := collMap[s.GetCollectionID()]; !ok {
			ret = append(ret, s)
		}
	}
	return ret
}

func FindMaxVersionSegments(segments []*meta.Segment) []*meta.Segment {
	versions := make(map[int64]int64)
	segMap := make(map[int64]*meta.Segment)
	for _, s := range segments {
		v, ok := versions[s.GetID()]
		if !ok || v < s.Version {
			versions[s.GetID()] = s.Version
			segMap[s.GetID()] = s
		}
	}
	ret := make([]*meta.Segment, 0)
	for _, s := range segMap {
		ret = append(ret, s)
	}
	return ret
}

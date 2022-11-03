// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

/*
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querynodev2/params"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

var (
	cgoPool *concurrency.Pool
)

func init() {
	var err error
	cgoPool, err = concurrency.NewPool(runtime.NumCPU())
	if err != nil {
		panic(err)
	}
}

type SegmentFilter func(segment *LocalSegment) bool

func WithCollection(collectionID UniqueID) SegmentFilter {
	return func(segment *LocalSegment) bool {
		return segment.collectionID == collectionID
	}
}

func WithPartition(partitionID UniqueID) SegmentFilter {
	return func(segment *LocalSegment) bool {
		return segment.partitionID == partitionID
	}
}

func WithType(typ SegmentType) SegmentFilter {
	return func(segment *LocalSegment) bool {
		return segment.typ == typ
	}
}

type Manager interface {
	// Collection related

	// PutCollectionAndRef puts the given collection in,
	// and increases the ref count of the given collection,
	// returns the increased ref count
	PutCollectionAndRef(collectionID UniqueID, schema *schemapb.CollectionSchema, loadType querypb.LoadType) int32

	// UnrefCollection decreases the ref count of the given collection,
	// this will remove the collection if it sets the ref count to 0,
	// returns the decreased ref count
	UnrefCollection(collectionID UniqueID) int32
	GetCollection(collectionID UniqueID) *Collection

	// Segment related

	// Put puts the given segments in,
	// and increases the ref count of the corresponding collection,
	// dup segments will not increase the ref count
	Put(segmentType SegmentType, segments ...*LocalSegment)
	Get(segmentID UniqueID) *LocalSegment
	GetBy(filters ...SegmentFilter) []*LocalSegment
	GetSealed(segmentID UniqueID) *LocalSegment
	GetGrowing(segmentID UniqueID) *LocalSegment
	// Remove removes the given segment,
	// and decreases the ref count of the corresponding collection,
	// will not decrease the ref count if the given segment not exists
	Remove(segmentID UniqueID, scope querypb.DataScope)
}

// Manager manages all collections and segments
type manager struct {
	mu sync.RWMutex // guards all

	collections     map[UniqueID]*Collection
	growingSegments map[UniqueID]*LocalSegment
	sealedSegments  map[UniqueID]*LocalSegment
}

var _ Manager = (*manager)(nil)

// getSegmentsMemSize get the memory size in bytes of all the Segments
// func (mgr *manager) getSegmentsMemSize() int64 {
// 	mgr.mu.RLock()
// 	defer mgr.mu.RUnlock()

// 	memSize := int64(0)
// 	for _, segment := range mgr.growingSegments {
// 		memSize += segment.getMemSize()
// 	}
// 	for _, segment := range mgr.sealedSegments {
// 		memSize += segment.getMemSize()
// 	}
// 	return memSize
// }

func (mgr *manager) PutCollectionAndRef(collectionID UniqueID, schema *schemapb.CollectionSchema, loadType querypb.LoadType) int32 {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if collection, ok := mgr.collections[collectionID]; ok {
		return collection.ref.Inc()
	}

	var collection = NewCollection(collectionID, schema, loadType)
	mgr.collections[collectionID] = collection
	metrics.QueryNodeNumCollections.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(len(mgr.collections)))
	return collection.ref.Inc()
}

func (mgr *manager) GetCollection(collectionID UniqueID) *Collection {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	return mgr.collections[collectionID]
}

func (mgr *manager) UnrefCollection(collectionID UniqueID) int32 {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	collection, ok := mgr.collections[collectionID]
	if !ok {
		return 0
	}

	ref := collection.ref.Dec()
	if ref <= 0 {
		DeleteCollection(collection)
		delete(mgr.collections, collectionID)
		metrics.QueryNodeNumCollections.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(len(mgr.collections)))
	}
	return ref
}

func (mgr *manager) Put(segmentType SegmentType, segments ...*LocalSegment) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	targetMap := mgr.growingSegments
	switch segmentType {
	case SegmentTypeGrowing:
		targetMap = mgr.growingSegments
	case SegmentTypeSealed:
		targetMap = mgr.sealedSegments
	default:
		panic("unexpected segment type")
	}

	for _, segment := range segments {
		targetMap[segment.ID()] = segment
	}
}

func (mgr *manager) Get(segmentID UniqueID) *LocalSegment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.growingSegments[segmentID]; ok {
		return segment
	} else if segment, ok = mgr.sealedSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *manager) GetBy(filters ...SegmentFilter) []*LocalSegment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	ret := make([]*LocalSegment, 0)
	for _, segment := range mgr.sealedSegments {
		if filter(segment, filters...) {
			ret = append(ret, segment)
		}
	}
	return ret
}

func filter(segment *LocalSegment, filters ...SegmentFilter) bool {
	for _, filter := range filters {
		if !filter(segment) {
			return false
		}
	}
	return true
}

func (mgr *manager) GetSealed(segmentID UniqueID) *LocalSegment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.sealedSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *manager) GetGrowing(segmentID UniqueID) *LocalSegment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.growingSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *manager) Remove(segmentID UniqueID, scope querypb.DataScope) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	switch scope {
	case querypb.DataScope_Streaming:
		delete(mgr.growingSegments, segmentID)

	case querypb.DataScope_Historical:
		delete(mgr.sealedSegments, segmentID)

	case querypb.DataScope_All:
		delete(mgr.growingSegments, segmentID)
		delete(mgr.sealedSegments, segmentID)
	}
}

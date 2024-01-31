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

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type LoadStatus string

const (
	LoadStatusMeta     LoadStatus = "meta"
	LoadStatusMapped              = "mapped"
	LoadStatusInMemory            = "in_memory"
)

type Segment interface {
	// Properties
	ID() int64
	Collection() int64
	Partition() int64
	Shard() string
	Version() int64
	CASVersion(int64, int64) bool
	StartPosition() *msgpb.MsgPosition
	Type() SegmentType
	Level() datapb.SegmentLevel
	LoadStatus() LoadStatus
	RLock() error
	RUnlock()

	// Stats related
	// InsertCount returns the number of inserted rows, not effected by deletion
	InsertCount() int64
	// RowNum returns the number of rows, it's slow, so DO NOT call it in a loop
	RowNum() int64
	MemSize() int64

	// Index related
	GetIndex(fieldID int64) *IndexedFieldInfo
	ExistIndex(fieldID int64) bool
	Indexes() []*IndexedFieldInfo
	HasRawData(fieldID int64) bool

	// Modification related
	Insert(ctx context.Context, rowIDs []int64, timestamps []typeutil.Timestamp, record *segcorepb.InsertRecord) error
	Delete(ctx context.Context, primaryKeys []storage.PrimaryKey, timestamps []typeutil.Timestamp) error
	LoadDeltaData(ctx context.Context, deltaData *storage.DeleteData) error
	LastDeltaTimestamp() uint64
	Release()

	// Bloom filter related
	UpdateBloomFilter(pks []storage.PrimaryKey)
	MayPkExist(pk storage.PrimaryKey) bool

	// Read operations
	Search(ctx context.Context, searchReq *SearchRequest) (*SearchResult, error)
	Retrieve(ctx context.Context, plan *RetrievePlan) (*segcorepb.RetrieveResults, error)
}

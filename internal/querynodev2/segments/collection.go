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
	"sync"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/atomic"
)

// Collection is a wrapper of the underlying C-structure C.CCollection
type Collection struct {
	mu            sync.RWMutex // protects colllectionPtr
	collectionPtr C.CCollection
	id            int64
	partitions    typeutil.UniqueSet
	loadType      querypb.LoadType
	schema        *schemapb.CollectionSchema
	ref           *atomic.Int32
}

// ID returns collection id
func (c *Collection) ID() int64 {
	return c.id
}

// Schema returns the schema of collection
func (c *Collection) Schema() *schemapb.CollectionSchema {
	return c.schema
}

// getPartitionIDs return partitionIDs of collection
func (c *Collection) GetPartitions() []int64 {
	return c.partitions.Collect()
}

// addPartitionID would add a partition id to partition id list of collection
func (c *Collection) AddPartition(partitionID int64) {
	c.partitions.Insert(partitionID)
}

// removePartitionID removes the partition id from partition id list of collection
func (c *Collection) RemovePartition(partitionID int64) {
	c.partitions.Remove(partitionID)
}

// getLoadType get the loadType of collection, which is loadTypeCollection or loadTypePartition
func (c *Collection) GetLoadType() querypb.LoadType {
	return c.loadType
}

// newCollection returns a new Collection
func NewCollection(collectionID int64, schema *schemapb.CollectionSchema, loadType querypb.LoadType) *Collection {
	/*
		CCollection
		NewCollection(const char* schema_proto_blob);
	*/
	schemaBlob := proto.MarshalTextString(schema)
	cSchemaBlob := C.CString(schemaBlob)
	defer C.free(unsafe.Pointer(cSchemaBlob))

	collection := C.NewCollection(cSchemaBlob)
	return &Collection{
		collectionPtr: collection,
		id:            collectionID,
		schema:        schema,
		partitions:    typeutil.NewUniqueSet(),
		loadType:      loadType,
		ref:           atomic.NewInt32(0),
	}
}

// deleteCollection delete collection and free the collection memory
func DeleteCollection(collection *Collection) {
	/*
		void
		deleteCollection(CCollection collection);
	*/
	cPtr := collection.collectionPtr
	C.DeleteCollection(cPtr)

	collection.collectionPtr = nil
}

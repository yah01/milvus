package segments

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

func GetPkField(schema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	for _, field := range schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field
		}
	}
	return nil
}

// ProcessDeleteMessages would execute delete operations for growing segments
func DispatchDeleteMessages(segments map[int64]*LocalSegment, msg *msgstream.DeleteMsg, deleteRecords map[int64]*DeleteRecord) {
	primaryKeys := storage.ParseIDs2PrimaryKeys(msg.PrimaryKeys)
	for segmentID, segment := range segments {
		pks, tss := filterSegmentsByPKs(primaryKeys, msg.Timestamps, segment)
		deleteRecord, ok := deleteRecords[segmentID]
		if !ok {
			deleteRecord = NewDeleteRecordWithCap(len(pks))
			deleteRecords[segmentID] = deleteRecord
		}
		deleteRecord.PrimaryKeys = append(deleteRecord.PrimaryKeys, pks...)
		deleteRecord.Timestamps = append(deleteRecord.Timestamps, tss...)
	}
}

// filterSegmentsByPKs would filter segments by primary keys
func filterSegmentsByPKs(pks []storage.PrimaryKey, timestamps []Timestamp, segment *LocalSegment) ([]storage.PrimaryKey, []Timestamp) {
	retPks := make([]storage.PrimaryKey, 0)
	retTss := make([]Timestamp, 0)
	for index, pk := range pks {
		if segment.MayPkExist(pk) {
			retPks = append(retPks, pk)
			retTss = append(retTss, timestamps[index])
		}
	}
	return retPks, retTss
}

// TODO: remove this function to proper file
// getPrimaryKeys would get primary keys by insert messages
func GetPrimaryKeys(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	if msg.IsRowBased() {
		return getPKsFromRowBasedInsertMsg(msg, schema)
	}
	return getPKsFromColumnBasedInsertMsg(msg, schema)
}

func getPKsFromRowBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	offset := 0
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			break
		}
		switch field.DataType {
		case schemapb.DataType_Bool:
			offset++
		case schemapb.DataType_Int8:
			offset++
		case schemapb.DataType_Int16:
			offset += 2
		case schemapb.DataType_Int32:
			offset += 4
		case schemapb.DataType_Int64:
			offset += 8
		case schemapb.DataType_Float:
			offset += 4
		case schemapb.DataType_Double:
			offset += 8
		case schemapb.DataType_FloatVector:
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim * 4
					break
				}
			}
		case schemapb.DataType_BinaryVector:
			for _, t := range field.TypeParams {
				if t.Key == "dim" {
					dim, err := strconv.Atoi(t.Value)
					if err != nil {
						return nil, fmt.Errorf("strconv wrong on get dim, err = %s", err)
					}
					offset += dim / 8
					break
				}
			}
		}
	}

	blobReaders := make([]io.Reader, len(msg.RowData))
	for i, blob := range msg.RowData {
		blobReaders[i] = bytes.NewReader(blob.GetValue()[offset : offset+8])
	}
	pks := make([]storage.PrimaryKey, len(blobReaders))

	for i, reader := range blobReaders {
		var int64PkValue int64
		err := binary.Read(reader, common.Endian, &int64PkValue)
		if err != nil {
			log.Warn("binary read blob value failed", zap.Error(err))
			return nil, err
		}
		pks[i] = storage.NewInt64PrimaryKey(int64PkValue)
	}

	return pks, nil
}

func getPKsFromColumnBasedInsertMsg(msg *msgstream.InsertMsg, schema *schemapb.CollectionSchema) ([]storage.PrimaryKey, error) {
	primaryFieldSchema, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	primaryFieldData, err := typeutil.GetPrimaryFieldData(msg.GetFieldsData(), primaryFieldSchema)
	if err != nil {
		return nil, err
	}

	pks, err := storage.ParseFieldData2PrimaryKeys(primaryFieldData)
	if err != nil {
		return nil, err
	}

	return pks, nil
}

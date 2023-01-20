package storage

import (
	"bytes"
	"unsafe"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/parquet"
	"github.com/apache/arrow/go/v8/parquet/file"
	"github.com/apache/arrow/go/v8/parquet/metadata"
	"github.com/apache/arrow/go/v8/parquet/schema"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

const (
	RowGroupSize = 256 * 1024 * 1024 // 256 MiB
)

type PayloadWriterV2 struct {
	writer     *file.Writer
	rowGroup   file.SerialRowGroupWriter
	column     file.ColumnChunkWriter
	columnType schemapb.DataType
	dim        int
	buffer     *bytes.Buffer
}

func NewPayloadWriterV2(columnType schemapb.DataType, dim ...int) (*PayloadWriterV2, error) {
	var err error
	w := &PayloadWriterV2{
		columnType: columnType,
		buffer:     &bytes.Buffer{},
	}
	if len(dim) > 0 {
		w.dim = dim[0]
	}
	schema := schema.s
	w.writer = file.NewParquetWriter(w.buffer, &schema.GroupNode{})
	w.writer.
	w.rowGroup = w.writer.AppendRowGroup()
	w.rowGroup.NextColumn()
	w.column, err = w.rowGroup.NextColumn()
	return w, err
}

func (w *PayloadWriterV2) AddDataToPayload(data interface{}, dim ...int) error {
	panic("not implemented") // TODO: Implement
}

func (w *PayloadWriterV2) AddBoolToPayload(data []bool) error {
	panic("not implemented") // TODO: Implement
}

func (w *PayloadWriterV2) AddByteToPayload(data []byte) error {
	panic("not implemented") // TODO: Implement
}

func (w *PayloadWriterV2) AddInt8ToPayload(data []int8) error {
	panic("not implemented") // TODO: Implement
}

func (w *PayloadWriterV2) AddInt16ToPayload(data []int16) error {
	panic("not implemented") // TODO: Implement
}

func (w *PayloadWriterV2) AddInt32ToPayload(data []int32) error {
	if w.rowGroup.TotalBytesWritten()+sliceByteSize(data) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	return writeBatch[int32, *file.Int32ColumnChunkWriter](w.column, data)
}

func (w *PayloadWriterV2) AddInt64ToPayload(data []int64) error {
	if w.rowGroup.TotalBytesWritten()+sliceByteSize(data) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	return writeBatch[int64, *file.Int64ColumnChunkWriter](w.column, data)
}

func (w *PayloadWriterV2) AddFloatToPayload(data []float32) error {
	if w.rowGroup.TotalBytesWritten()+sliceByteSize(data) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	return writeBatch[float32, *file.Float32ColumnChunkWriter](w.column, data)
}

func (w *PayloadWriterV2) AddDoubleToPayload(data []float64) error {
	if w.rowGroup.TotalBytesWritten()+sliceByteSize(data) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	return writeBatch[float64, *file.Float64ColumnChunkWriter](w.column, data)
}

func (w *PayloadWriterV2) AddOneStringToPayload(data string) error {
	if w.rowGroup.TotalBytesWritten()+int64(len(data)) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	return writeBatch[parquet.ByteArray, *file.ByteArrayColumnChunkWriter](
		w.column, parquet.ByteArrayTraits.CastFromBytes([]byte(data)))
}

func (w *PayloadWriterV2) AddBinaryVectorToPayload(data []byte, dim int) error {
	if w.rowGroup.TotalBytesWritten()+sliceByteSize(data) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	return writeBatch[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkWriter](
		w.column, parquet.FixedLenByteArrayTraits.CastFromBytes(data))
}

func (w *PayloadWriterV2) AddFloatVectorToPayload(data []float32, dim int) error {
	if w.rowGroup.TotalBytesWritten()+sliceByteSize(data) > RowGroupSize {
		w.rowGroup = w.writer.AppendRowGroup()
	}

	rawBytes := arrow.Float32Traits.CastToBytes(data)
	bytes := parquet.FixedLenByteArrayTraits.CastFromBytes(rawBytes)
	return writeBatch[parquet.FixedLenByteArray, *file.FixedLenByteArrayColumnChunkWriter](
		w.column, bytes)
}

func (w *PayloadWriterV2) Flush() error {
	return w.Close()
}

func (w *PayloadWriterV2) GetPayloadBufferFromWriter() ([]byte, error) {
	return w.buffer.Bytes(), nil
}

func (w *PayloadWriterV2) GetPayloadLengthFromWriter() (int, error) {
	return w.writer.NumRows(), nil
}

func (w *PayloadWriterV2) Close() error {
	return w.writer.Close()
}

func writeBatch[T any, W interface {
	WriteBatch(values []T, defLevels, repLevels []int16) (valueOffset int64, err error)
}](writer file.ColumnChunkWriter, data []T) error {
	columnWriter := writer.(W)
	_, err := columnWriter.WriteBatch(data, nil, nil)
	return err
}

func sliceByteSize[T any](slice []T) int64 {
	if len(slice) == 0 {
		return 0
	}
	return int64(unsafe.Sizeof(slice[0])) * int64(len(slice))
}

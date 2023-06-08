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

package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// EventTypeCode represents event type by code
type EventTypeCode int8

// EventTypeCode definitions
const (
	DescriptorEventType EventTypeCode = iota
	InsertEventType
	DeleteEventType
	CreateCollectionEventType
	DropCollectionEventType
	CreatePartitionEventType
	DropPartitionEventType
	IndexFileEventType
	EventTypeEnd
)

// String returns the string representation
func (code EventTypeCode) String() string {
	codes := map[EventTypeCode]string{
		DescriptorEventType:       "DescriptorEventType",
		InsertEventType:           "InsertEventType",
		DeleteEventType:           "DeleteEventType",
		CreateCollectionEventType: "CreateCollectionEventType",
		DropCollectionEventType:   "DropCollectionEventType",
		CreatePartitionEventType:  "CreatePartitionEventType",
		DropPartitionEventType:    "DropPartitionEventType",
		IndexFileEventType:        "IndexFileEventType",
	}
	if eventTypeStr, ok := codes[code]; ok {
		return eventTypeStr
	}
	return "InvalidEventType"
}

type descriptorEvent struct {
	descriptorEventHeader
	descriptorEventData
}

// GetMemoryUsageInBytes returns descriptor Event memory usage in bytes
func (event *descriptorEvent) GetMemoryUsageInBytes() int32 {
	return event.descriptorEventHeader.GetMemoryUsageInBytes() + event.descriptorEventData.GetMemoryUsageInBytes()
}

// Write writes descriptor event into buffer
func (event *descriptorEvent) Write(buffer io.Writer) error {
	err := event.descriptorEventData.FinishExtra()
	if err != nil {
		return err
	}
	event.descriptorEventHeader.EventLength = event.descriptorEventHeader.GetMemoryUsageInBytes() + event.descriptorEventData.GetMemoryUsageInBytes()
	event.descriptorEventHeader.NextPosition = int32(binary.Size(MagicNumber)) + event.descriptorEventHeader.EventLength

	if err := event.descriptorEventHeader.Write(buffer); err != nil {
		return err
	}
	if err := event.descriptorEventData.Write(buffer); err != nil {
		return err
	}
	return nil
}

// EventWriter abstracts event writer
type EventWriter interface {
	PayloadWriterInterface
	// Finish set meta in header and no data can be added to event writer
	Finish() error
	// Close release resources
	Close()
	GetMemoryUsageInBytes() (int32, error)
	SetOffset(offset int32)
	Header() eventHeader
}

type baseEventWriter struct {
	eventHeader
	PayloadWriterInterface
	isClosed         bool
	isFinish         bool
	offset           int32
	buf              io.Writer
	getEventDataSize func() int32
	writeEventData   func(buffer io.Writer) error
}

func (writer *baseEventWriter) GetMemoryUsageInBytes() (int32, error) {
	size := writer.getEventDataSize() + writer.eventHeader.GetMemoryUsageInBytes() + int32(writer.BufMemSize())
	return size, nil
}

func (writer *baseEventWriter) Finish() error {
	if !writer.isFinish {
		writer.isFinish = true

		if err := writer.eventHeader.Write(writer.buf); err != nil {
			return err
		}
		if err := writer.writeEventData(writer.buf); err != nil {
			return err
		}
		if err := writer.FinishPayloadWriter(); err != nil {
			return err
		}

		eventLength, err := writer.GetMemoryUsageInBytes()
		if err != nil {
			return err
		}
		writer.EventLength = eventLength
		writer.NextPosition = eventLength + writer.offset
	}
	return nil
}

func (writer *baseEventWriter) Close() {
	if !writer.isClosed {
		writer.isFinish = true
		writer.isClosed = true
		writer.ReleasePayloadWriter()
	}
}

func (writer *baseEventWriter) SetOffset(offset int32) {
	writer.offset = offset
}

func (writer *baseEventWriter) Header() eventHeader {
	return writer.eventHeader
}

type insertEventWriter struct {
	baseEventWriter
	insertEventData
}

type deleteEventWriter struct {
	baseEventWriter
	deleteEventData
}

type createCollectionEventWriter struct {
	baseEventWriter
	createCollectionEventData
}

type dropCollectionEventWriter struct {
	baseEventWriter
	dropCollectionEventData
}

type createPartitionEventWriter struct {
	baseEventWriter
	createPartitionEventData
}

type dropPartitionEventWriter struct {
	baseEventWriter
	dropPartitionEventData
}

type indexFileEventWriter struct {
	baseEventWriter
	indexFileEventData
}

func newDescriptorEvent() *descriptorEvent {
	header := newDescriptorEventHeader()
	data := newDescriptorEventData()
	return &descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}
}

func newInsertEventWriter(dataType schemapb.DataType, buf *bytes.Buffer, dim ...int) (*insertEventWriter, error) {
	var payloadWriter PayloadWriterInterface
	var err error
	if typeutil.IsVectorType(dataType) {
		if len(dim) != 1 {
			return nil, fmt.Errorf("incorrect input numbers")
		}
		payloadWriter, err = NewPayloadWriter(dataType, buf, dim[0])
	} else {
		payloadWriter, err = NewPayloadWriter(dataType, buf)
	}
	if err != nil {
		return nil, err
	}
	header := newEventHeader(InsertEventType)
	data := newInsertEventData()

	writer := &insertEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		insertEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.insertEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.insertEventData.WriteEventData
	return writer, nil
}

func newDeleteEventWriter(dataType schemapb.DataType, buf *bytes.Buffer) (*deleteEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType, buf)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(DeleteEventType)
	data := newDeleteEventData()

	writer := &deleteEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		deleteEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.deleteEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.deleteEventData.WriteEventData
	return writer, nil
}

func newCreateCollectionEventWriter(dataType schemapb.DataType, buf *bytes.Buffer) (*createCollectionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType, buf)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(CreateCollectionEventType)
	data := newCreateCollectionEventData()

	writer := &createCollectionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		createCollectionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.createCollectionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.createCollectionEventData.WriteEventData
	return writer, nil
}

func newDropCollectionEventWriter(dataType schemapb.DataType, buf *bytes.Buffer) (*dropCollectionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType, buf)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(DropCollectionEventType)
	data := newDropCollectionEventData()

	writer := &dropCollectionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		dropCollectionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.dropCollectionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.dropCollectionEventData.WriteEventData
	return writer, nil
}

func newCreatePartitionEventWriter(dataType schemapb.DataType, buf *bytes.Buffer) (*createPartitionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType, buf)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(CreatePartitionEventType)
	data := newCreatePartitionEventData()

	writer := &createPartitionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		createPartitionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.createPartitionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.createPartitionEventData.WriteEventData
	return writer, nil
}

func newDropPartitionEventWriter(dataType schemapb.DataType, buf *bytes.Buffer) (*dropPartitionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType, buf)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(DropPartitionEventType)
	data := newDropPartitionEventData()

	writer := &dropPartitionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		dropPartitionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.dropPartitionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.dropPartitionEventData.WriteEventData
	return writer, nil
}

func newIndexFileEventWriter(dataType schemapb.DataType, buf *bytes.Buffer) (*indexFileEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType, buf)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(IndexFileEventType)
	data := newIndexFileEventData()

	writer := &indexFileEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			buf:                    buf,
		},
		indexFileEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.indexFileEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.indexFileEventData.WriteEventData

	return writer, nil
}

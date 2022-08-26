// Code generated by mockery v2.14.0. DO NOT EDIT.

package meta

import (
	context "context"

	datapb "github.com/milvus-io/milvus/internal/proto/datapb"
	indexpb "github.com/milvus-io/milvus/internal/proto/indexpb"

	mock "github.com/stretchr/testify/mock"

	querypb "github.com/milvus-io/milvus/internal/proto/querypb"

	schemapb "github.com/milvus-io/milvus/internal/proto/schemapb"
)

// MockBroker is an autogenerated mock type for the Broker type
type MockBroker struct {
	mock.Mock
}

type MockBroker_Expecter struct {
	mock *mock.Mock
}

func (_m *MockBroker) EXPECT() *MockBroker_Expecter {
	return &MockBroker_Expecter{mock: &_m.Mock}
}

// AcquireSegmentsReferLock provides a mock function with given fields: ctx, segmentIDs
func (_m *MockBroker) AcquireSegmentsReferLock(ctx context.Context, segmentIDs []int64) error {
	ret := _m.Called(ctx, segmentIDs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []int64) error); ok {
		r0 = rf(ctx, segmentIDs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockBroker_AcquireSegmentsReferLock_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AcquireSegmentsReferLock'
type MockBroker_AcquireSegmentsReferLock_Call struct {
	*mock.Call
}

// AcquireSegmentsReferLock is a helper method to define mock.On call
//  - ctx context.Context
//  - segmentIDs []int64
func (_e *MockBroker_Expecter) AcquireSegmentsReferLock(ctx interface{}, segmentIDs interface{}) *MockBroker_AcquireSegmentsReferLock_Call {
	return &MockBroker_AcquireSegmentsReferLock_Call{Call: _e.mock.On("AcquireSegmentsReferLock", ctx, segmentIDs)}
}

func (_c *MockBroker_AcquireSegmentsReferLock_Call) Run(run func(ctx context.Context, segmentIDs []int64)) *MockBroker_AcquireSegmentsReferLock_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].([]int64))
	})
	return _c
}

func (_c *MockBroker_AcquireSegmentsReferLock_Call) Return(_a0 error) *MockBroker_AcquireSegmentsReferLock_Call {
	_c.Call.Return(_a0)
	return _c
}

// GetCollectionSchema provides a mock function with given fields: ctx, collectionID
func (_m *MockBroker) GetCollectionSchema(ctx context.Context, collectionID int64) (*schemapb.CollectionSchema, error) {
	ret := _m.Called(ctx, collectionID)

	var r0 *schemapb.CollectionSchema
	if rf, ok := ret.Get(0).(func(context.Context, int64) *schemapb.CollectionSchema); ok {
		r0 = rf(ctx, collectionID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*schemapb.CollectionSchema)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, collectionID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_GetCollectionSchema_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetCollectionSchema'
type MockBroker_GetCollectionSchema_Call struct {
	*mock.Call
}

// GetCollectionSchema is a helper method to define mock.On call
//  - ctx context.Context
//  - collectionID int64
func (_e *MockBroker_Expecter) GetCollectionSchema(ctx interface{}, collectionID interface{}) *MockBroker_GetCollectionSchema_Call {
	return &MockBroker_GetCollectionSchema_Call{Call: _e.mock.On("GetCollectionSchema", ctx, collectionID)}
}

func (_c *MockBroker_GetCollectionSchema_Call) Run(run func(ctx context.Context, collectionID int64)) *MockBroker_GetCollectionSchema_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *MockBroker_GetCollectionSchema_Call) Return(_a0 *schemapb.CollectionSchema, _a1 error) *MockBroker_GetCollectionSchema_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// GetIndexFilePaths provides a mock function with given fields: ctx, buildID
func (_m *MockBroker) GetIndexFilePaths(ctx context.Context, buildID int64) ([]*indexpb.IndexFilePathInfo, error) {
	ret := _m.Called(ctx, buildID)

	var r0 []*indexpb.IndexFilePathInfo
	if rf, ok := ret.Get(0).(func(context.Context, int64) []*indexpb.IndexFilePathInfo); ok {
		r0 = rf(ctx, buildID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*indexpb.IndexFilePathInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, buildID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_GetIndexFilePaths_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetIndexFilePaths'
type MockBroker_GetIndexFilePaths_Call struct {
	*mock.Call
}

// GetIndexFilePaths is a helper method to define mock.On call
//  - ctx context.Context
//  - buildID int64
func (_e *MockBroker_Expecter) GetIndexFilePaths(ctx interface{}, buildID interface{}) *MockBroker_GetIndexFilePaths_Call {
	return &MockBroker_GetIndexFilePaths_Call{Call: _e.mock.On("GetIndexFilePaths", ctx, buildID)}
}

func (_c *MockBroker_GetIndexFilePaths_Call) Run(run func(ctx context.Context, buildID int64)) *MockBroker_GetIndexFilePaths_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *MockBroker_GetIndexFilePaths_Call) Return(_a0 []*indexpb.IndexFilePathInfo, _a1 error) *MockBroker_GetIndexFilePaths_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// GetIndexInfo provides a mock function with given fields: ctx, collectionID, segmentID
func (_m *MockBroker) GetIndexInfo(ctx context.Context, collectionID int64, segmentID int64) ([]*querypb.FieldIndexInfo, error) {
	ret := _m.Called(ctx, collectionID, segmentID)

	var r0 []*querypb.FieldIndexInfo
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) []*querypb.FieldIndexInfo); ok {
		r0 = rf(ctx, collectionID, segmentID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*querypb.FieldIndexInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) error); ok {
		r1 = rf(ctx, collectionID, segmentID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_GetIndexInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetIndexInfo'
type MockBroker_GetIndexInfo_Call struct {
	*mock.Call
}

// GetIndexInfo is a helper method to define mock.On call
//  - ctx context.Context
//  - collectionID int64
//  - segmentID int64
func (_e *MockBroker_Expecter) GetIndexInfo(ctx interface{}, collectionID interface{}, segmentID interface{}) *MockBroker_GetIndexInfo_Call {
	return &MockBroker_GetIndexInfo_Call{Call: _e.mock.On("GetIndexInfo", ctx, collectionID, segmentID)}
}

func (_c *MockBroker_GetIndexInfo_Call) Run(run func(ctx context.Context, collectionID int64, segmentID int64)) *MockBroker_GetIndexInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *MockBroker_GetIndexInfo_Call) Return(_a0 []*querypb.FieldIndexInfo, _a1 error) *MockBroker_GetIndexInfo_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// GetPartitions provides a mock function with given fields: ctx, collectionID
func (_m *MockBroker) GetPartitions(ctx context.Context, collectionID int64) ([]int64, error) {
	ret := _m.Called(ctx, collectionID)

	var r0 []int64
	if rf, ok := ret.Get(0).(func(context.Context, int64) []int64); ok {
		r0 = rf(ctx, collectionID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int64)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, collectionID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_GetPartitions_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetPartitions'
type MockBroker_GetPartitions_Call struct {
	*mock.Call
}

// GetPartitions is a helper method to define mock.On call
//  - ctx context.Context
//  - collectionID int64
func (_e *MockBroker_Expecter) GetPartitions(ctx interface{}, collectionID interface{}) *MockBroker_GetPartitions_Call {
	return &MockBroker_GetPartitions_Call{Call: _e.mock.On("GetPartitions", ctx, collectionID)}
}

func (_c *MockBroker_GetPartitions_Call) Run(run func(ctx context.Context, collectionID int64)) *MockBroker_GetPartitions_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *MockBroker_GetPartitions_Call) Return(_a0 []int64, _a1 error) *MockBroker_GetPartitions_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// GetRecoveryInfo provides a mock function with given fields: ctx, collectionID, partitionID
func (_m *MockBroker) GetRecoveryInfo(ctx context.Context, collectionID int64, partitionID int64) ([]*datapb.VchannelInfo, []*datapb.SegmentBinlogs, error) {
	ret := _m.Called(ctx, collectionID, partitionID)

	var r0 []*datapb.VchannelInfo
	if rf, ok := ret.Get(0).(func(context.Context, int64, int64) []*datapb.VchannelInfo); ok {
		r0 = rf(ctx, collectionID, partitionID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*datapb.VchannelInfo)
		}
	}

	var r1 []*datapb.SegmentBinlogs
	if rf, ok := ret.Get(1).(func(context.Context, int64, int64) []*datapb.SegmentBinlogs); ok {
		r1 = rf(ctx, collectionID, partitionID)
	} else {
		if ret.Get(1) != nil {
			r1 = ret.Get(1).([]*datapb.SegmentBinlogs)
		}
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(context.Context, int64, int64) error); ok {
		r2 = rf(ctx, collectionID, partitionID)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// MockBroker_GetRecoveryInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetRecoveryInfo'
type MockBroker_GetRecoveryInfo_Call struct {
	*mock.Call
}

// GetRecoveryInfo is a helper method to define mock.On call
//  - ctx context.Context
//  - collectionID int64
//  - partitionID int64
func (_e *MockBroker_Expecter) GetRecoveryInfo(ctx interface{}, collectionID interface{}, partitionID interface{}) *MockBroker_GetRecoveryInfo_Call {
	return &MockBroker_GetRecoveryInfo_Call{Call: _e.mock.On("GetRecoveryInfo", ctx, collectionID, partitionID)}
}

func (_c *MockBroker_GetRecoveryInfo_Call) Run(run func(ctx context.Context, collectionID int64, partitionID int64)) *MockBroker_GetRecoveryInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64), args[2].(int64))
	})
	return _c
}

func (_c *MockBroker_GetRecoveryInfo_Call) Return(_a0 []*datapb.VchannelInfo, _a1 []*datapb.SegmentBinlogs, _a2 error) *MockBroker_GetRecoveryInfo_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

// GetSegmentInfo provides a mock function with given fields: ctx, segmentID
func (_m *MockBroker) GetSegmentInfo(ctx context.Context, segmentID ...int64) ([]*datapb.SegmentInfo, error) {
	_va := make([]interface{}, len(segmentID))
	for _i := range segmentID {
		_va[_i] = segmentID[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 []*datapb.SegmentInfo
	if rf, ok := ret.Get(0).(func(context.Context, ...int64) []*datapb.SegmentInfo); ok {
		r0 = rf(ctx, segmentID...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*datapb.SegmentInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...int64) error); ok {
		r1 = rf(ctx, segmentID...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_GetSegmentInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'GetSegmentInfo'
type MockBroker_GetSegmentInfo_Call struct {
	*mock.Call
}

// GetSegmentInfo is a helper method to define mock.On call
//  - ctx context.Context
//  - segmentID ...int64
func (_e *MockBroker_Expecter) GetSegmentInfo(ctx interface{}, segmentID ...interface{}) *MockBroker_GetSegmentInfo_Call {
	return &MockBroker_GetSegmentInfo_Call{Call: _e.mock.On("GetSegmentInfo",
		append([]interface{}{ctx}, segmentID...)...)}
}

func (_c *MockBroker_GetSegmentInfo_Call) Run(run func(ctx context.Context, segmentID ...int64)) *MockBroker_GetSegmentInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]int64, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(int64)
			}
		}
		run(args[0].(context.Context), variadicArgs...)
	})
	return _c
}

func (_c *MockBroker_GetSegmentInfo_Call) Return(_a0 []*datapb.SegmentInfo, _a1 error) *MockBroker_GetSegmentInfo_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// LoadIndexExtraInfo provides a mock function with given fields: ctx, fieldPathInfo
func (_m *MockBroker) LoadIndexExtraInfo(ctx context.Context, fieldPathInfo *indexpb.IndexFilePathInfo) (*ExtraIndexInfo, error) {
	ret := _m.Called(ctx, fieldPathInfo)

	var r0 *ExtraIndexInfo
	if rf, ok := ret.Get(0).(func(context.Context, *indexpb.IndexFilePathInfo) *ExtraIndexInfo); ok {
		r0 = rf(ctx, fieldPathInfo)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*ExtraIndexInfo)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *indexpb.IndexFilePathInfo) error); ok {
		r1 = rf(ctx, fieldPathInfo)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_LoadIndexExtraInfo_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'LoadIndexExtraInfo'
type MockBroker_LoadIndexExtraInfo_Call struct {
	*mock.Call
}

// LoadIndexExtraInfo is a helper method to define mock.On call
//  - ctx context.Context
//  - fieldPathInfo *indexpb.IndexFilePathInfo
func (_e *MockBroker_Expecter) LoadIndexExtraInfo(ctx interface{}, fieldPathInfo interface{}) *MockBroker_LoadIndexExtraInfo_Call {
	return &MockBroker_LoadIndexExtraInfo_Call{Call: _e.mock.On("LoadIndexExtraInfo", ctx, fieldPathInfo)}
}

func (_c *MockBroker_LoadIndexExtraInfo_Call) Run(run func(ctx context.Context, fieldPathInfo *indexpb.IndexFilePathInfo)) *MockBroker_LoadIndexExtraInfo_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(*indexpb.IndexFilePathInfo))
	})
	return _c
}

func (_c *MockBroker_LoadIndexExtraInfo_Call) Return(_a0 *ExtraIndexInfo, _a1 error) *MockBroker_LoadIndexExtraInfo_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// ReleaseSegmentReferLock provides a mock function with given fields: ctx, segmentIDs
func (_m *MockBroker) ReleaseSegmentReferLock(ctx context.Context, segmentIDs []int64) error {
	ret := _m.Called(ctx, segmentIDs)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []int64) error); ok {
		r0 = rf(ctx, segmentIDs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockBroker_ReleaseSegmentReferLock_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ReleaseSegmentReferLock'
type MockBroker_ReleaseSegmentReferLock_Call struct {
	*mock.Call
}

// ReleaseSegmentReferLock is a helper method to define mock.On call
//  - ctx context.Context
//  - segmentIDs []int64
func (_e *MockBroker_Expecter) ReleaseSegmentReferLock(ctx interface{}, segmentIDs interface{}) *MockBroker_ReleaseSegmentReferLock_Call {
	return &MockBroker_ReleaseSegmentReferLock_Call{Call: _e.mock.On("ReleaseSegmentReferLock", ctx, segmentIDs)}
}

func (_c *MockBroker_ReleaseSegmentReferLock_Call) Run(run func(ctx context.Context, segmentIDs []int64)) *MockBroker_ReleaseSegmentReferLock_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].([]int64))
	})
	return _c
}

func (_c *MockBroker_ReleaseSegmentReferLock_Call) Return(_a0 error) *MockBroker_ReleaseSegmentReferLock_Call {
	_c.Call.Return(_a0)
	return _c
}

type mockConstructorTestingTNewMockBroker interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockBroker creates a new instance of MockBroker. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockBroker(t mockConstructorTestingTNewMockBroker) *MockBroker {
	mock := &MockBroker{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

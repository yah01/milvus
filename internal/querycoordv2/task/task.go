package task

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type Status = int32
type Priority = int32

const (
	TaskStatusCreated Status = iota + 1
	TaskStatusStarted
	TaskStatusSucceeded
	TaskStatusCanceled
	TaskStatusStale
)

const (
	TaskPriorityLow = iota
	TaskPriorityNormal
	TaskPriorityHigh
)

var (
	// All task priorities from low to high
	TaskPriorities = []Priority{TaskPriorityLow, TaskPriorityNormal, TaskPriorityHigh}
)

type Task interface {
	Context() context.Context
	SourceID() UniqueID
	ID() UniqueID
	CollectionID() UniqueID
	ReplicaID() UniqueID
	SetID(id UniqueID)
	Status() Status
	SetStatus(status Status)
	Err() error
	SetErr(err error)
	Priority() Priority
	SetPriority(priority Priority)

	Cancel()
	Wait() error
	Actions() []Action
	Step() int
	IsFinished(dist *meta.DistributionManager) bool
}

type baseTask struct {
	ctx    context.Context
	cancel context.CancelFunc
	doneCh chan struct{}

	sourceID     UniqueID // RequestID
	id           UniqueID // Set by scheduler
	collectionID UniqueID
	replicaID    UniqueID
	loadType     querypb.LoadType

	status   Status
	priority Priority
	err      error
	actions  []Action
	step     int

	successCallbacks []func()
	failureCallbacks []func()
}

func newBaseTask(ctx context.Context, timeout time.Duration, sourceID, collectionID, replicaID UniqueID) *baseTask {
	ctx, cancel := context.WithTimeout(ctx, timeout)

	return &baseTask{
		sourceID:     sourceID,
		collectionID: collectionID,
		replicaID:    replicaID,

		status:   TaskStatusStarted,
		priority: TaskPriorityNormal,
		ctx:      ctx,
		cancel:   cancel,
		doneCh:   make(chan struct{}),
	}
}

func (task *baseTask) Context() context.Context {
	return task.ctx
}

func (task *baseTask) SourceID() UniqueID {
	return task.sourceID
}

func (task *baseTask) ID() UniqueID {
	return task.id
}

func (task *baseTask) SetID(id UniqueID) {
	task.id = id
}

func (task *baseTask) CollectionID() UniqueID {
	return task.collectionID
}

func (task *baseTask) ReplicaID() UniqueID {
	return task.replicaID
}

func (task *baseTask) LoadType() querypb.LoadType {
	return task.loadType
}

func (task *baseTask) Status() Status {
	return atomic.LoadInt32(&task.status)
}

func (task *baseTask) SetStatus(status Status) {
	atomic.StoreInt32(&task.status, status)
}

func (task *baseTask) Priority() Priority {
	return task.priority
}

func (task *baseTask) SetPriority(priority Priority) {
	task.priority = priority
}

func (task *baseTask) Err() error {
	return task.err
}

func (task *baseTask) SetErr(err error) {
	task.err = err
}

func (task *baseTask) Cancel() {
	task.cancel()
	select {
	case _, ok := <-task.doneCh:
		if ok {
			close(task.doneCh)
		}
	default:
	}
}

func (task *baseTask) Wait() error {
	<-task.doneCh
	return task.err
}

func (task *baseTask) Actions() []Action {
	return task.actions
}

func (task *baseTask) Step() int {
	return task.step
}

func (task *baseTask) IsFinished(distMgr *meta.DistributionManager) bool {
	if task.Status() != TaskStatusStarted {
		return false
	}

	actions, step := task.Actions(), task.Step()
	for step < len(actions) && actions[step].IsFinished(distMgr) {
		task.step++
		step++
	}

	return task.Step() >= len(actions)
}

type SegmentTask struct {
	*baseTask

	segmentID UniqueID
}

// NewSegmentTask creates a SegmentTask with actions,
// all actions must process the same segment,
// empty actions is not allowed
func NewSegmentTask(ctx context.Context,
	timeout time.Duration,
	sourceID,
	collectionID,
	replicaID UniqueID,
	actions ...Action) *SegmentTask {
	if len(actions) == 0 {
		panic("empty actions is not allowed")
	}

	segmentID := int64(-1)
	for _, action := range actions {
		action, ok := action.(*SegmentAction)
		if !ok {
			panic("SegmentTask can only contain SegmentActions")
		}
		if segmentID == -1 {
			segmentID = action.segmentID
		} else if segmentID != action.SegmentID() {
			panic("all actions must process the same segment")
		}
	}

	base := newBaseTask(ctx, timeout, sourceID, collectionID, replicaID)
	base.actions = actions
	return &SegmentTask{
		baseTask: base,

		segmentID: segmentID,
	}
}

func (task *SegmentTask) SegmentID() UniqueID {
	return task.segmentID
}

type ChannelTask struct {
	*baseTask

	channel string
}

// NewChannelTask creates a ChannelTask with actions,
// all actions must process the same channel, and the same type of channel
// empty actions is not allowed
func NewChannelTask(ctx context.Context,
	timeout time.Duration,
	sourceID,
	collectionID,
	replicaID UniqueID,
	actions ...Action) *ChannelTask {
	if len(actions) == 0 {
		panic("empty actions is not allowed")
	}

	channel := ""
	for _, action := range actions {
		channelAction, ok := action.(interface{ ChannelName() string })
		if !ok {
			panic("ChannelTask must contain only ChannelAction")
		}
		if channel == "" {
			channel = channelAction.ChannelName()
		} else if channel != channelAction.ChannelName() {
			panic("all actions must process the same channel")
		}
	}

	base := newBaseTask(ctx, timeout, sourceID, collectionID, replicaID)
	base.actions = actions
	return &ChannelTask{
		baseTask: base,

		channel: channel,
	}
}

func (task *ChannelTask) Channel() string {
	return task.channel
}

package tasks

import (
	"context"
	"runtime"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	MaxProcessTaskNum = 1024 * 10
)

type Scheduler struct {
	rwmutex sync.RWMutex

	searchProcessNum  *atomic.Int32
	searchWaitQueue   chan *SearchTask
	mergedSearchTasks []*SearchTask

	queryProcessQueue chan *QueryTask
	queryWaitQueue    chan *QueryTask

	pool *concurrency.Pool
}

func NewScheduler() *Scheduler {
	maxWaitTaskNum := paramtable.Get().QueryNodeCfg.MaxReceiveChanSize.GetAsInt()
	pool, err := concurrency.NewPool(runtime.GOMAXPROCS(0)*2, ants.WithPreAlloc(true))
	if err != nil {
		log.Fatal("failed to create pool", zap.Error(err))
	}
	return &Scheduler{
		searchProcessNum: atomic.NewInt32(0),
		searchWaitQueue:  make(chan *SearchTask, maxWaitTaskNum),
		// queryProcessQueue: make(chan),

		pool: pool,
	}
}

func (s *Scheduler) Add(task Task) bool {
	switch t := task.(type) {
	case *SearchTask:
		select {
		case s.searchWaitQueue <- t:
		default:
			return false
		}
	}

	return true
}

// schedule all tasks in the order:
// try execute merged tasks
// try execute waitting tasks
func (s *Scheduler) Schedule(ctx context.Context) {
	for {
		if len(s.mergedSearchTasks) > 0 {
			leftTasks := make([]*SearchTask, 0, len(s.mergedSearchTasks))
			for _, task := range s.mergedSearchTasks {
				if !s.tryPromote(task) {
					leftTasks = append(leftTasks, task)
					break
				}
				s.process(task)
			}
			s.mergedSearchTasks = leftTasks
		}

		select {
		case <-ctx.Done():
			return

		case t := <-s.searchWaitQueue:
			if err := t.Canceled(); err != nil {
				t.Done(err)
				continue
			}

			// Now we have no enough resource to execute this task,
			// just wait and try to merge it with another tasks
			if !s.tryPromote(t) {
				s.mergeTasks(t)
			} else {
				s.process(t)
			}
		}
	}
}

func (s *Scheduler) tryPromote(t Task) bool {
	current := s.searchProcessNum.Load()
	if current >= MaxProcessTaskNum ||
		!s.searchProcessNum.CAS(current, current+1) {
		return false
	}

	return true
}

func (s *Scheduler) process(t Task) {
	s.pool.Submit(func() (interface{}, error) {
		err := t.Execute()
		t.Done(err)
		s.searchProcessNum.Dec()
		return nil, err
	})
}

func (s *Scheduler) mergeTasks(t Task) {
	switch t := t.(type) {
	case *SearchTask:
		merged := false
		for _, task := range s.mergedSearchTasks {
			if task.Merge(t) {
				merged = true
				break
			}
		}
		if !merged {
			s.mergedSearchTasks = append(s.mergedSearchTasks, t)
		}
	}
}

package observers

import (
	"context"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/retry"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	waitIndexQueueSizeLimit = 50
)

type HandoffObserver struct {
	store    meta.Store
	c        chan struct{}
	wg       sync.WaitGroup
	meta     *meta.Meta
	dist     *meta.DistributionManager
	target   *meta.TargetManager
	broker   meta.Broker
	revision int64

	handoffEventLock sync.RWMutex
	// notice: another better way is to trigger a handoff after index ready
	waitingIndexQueue map[typeutil.UniqueID]*querypb.SegmentInfo
	//triggered events, used to check finish status
	triggeredHandoffEvents map[typeutil.UniqueID]*datapb.SegmentInfo
}

func NewHandoffObserver(
	ctx context.Context,
	store meta.Store,
	meta *meta.Meta,
	dist *meta.DistributionManager,
	target *meta.TargetManager,
	broker meta.Broker) *HandoffObserver {
	ob := &HandoffObserver{
		store:                  store,
		c:                      make(chan struct{}),
		meta:                   meta,
		dist:                   dist,
		target:                 target,
		broker:                 broker,
		waitingIndexQueue:      make(map[typeutil.UniqueID]*querypb.SegmentInfo, 0),
		triggeredHandoffEvents: map[typeutil.UniqueID]*datapb.SegmentInfo{},
	}

	return ob
}

func (ob *HandoffObserver) Start(ctx context.Context) {
	ob.wg.Add(1)
	go ob.schedule(ctx)
}

func (ob *HandoffObserver) Stop() {
	close(ob.c)
	ob.wg.Wait()
}

func (ob *HandoffObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("start watch segment handoff loop")
	ticker := time.NewTicker(Params.QueryCoordCfg.CheckHandoffInterval)
	watchChan := ob.store.WatchHandoffEvent(ob.revision + 1)
	for {
		select {
		case <-ctx.Done():
			log.Info("close handoff handler due to context done!")
			return
		case <-ob.c:
			log.Info("close handoff handler")
			return

		case resp, ok := <-watchChan:
			if !ok {
				log.Error("watch segment handoff loop failed because watch channel is closed!")
			}

			if err := resp.Err(); err != nil {
				log.Warn("receive error handoff event from etcd",
					zap.Error(err))
			}

			for _, event := range resp.Events {
				segmentInfo := &querypb.SegmentInfo{}
				err := proto.Unmarshal(event.Kv.Value, segmentInfo)
				if err != nil {
					log.Error("failed to deserialize handoff event", zap.Error(err))
					continue
				}

				switch event.Type {
				case mvccpb.PUT:
					ob.tryHandoff(ctx, segmentInfo)
				default:
					log.Warn("receive handoff event",
						zap.String("type", event.Type.String()),
						zap.String("key", string(event.Kv.Key)),
					)
				}
			}

		case <-ticker.C:
			for _, segment := range ob.waitingIndexQueue {
				ob.tryHandoff(ctx, segment)
			}
			ob.tryRelease()
			ob.tryClean(ctx)
		}
	}
}

func (ob *HandoffObserver) tryHandoff(ctx context.Context, segment *querypb.SegmentInfo) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()
	log := log.With(zap.Int64("collectionID", segment.CollectionID),
		zap.Int64("partitionID", segment.PartitionID),
		zap.Int64("segmentID", segment.SegmentID))

	if Params.QueryCoordCfg.AutoHandoff &&
		ob.meta.Exist(segment.GetCollectionID()) {
		// filter index ready segment
		isSegmentIndexReady := func(segment *querypb.SegmentInfo) bool {
			// TODO we sobuld not directly poll the index info, wait for notification sobuld be a better idea.
			_, err := ob.broker.GetIndexInfo(ctx, segment.CollectionID, segment.SegmentID)
			return err == nil
		}

		if ob.meta.GetStatus(segment.GetCollectionID()) != querypb.LoadStatus_Invalid &&
			isSegmentIndexReady(segment) {
			// when handoff event load a segment, it sobuld remove all recursive handoff compact from
			targets := ob.target.GetSegmentsByCollection(segment.GetCollectionID(), segment.GetPartitionID())
			recursiveCompactFrom := ob.getOverrideSegmentInfo(targets, segment.CompactionFrom...)
			recursiveCompactFrom = append(recursiveCompactFrom, segment.GetCompactionFrom()...)

			segmentInfo := &datapb.SegmentInfo{
				ID:                  segment.SegmentID,
				CollectionID:        segment.CollectionID,
				PartitionID:         segment.PartitionID,
				InsertChannel:       segment.GetDmChannel(),
				State:               segment.GetSegmentState(),
				CreatedByCompaction: segment.GetCreatedByCompaction(),
				CompactionFrom:      recursiveCompactFrom,
			}

			delete(ob.waitingIndexQueue, segment.SegmentID)
			ob.triggeredHandoffEvents[segment.SegmentID] = segmentInfo

			log.Info("handoff segment, register to target")
			ob.target.HandoffSegment(segmentInfo, segmentInfo.CompactionFrom...)
		} else {
			ob.waitingIndexQueue[segment.SegmentID] = segment
			if len(ob.waitingIndexQueue) > waitIndexQueueSizeLimit {
				log.Warn("waiting index queue size over limit",
					zap.Int("queueSize", len(ob.waitingIndexQueue)),
					zap.Int("queueSizeLimit", waitIndexQueueSizeLimit),
				)
			}
		}

	} else {
		// ignore handoff task
		log.Debug("handoff event trigger failed due to collection/partition is not loaded!")
		segmentInfo := &datapb.SegmentInfo{
			ID:                  segment.SegmentID,
			CollectionID:        segment.CollectionID,
			PartitionID:         segment.PartitionID,
			InsertChannel:       segment.DmChannel,
			CreatedByCompaction: segment.GetCreatedByCompaction(),
			CompactionFrom:      segment.CompactionFrom,
		}

		ob.cleanEvent(ctx, segmentInfo)
	}

}

func (ob *HandoffObserver) isSealedSegmentReleased(id typeutil.UniqueID) bool {
	return len(ob.dist.LeaderViewManager.GetSealedSegmentDist(id)) == 0
}

func (ob *HandoffObserver) isGrowingSegmentReleased(id typeutil.UniqueID) bool {
	return len(ob.dist.LeaderViewManager.GetGrowingSegmentDist(id)) == 0
}

func (ob *HandoffObserver) isSealedSegmentLoaded(segment *datapb.SegmentInfo) bool {
	// must be sealed segment loaded in all replica, in case of handoff between growing and sealed
	nodes := ob.dist.LeaderViewManager.GetSealedSegmentDist(segment.ID)
	replicas := utils.GroupNodesByReplica(ob.meta.ReplicaManager, segment.CollectionID, nodes)
	return len(replicas) == len(ob.meta.ReplicaManager.GetByCollection(segment.CollectionID))
}

func (ob *HandoffObserver) getOverrideSegmentInfo(handOffSegments []*datapb.SegmentInfo, segmentIDs ...typeutil.UniqueID) []typeutil.UniqueID {
	overrideSegments := make([]typeutil.UniqueID, 0)
	for _, segmentID := range segmentIDs {
		for _, segmentInHandoff := range handOffSegments {
			if segmentID == segmentInHandoff.ID {
				toReleaseSegments := ob.getOverrideSegmentInfo(handOffSegments, segmentInHandoff.CompactionFrom...)
				if len(toReleaseSegments) > 0 {
					overrideSegments = append(overrideSegments, toReleaseSegments...)
				}

				overrideSegments = append(overrideSegments, segmentID)
			}
		}
	}

	return overrideSegments
}

func (ob *HandoffObserver) cleanEvent(ctx context.Context, segmentInfo *datapb.SegmentInfo) error {
	log := log.With(zap.Int64("collectionID", segmentInfo.CollectionID),
		zap.Int64("partitionID", segmentInfo.PartitionID),
		zap.Int64("segmentID", segmentInfo.ID))

	// add retry logic
	err := retry.Do(ctx, func() error {
		return ob.store.RemoveHandoffEvent(segmentInfo)
	}, retry.Attempts(5))

	if err != nil {
		log.Warn("failed to clean handoff event from etcd", zap.Error(err))
	}
	return err
}

func (ob *HandoffObserver) tryRelease() {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()
	for _, segment := range ob.triggeredHandoffEvents {
		if ob.isSealedSegmentLoaded(segment) {
			compactSource := segment.CompactionFrom

			for _, toRelease := range compactSource {
				// when handoff happens between growing and sealed, both with same segment id, so can't remove from target here
				if segment.CreatedByCompaction {
					ob.target.RemoveSegment(toRelease)
				}
			}
		}
	}
}

func (ob *HandoffObserver) tryClean(ctx context.Context) {
	ob.handoffEventLock.Lock()
	defer ob.handoffEventLock.Unlock()
	for _, segment := range ob.triggeredHandoffEvents {
		if !ob.isSegmentExistOnTarget(segment) || ob.isAllCompactFromReleased(segment) {
			err := ob.cleanEvent(ctx, segment)
			if err == nil {
				delete(ob.triggeredHandoffEvents, segment.ID)
			}
		}
	}
}

func (ob *HandoffObserver) isSegmentExistOnTarget(segmentInfo *datapb.SegmentInfo) bool {
	return ob.target.ContainSegment(segmentInfo.ID)
}

func (ob *HandoffObserver) isAllCompactFromReleased(segmentInfo *datapb.SegmentInfo) bool {
	for _, segment := range segmentInfo.CompactionFrom {
		if segmentInfo.CreatedByCompaction {
			if !ob.isSealedSegmentReleased(segment) {
				return false
			}
		} else {
			if !ob.isGrowingSegmentReleased(segment) {
				return false
			}
		}
	}

	return true
}

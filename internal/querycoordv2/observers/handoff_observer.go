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

type HandoffObserver struct {
	store    meta.Store
	c        chan struct{}
	wg       sync.WaitGroup
	meta     *meta.Meta
	dist     *meta.DistributionManager
	target   *meta.TargetManager
	revision int64

	handoffEventLock sync.RWMutex
	//triggered events, used to check finish status
	triggeredHandoffEvents map[typeutil.UniqueID]*datapb.SegmentInfo
}

func NewHandoffObserver(store meta.Store, meta *meta.Meta, dist *meta.DistributionManager, target *meta.TargetManager) *HandoffObserver {
	return &HandoffObserver{
		store:                  store,
		c:                      make(chan struct{}),
		meta:                   meta,
		dist:                   dist,
		target:                 target,
		triggeredHandoffEvents: map[typeutil.UniqueID]*datapb.SegmentInfo{},
	}
}

func (ob *HandoffObserver) reloadFromStore(ctx context.Context) error {
	_, handoffReqValues, version, err := ob.store.LoadHandoffWithRevision()
	if err != nil {
		log.Error("reloadFromKV: LoadWithRevision from kv failed", zap.Error(err))
		return err
	}
	ob.revision = version

	for _, value := range handoffReqValues {
		segmentInfo := &querypb.SegmentInfo{}
		err := proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			log.Error("reloadFromKV: unmarshal failed", zap.Any("error", err.Error()))
			return err
		}
		ob.tryHandoff(ctx, segmentInfo)
	}

	return nil
}

func (ob *HandoffObserver) Start(ctx context.Context) error {
	log.Info("Start reload handoff event from etcd")
	if err := ob.reloadFromStore(ctx); err != nil {
		log.Error("handoff observer reload from kv failed", zap.Error(err))
		return err
	}
	log.Info("Finish reload handoff event from etcd")

	ob.wg.Add(1)
	go ob.schedule(ctx)

	return nil
}

func (ob *HandoffObserver) Stop() {
	close(ob.c)
	ob.wg.Wait()
}

func (ob *HandoffObserver) schedule(ctx context.Context) {
	defer ob.wg.Done()
	log.Info("start watch segment handoff loop")
	ticker := time.NewTicker(Params.QueryCoordCfg.CheckHandoffInterval)
	log.Info("handoff interval", zap.String("interval", Params.QueryCoordCfg.CheckHandoffInterval.String()))
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
					log.Warn("HandoffObserver: receive event",
						zap.String("type", event.Type.String()),
						zap.String("key", string(event.Kv.Key)),
					)
				}
			}

		case <-ticker.C:
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

	if Params.QueryCoordCfg.AutoHandoff && ob.meta.Exist(segment.GetCollectionID()) {
		targets := ob.target.GetSegmentsByCollection(segment.GetCollectionID(), segment.GetPartitionID())
		// when handoff event load a segment, it sobuld remove all recursive handoff compact from
		uniqueSet := typeutil.NewUniqueSet()
		recursiveCompactFrom := ob.getOverrideSegmentInfo(targets, segment.CompactionFrom...)
		uniqueSet.Insert(recursiveCompactFrom...)
		uniqueSet.Insert(segment.GetCompactionFrom()...)

		segmentInfo := &datapb.SegmentInfo{
			ID:                  segment.SegmentID,
			CollectionID:        segment.CollectionID,
			PartitionID:         segment.PartitionID,
			InsertChannel:       segment.GetDmChannel(),
			State:               segment.GetSegmentState(),
			CreatedByCompaction: segment.GetCreatedByCompaction(),
			CompactionFrom:      uniqueSet.Collect(),
		}

		ob.triggeredHandoffEvents[segment.SegmentID] = segmentInfo
		log.Info("HandoffObserver: handoff segment, register to target")
		ob.target.HandoffSegment(segmentInfo, segmentInfo.CompactionFrom...)
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

func (ob *HandoffObserver) isSegmentReleased(id typeutil.UniqueID) bool {
	return len(ob.dist.LeaderViewManager.GetSegmentDist(id)) == 0
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
	log := log.With(
		zap.Int64("collectionID", segmentInfo.CollectionID),
		zap.Int64("partitionID", segmentInfo.PartitionID),
		zap.Int64("segmentID", segmentInfo.ID),
	)

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
		if ob.meta.GetStatus(segment.GetCollectionID()) != querypb.LoadStatus_Invalid &&
			(ob.isSealedSegmentLoaded(segment) || !ob.isSegmentExistOnTarget(segment)) {
			compactSource := segment.CompactionFrom

			for _, toRelease := range compactSource {
				// when handoff happens between growing and sealed, both with same segment id, so can't remove from target here
				if segment.CreatedByCompaction {
					log.Info("HandoffObserver: remove compactFrom segment",
						zap.Int64("collectionID", segment.CollectionID),
						zap.Int64("partitionID", segment.PartitionID),
						zap.Int64("compactedSegment", segment.ID),
						zap.Int64("toReleaseSegment", toRelease),
					)
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
		if ob.isAllCompactFromReleased(segment) {
			log.Info("HandoffObserver: clean handoff event after handoff finished!",
				zap.Int64("collectionID", segment.CollectionID),
				zap.Int64("partitionID", segment.PartitionID),
				zap.Int64("segmentID", segment.ID),
			)
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
	if !segmentInfo.CreatedByCompaction {
		return !ob.isGrowingSegmentReleased(segmentInfo.ID)
	}

	for _, segment := range segmentInfo.CompactionFrom {
		if !ob.isSegmentReleased(segment) {
			return false
		}
	}
	return true
}

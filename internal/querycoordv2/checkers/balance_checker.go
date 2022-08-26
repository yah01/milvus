package checkers

import (
	"context"

	"github.com/milvus-io/milvus/internal/querycoordv2/balance"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
)

// BalanceChecker checks the cluster distribution and generates balance tasks.
type BalanceChecker struct {
	baseChecker
	balance.Balance
}

func NewBalanceChecker(balancer balance.Balance) *BalanceChecker {
	return &BalanceChecker{
		Balance: balancer,
	}
}

func (b *BalanceChecker) Description() string {
	return "BalanceChecker checks the cluster distribution and generates balance tasks"
}

func (b *BalanceChecker) Check(ctx context.Context) []task.Task {
	ret := make([]task.Task, 0)
	segmentPlans, channelPlans := b.Balance.Balance()
	tasks := balance.CreateSegmentTasksFromPlans(ctx, b.ID(), segmentTaskTimeout, segmentPlans)
	ret = append(ret, tasks...)
	tasks = balance.CreateChannelTasksFromPlans(ctx, b.ID(), channelTaskTimeout, channelPlans)
	ret = append(ret, tasks...)
	return ret
}

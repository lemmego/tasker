package batch

import (
	"context"

	"github.com/lemmego/tasker"
)

type Batcher struct {
	manager *tasker.Manager
}

func New(mgr *tasker.Manager) *Batcher {
	return &Batcher{manager: mgr}
}

func (b *Batcher) Dispatch(ctx context.Context, jobs []tasker.Job, opts ...tasker.DispatchOpt) ([]*tasker.JobRow, error) {
	return b.manager.DispatchBatch(ctx, jobs, opts...)
}

func (b *Batcher) Retry(ctx context.Context, ids []tasker.JobID) error {
	return b.manager.RetryBatch(ctx, ids)
}

func (b *Batcher) Cancel(ctx context.Context, ids []tasker.JobID) error {
	return b.manager.CancelBatch(ctx, ids)
}

func (b *Batcher) QueryByBatchID(ctx context.Context, batchID string) ([]*tasker.JobRow, error) {
	rows, _, err := b.manager.QueryJobs(ctx, tasker.JobFilter{
		Limit: 1000,
	})
	if err != nil {
		return nil, err
	}

	var result []*tasker.JobRow
	for _, row := range rows {
		if row.BatchID == batchID {
			result = append(result, row)
		}
	}
	return result, nil
}

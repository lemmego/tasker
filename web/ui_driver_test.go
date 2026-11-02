package web

import (
	"context"
	"time"

	"github.com/lemmego/tasker"
)

// stubDriver is a Driver that answers rather than works.
//
// The dashboard tests need a Manager whose stats calls return something: a
// Manager built with no driver panics on the first stats read, which is how
// the first version of the stream test failed — the handler was fine and the
// fixture was nil.
//
// Every method is present because Driver is an interface and a partial
// implementation does not compile. The ones the dashboard does not call
// return zero values rather than panicking, so a test that starts calling
// one gets an empty answer to assert against instead of a stack trace.
type stubDriver struct {
	stats  GlobalStatsFunc
	nodes  []tasker.NodeInfo
	jobs   []*tasker.JobRow
	queues map[tasker.QueueName]*tasker.QueueStats
}

// GlobalStatsFunc lets a test decide what the stream reports, including
// failing — the case the stream has to survive without dropping the
// connection.
type GlobalStatsFunc func(context.Context) (*tasker.GlobalStats, error)

func (d *stubDriver) GlobalStats(ctx context.Context) (*tasker.GlobalStats, error) {
	if d.stats != nil {
		return d.stats(ctx)
	}
	return &tasker.GlobalStats{Status: "running", Processes: 1}, nil
}

// QueryJobs honours the state filter and nothing else.
//
// The state filter is not optional for a fixture: the sidebar derives every
// count from it, so a driver that ignored it reported the same number beside
// all eight panels — which looks like a rendering bug and is a fixture that
// answers the same question eight times.
func (d *stubDriver) QueryJobs(_ context.Context, f tasker.JobFilter) ([]*tasker.JobRow, int64, error) {
	if len(f.States) == 0 {
		return d.jobs, int64(len(d.jobs)), nil
	}
	var out []*tasker.JobRow
	for _, j := range d.jobs {
		for _, want := range f.States {
			if j.State == want {
				out = append(out, j)
				break
			}
		}
	}
	return out, int64(len(out)), nil
}
func (d *stubDriver) ListNodes(context.Context) ([]tasker.NodeInfo, error) { return d.nodes, nil }
func (d *stubDriver) QueueStats(_ context.Context, q tasker.QueueName) (*tasker.QueueStats, error) {
	if d.queues != nil {
		if qs, ok := d.queues[q]; ok {
			return qs, nil
		}
	}
	return &tasker.QueueStats{Queue: q}, nil
}
func (d *stubDriver) JobStats(_ context.Context, kind string) (*tasker.JobTypeStats, error) {
	return &tasker.JobTypeStats{Kind: kind}, nil
}

func (d *stubDriver) Enqueue(context.Context, *tasker.JobRow) error        { return nil }
func (d *stubDriver) EnqueueBatch(context.Context, []*tasker.JobRow) error { return nil }
func (d *stubDriver) Claim(context.Context, tasker.QueueName, tasker.NodeID, int) ([]*tasker.JobRow, error) {
	return nil, nil
}
func (d *stubDriver) Complete(context.Context, tasker.JobID, []byte) error { return nil }
func (d *stubDriver) Fail(context.Context, tasker.JobID, error) error      { return nil }
func (d *stubDriver) ScheduleRetry(context.Context, tasker.JobID, error, time.Time) error {
	return nil
}
func (d *stubDriver) Retry(context.Context, tasker.JobID) (*tasker.JobRow, error)  { return nil, nil }
func (d *stubDriver) RetryBatch(context.Context, []tasker.JobID) error             { return nil }
func (d *stubDriver) Cancel(context.Context, tasker.JobID) (*tasker.JobRow, error) { return nil, nil }
func (d *stubDriver) CancelBatch(context.Context, []tasker.JobID) error            { return nil }
func (d *stubDriver) GetByID(_ context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	for _, j := range d.jobs {
		if j.ID == id {
			return j, nil
		}
	}
	return nil, nil
}
func (d *stubDriver) Ping(context.Context) error { return nil }
func (d *stubDriver) Close() error               { return nil }
func (d *stubDriver) RegisterNode(context.Context, tasker.NodeInfo, time.Duration) error {
	return nil
}
func (d *stubDriver) DeregisterNode(context.Context, tasker.NodeID) error { return nil }
func (d *stubDriver) Heartbeat(context.Context, tasker.NodeID, time.Duration) error {
	return nil
}
func (d *stubDriver) LeaderElection(context.Context, tasker.NodeID, time.Duration) (bool, error) {
	return false, nil
}
func (d *stubDriver) IsLeader(context.Context, tasker.NodeID) (bool, error) { return false, nil }
func (d *stubDriver) ResignLeadership(context.Context, tasker.NodeID) error { return nil }
func (d *stubDriver) AcquireLock(context.Context, string, tasker.NodeID, time.Duration) (bool, error) {
	return false, nil
}
func (d *stubDriver) ReleaseLock(context.Context, string, tasker.NodeID) error { return nil }
func (d *stubDriver) RequeueStale(context.Context, time.Duration) (int64, error) {
	return 0, nil
}
func (d *stubDriver) Prune(context.Context, time.Time, []tasker.State) (int64, error) {
	return 0, nil
}

var _ tasker.Driver = (*stubDriver)(nil)

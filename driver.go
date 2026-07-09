package tasker

import (
	"context"
	"time"
)

type Driver interface {
	Enqueue(ctx context.Context, job *JobRow) error
	EnqueueBatch(ctx context.Context, jobs []*JobRow) error
	Claim(ctx context.Context, queue QueueName, nodeID NodeID, max int) ([]*JobRow, error)
	Complete(ctx context.Context, id JobID, output []byte) error
	Fail(ctx context.Context, id JobID, err error) error
	Retry(ctx context.Context, id JobID) (*JobRow, error)
	RetryBatch(ctx context.Context, ids []JobID) error
	Cancel(ctx context.Context, id JobID) (*JobRow, error)
	CancelBatch(ctx context.Context, ids []JobID) error
	GetByID(ctx context.Context, id JobID) (*JobRow, error)
	QueryJobs(ctx context.Context, filter JobFilter) ([]*JobRow, int64, error)
	QueueStats(ctx context.Context, queue QueueName) (*QueueStats, error)
	GlobalStats(ctx context.Context) (*GlobalStats, error)
	JobStats(ctx context.Context, kind string) (*JobTypeStats, error)
	Ping(ctx context.Context) error
	Close() error
	RegisterNode(ctx context.Context, node NodeInfo, ttl time.Duration) error
	DeregisterNode(ctx context.Context, nodeID NodeID) error
	Heartbeat(ctx context.Context, nodeID NodeID, ttl time.Duration) error
	ListNodes(ctx context.Context) ([]NodeInfo, error)
	LeaderElection(ctx context.Context, nodeID NodeID, ttl time.Duration) (bool, error)
	IsLeader(ctx context.Context, nodeID NodeID) (bool, error)
	ResignLeadership(ctx context.Context, nodeID NodeID) error
	AcquireLock(ctx context.Context, key string, nodeID NodeID, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key string, nodeID NodeID) error
	RequeueStale(ctx context.Context, timeout time.Duration) (int64, error)
	Prune(ctx context.Context, before time.Time, states []State) (int64, error)
}

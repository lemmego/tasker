package tasker

import (
	"time"
)

type JobID uint64

type State string

const (
	StatePending    State = "pending"
	StateScheduled  State = "scheduled"
	StateAvailable  State = "available"
	StateRunning    State = "running"
	StateCompleted  State = "completed"
	StateRetryable  State = "retryable"
	StateFailed     State = "failed"
	StateCancelled  State = "cancelled"
)

func (s State) Valid() bool {
	switch s {
	case StatePending, StateScheduled, StateAvailable, StateRunning,
		StateCompleted, StateRetryable, StateFailed, StateCancelled:
		return true
	}
	return false
}

func (s State) Terminal() bool {
	return s == StateCompleted || s == StateFailed || s == StateCancelled
}

type NodeID string

type QueueName string

type AttemptError struct {
	Attempt   int       `json:"attempt"`
	Error     string    `json:"error"`
	Timestamp time.Time `json:"timestamp"`
	Stack     string    `json:"stack,omitempty"`
}

type JobRow struct {
	ID           JobID              `json:"id"`
	UUID         string             `json:"uuid"`
	Queue        QueueName          `json:"queue"`
	Kind         string             `json:"kind"`
	Payload      []byte             `json:"payload"`
	State        State              `json:"state"`
	Priority     int                `json:"priority"`
	Attempt      int                `json:"attempt"`
	MaxAttempts  int                `json:"max_attempts"`
	AttemptedBy  []NodeID           `json:"attempted_by"`
	AttemptedAt  *time.Time         `json:"attempted_at"`
	Errors       []AttemptError     `json:"errors"`
	Output       []byte             `json:"output,omitempty"`
	Tags         []string           `json:"tags"`
	ScheduledAt  time.Time          `json:"scheduled_at"`
	CreatedAt    time.Time          `json:"created_at"`
	StartedAt    *time.Time         `json:"started_at"`
	CompletedAt  *time.Time         `json:"completed_at"`
	FinalizedAt  *time.Time         `json:"finalized_at"`
	NodeID       NodeID             `json:"node_id,omitempty"`
	BatchID      string             `json:"batch_id,omitempty"`
	Timeout      time.Duration      `json:"timeout,omitempty"`
	Metadata     map[string]string  `json:"metadata,omitempty"`
	UniqueKey    string             `json:"unique_key,omitempty"`
}

type QueueStats struct {
	Queue           QueueName `json:"queue"`
	Available       int64     `json:"available"`
	Running         int64     `json:"running"`
	Completed       int64     `json:"completed"`
	Failed          int64     `json:"failed"`
	Retryable       int64     `json:"retryable"`
	Scheduled       int64     `json:"scheduled"`
	AvgRuntimeMs    float64   `json:"avg_runtime_ms"`
	ThroughputPerMin float64  `json:"throughput_per_min"`
	WaitTimeMs      float64   `json:"wait_time_ms"`
}

type GlobalStats struct {
	Status          string `json:"status"`
	JobsPerMinute   int64  `json:"jobs_per_minute"`
	Processes       int    `json:"processes"`
	FailedJobs      int64  `json:"failed_jobs"`
	RecentJobs      int64  `json:"recent_jobs"`
	PausedMasters   int    `json:"paused_masters"`
}

type JobFilter struct {
	States  []State   `json:"states,omitempty"`
	Queues  []QueueName `json:"queues,omitempty"`
	Kinds   []string  `json:"kinds,omitempty"`
	Tags    []string  `json:"tags,omitempty"`
	Search  string    `json:"search,omitempty"`
	Limit   int       `json:"limit,omitempty"`
	Offset  int       `json:"offset,omitempty"`
	OrderBy string    `json:"order_by,omitempty"`
	Order   string    `json:"order,omitempty"`
}

type JobTypeStats struct {
	Kind            string  `json:"kind"`
	Throughput      float64 `json:"throughput"`
	AvgRuntimeMs    float64 `json:"avg_runtime_ms"`
	FailedCount     int64   `json:"failed_count"`
	TotalCount      int64   `json:"total_count"`
	Samples         int64   `json:"samples"`
}

type NodeInfo struct {
	ID        NodeID    `json:"id"`
	Host      string    `json:"host"`
	Port      int       `json:"port"`
	Queues    []QueueName `json:"queues"`
	Workers   int       `json:"workers"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Version   string    `json:"version"`
}

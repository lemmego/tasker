package tasker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Manager struct {
	mu         sync.RWMutex
	driver     Driver
	config     ManagerConfig
	workers    map[QueueName]*Pool
	done       chan struct{}
}

type ManagerConfig struct {
	DefaultQueue       QueueName
	DefaultMaxAttempts int
	DefaultTimeout     time.Duration
	NodeID             NodeID
}

func DefaultConfig() ManagerConfig {
	return ManagerConfig{
		DefaultQueue:       "default",
		DefaultMaxAttempts: 3,
		DefaultTimeout:     0,
		NodeID:             NodeID(fmt.Sprintf("node-%s", uuid.New().String()[:8])),
	}
}

type ManagerOption func(*Manager)

func WithDriver(d Driver) ManagerOption {
	return func(m *Manager) {
		m.driver = d
	}
}

func WithDefaultQueue(name QueueName) ManagerOption {
	return func(m *Manager) {
		m.config.DefaultQueue = name
	}
}

func WithMaxAttempts(n int) ManagerOption {
	return func(m *Manager) {
		m.config.DefaultMaxAttempts = n
	}
}

func WithNodeID(id NodeID) ManagerOption {
	return func(m *Manager) {
		m.config.NodeID = id
	}
}

func NewManager(opts ...ManagerOption) *Manager {
	m := &Manager{
		config:  DefaultConfig(),
		workers: make(map[QueueName]*Pool),
		done:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

var globalManager *Manager
var globalMu sync.RWMutex

func SetGlobal(m *Manager) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalManager = m
}

func Global() *Manager {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalManager
}

func (m *Manager) Driver() Driver {
	return m.driver
}

func (m *Manager) Config() ManagerConfig {
	return m.config
}

func (m *Manager) Dispatch(ctx context.Context, job Job, opts ...DispatchOpt) (*JobRow, error) {
	row, err := buildJobRow(job, m.config, opts...)
	if err != nil {
		return nil, err
	}
	if err := m.driver.Enqueue(ctx, row); err != nil {
		return nil, err
	}
	return row, nil
}

func (m *Manager) DispatchBatch(ctx context.Context, jobs []Job, opts ...DispatchOpt) ([]*JobRow, error) {
	rows := make([]*JobRow, 0, len(jobs))
	for _, job := range jobs {
		row, err := buildJobRow(job, m.config, opts...)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	if err := m.driver.EnqueueBatch(ctx, rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (m *Manager) Chain(ctx context.Context, jobs []Job, opts ...DispatchOpt) ([]*JobRow, error) {
	rows := make([]*JobRow, 0, len(jobs))
	for i, job := range jobs {
		row, err := buildJobRow(job, m.config, opts...)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			metadata := map[string]string{"parent_id": rows[i-1].UUID}
			if row.Metadata == nil {
				row.Metadata = make(map[string]string)
			}
			for k, v := range metadata {
				row.Metadata[k] = v
			}
			row.State = StatePending
		}
		rows = append(rows, row)
	}
	if err := m.driver.EnqueueBatch(ctx, rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (m *Manager) Retry(ctx context.Context, id JobID) (*JobRow, error) {
	return m.driver.Retry(ctx, id)
}

func (m *Manager) RetryBatch(ctx context.Context, ids []JobID) error {
	return m.driver.RetryBatch(ctx, ids)
}

func (m *Manager) Cancel(ctx context.Context, id JobID) (*JobRow, error) {
	return m.driver.Cancel(ctx, id)
}

func (m *Manager) CancelBatch(ctx context.Context, ids []JobID) error {
	return m.driver.CancelBatch(ctx, ids)
}

func (m *Manager) GetJob(ctx context.Context, id JobID) (*JobRow, error) {
	return m.driver.GetByID(ctx, id)
}

func (m *Manager) QueryJobs(ctx context.Context, filter JobFilter) ([]*JobRow, int64, error) {
	return m.driver.QueryJobs(ctx, filter)
}

func (m *Manager) QueueStats(ctx context.Context, queue QueueName) (*QueueStats, error) {
	return m.driver.QueueStats(ctx, queue)
}

func (m *Manager) GlobalStats(ctx context.Context) (*GlobalStats, error) {
	return m.driver.GlobalStats(ctx)
}

func (m *Manager) JobStats(ctx context.Context, kind string) (*JobTypeStats, error) {
	return m.driver.JobStats(ctx, kind)
}

func (m *Manager) Work(ctx context.Context, queue QueueName, concurrency int) error {
	pool := NewPool(m, queue, concurrency)
	m.mu.Lock()
	m.workers[queue] = pool
	m.mu.Unlock()
	return pool.Start(ctx)
}

func (m *Manager) Stop(ctx context.Context) error {
	close(m.done)
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, pool := range m.workers {
		if err := pool.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

func buildJobRow(job Job, cfg ManagerConfig, opts ...DispatchOpt) (*JobRow, error) {
	payload, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal job payload: %w", err)
	}

	dOpts := &dispatchOptions{}
	for _, opt := range opts {
		opt(dOpts)
	}

	queue := cfg.DefaultQueue
	if dOpts.queue != "" {
		queue = dOpts.queue
	}
	if q, ok := job.(ShouldQueue); ok && dOpts.queue == "" {
		queue = q.Queue()
	}

	maxAttempts := cfg.DefaultMaxAttempts
	if r, ok := job.(ShouldRetry); ok {
		maxAttempts = r.MaxAttempts()
	}

	scheduledAt := time.Now()
	if dOpts.delay > 0 {
		scheduledAt = scheduledAt.Add(dOpts.delay)
	} else if d, ok := job.(ShouldDelay); ok {
		scheduledAt = scheduledAt.Add(d.Delay())
	}

	var tags []string
	if dOpts.tags != nil {
		tags = dOpts.tags
	} else if t, ok := job.(ShouldTag); ok {
		tags = t.Tags()
	}

	state := StateAvailable
	if scheduledAt.After(time.Now()) {
		state = StateScheduled
	}

	var batchID string
	if dOpts.batchID != "" {
		batchID = dOpts.batchID
	} else if b, ok := job.(ShouldBatch); ok {
		batchID = b.BatchID()
	}

	var timeout time.Duration
	if t, ok := job.(ShouldTimeout); ok {
		timeout = t.Timeout()
	}

	return &JobRow{
		UUID:        uuid.New().String(),
		Queue:       queue,
		Kind:        fmt.Sprintf("%T", job),
		Payload:     payload,
		State:       state,
		Priority:    dOpts.priority,
		Attempt:     0,
		MaxAttempts: maxAttempts,
		Tags:        tags,
		ScheduledAt: scheduledAt,
		CreatedAt:   time.Now(),
		BatchID:     batchID,
		Timeout:     timeout,
		Metadata:    dOpts.metadata,
	}, nil
}

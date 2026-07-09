package supervisor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lemmego/tasker"
)

type QueueConfig struct {
	MaxWorkers int
	MinWorkers int
}

type Config struct {
	Queues           map[tasker.QueueName]QueueConfig
	HeartbeatInterval time.Duration
	RequeueInterval   time.Duration
	RequeueTimeout    time.Duration
	PruneInterval     time.Duration
	PruneAfter        time.Duration
	EnableAutoscale   bool
}

func DefaultConfig() Config {
	return Config{
		Queues: map[tasker.QueueName]QueueConfig{
			"default": {
				MaxWorkers: 10,
				MinWorkers: 1,
			},
		},
		HeartbeatInterval: 5 * time.Second,
		RequeueInterval:   30 * time.Second,
		RequeueTimeout:    60 * time.Second,
		PruneInterval:     24 * time.Hour,
		PruneAfter:        7 * 24 * time.Hour,
		EnableAutoscale:   false,
	}
}

type PoolManager interface {
	StartPool(ctx context.Context, queue tasker.QueueName, concurrency int) (*tasker.Pool, error)
	StopPool(ctx context.Context, queue tasker.QueueName) error
	ScalePool(queue tasker.QueueName, target int) error
	StopAll(ctx context.Context) error
	WorkerCount() int
	Pools() map[tasker.QueueName]*tasker.Pool
}

type Supervisor struct {
	mu        sync.Mutex
	manager   *tasker.Manager
	config    Config
	poolMgr   PoolManager
	running   bool
	stopped   chan struct{}
	cancel    context.CancelFunc
}

func New(mgr *tasker.Manager, cfg Config) *Supervisor {
	return &Supervisor{
		manager:  mgr,
		config:   cfg,
		poolMgr:  tasker.NewPoolManager(mgr),
		stopped:  make(chan struct{}),
	}
}

func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	s.running = true
	ctx, s.cancel = context.WithCancel(ctx)

	queues := make([]tasker.QueueName, 0, len(s.config.Queues))
	for queue, qcfg := range s.config.Queues {
		queues = append(queues, queue)
		if _, err := s.poolMgr.StartPool(ctx, queue, qcfg.MaxWorkers); err != nil {
			slog.Error("failed to start pool", "queue", queue, "error", err)
			continue
		}
	}

	nodeInfo := tasker.NodeInfo{
		ID:        s.manager.Config().NodeID,
		Queues:    queues,
		Workers:   s.poolMgr.WorkerCount(),
		Status:    "active",
		StartedAt: time.Now(),
		Version:   "1.0.0",
	}
	if err := s.manager.Driver().RegisterNode(ctx, nodeInfo, s.config.HeartbeatInterval); err != nil {
		slog.Error("failed to register node", "error", err)
	}

	go s.loop(ctx)

	slog.Info("supervisor started",
		"node", s.manager.Config().NodeID,
		"queues", len(s.config.Queues),
	)

	return nil
}

func (s *Supervisor) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	s.cancel()

	slog.Info("supervisor stopping")

	if err := s.poolMgr.StopAll(ctx); err != nil {
		slog.Error("error stopping pools", "error", err)
	}

	if err := s.manager.Driver().DeregisterNode(ctx, s.manager.Config().NodeID); err != nil {
		slog.Error("failed to deregister node", "error", err)
	}

	close(s.stopped)
	slog.Info("supervisor stopped")
	return nil
}

func (s *Supervisor) Wait() {
	<-s.stopped
}

func (s *Supervisor) PauseQueue(queue tasker.QueueName) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.config.Queues[queue]; !ok {
		return nil
	}

	return s.poolMgr.ScalePool(queue, 0)
}

func (s *Supervisor) ResumeQueue(queue tasker.QueueName) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cfg, ok := s.config.Queues[queue]
	if !ok {
		return nil
	}

	return s.poolMgr.ScalePool(queue, cfg.MaxWorkers)
}

func (s *Supervisor) ScaleQueue(queue tasker.QueueName, count int) error {
	return s.poolMgr.ScalePool(queue, count)
}

func (s *Supervisor) WorkerCount() int {
	return s.poolMgr.WorkerCount()
}

func (s *Supervisor) Pools() map[tasker.QueueName]*tasker.Pool {
	return s.poolMgr.Pools()
}

func (s *Supervisor) Queues() []tasker.QueueName {
	s.mu.Lock()
	defer s.mu.Unlock()
	queues := make([]tasker.QueueName, 0, len(s.config.Queues))
	for q := range s.config.Queues {
		queues = append(queues, q)
	}
	return queues
}

func (s *Supervisor) loop(ctx context.Context) {
	heartbeatTick := time.NewTicker(s.config.HeartbeatInterval)
	requeueTick := time.NewTicker(s.config.RequeueInterval)
	pruneTick := time.NewTicker(s.config.PruneInterval)
	autoscaleTick := time.NewTicker(10 * time.Second)

	defer heartbeatTick.Stop()
	defer requeueTick.Stop()
	defer pruneTick.Stop()
	defer autoscaleTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-heartbeatTick.C:
			s.doHeartbeat(ctx)

		case <-requeueTick.C:
			s.doRequeue(ctx)

		case <-pruneTick.C:
			s.doPrune(ctx)

		case <-autoscaleTick.C:
			if s.config.EnableAutoscale {
				s.doAutoscale(ctx)
			}
		}
	}
}

func (s *Supervisor) doHeartbeat(ctx context.Context) {
	driver := s.manager.Driver()
	if err := driver.Heartbeat(ctx, s.manager.Config().NodeID, s.config.HeartbeatInterval); err != nil {
		slog.Error("heartbeat failed", "error", err)
	}
}

func (s *Supervisor) doRequeue(ctx context.Context) {
	driver := s.manager.Driver()
	count, err := driver.RequeueStale(ctx, s.config.RequeueTimeout)
	if err != nil {
		slog.Error("requeue stale jobs failed", "error", err)
	} else if count > 0 {
		slog.Info("requeued stale jobs", "count", count)
	}
}

func (s *Supervisor) doPrune(ctx context.Context) {
	driver := s.manager.Driver()
	before := time.Now().Add(-s.config.PruneAfter)
	states := []tasker.State{tasker.StateCompleted, tasker.StateFailed, tasker.StateCancelled}
	count, err := driver.Prune(ctx, before, states)
	if err != nil {
		slog.Error("prune failed", "error", err)
	} else if count > 0 {
		slog.Info("pruned old jobs", "count", count)
	}
}

func (s *Supervisor) doAutoscale(ctx context.Context) {
	for queue, qcfg := range s.config.Queues {
		stats, err := s.manager.QueueStats(ctx, queue)
		if err != nil {
			continue
		}

		desired := s.computeDesiredWorkers(stats, qcfg)
		current := s.poolMgr.WorkerCount()

		if desired != current && desired >= qcfg.MinWorkers {
			slog.Debug("auto-scaling",
				"queue", queue,
				"current", current,
				"desired", desired,
			)
			if err := s.poolMgr.ScalePool(queue, desired); err != nil {
				slog.Error("auto-scale failed", "queue", queue, "error", err)
			}
		}
	}
}

func (s *Supervisor) computeDesiredWorkers(stats *tasker.QueueStats, cfg QueueConfig) int {
	available := stats.Available + stats.Retryable

	if available == 0 {
		return cfg.MinWorkers
	}

	desired := int(available / 2)
	if desired < cfg.MinWorkers {
		desired = cfg.MinWorkers
	}
	if desired > cfg.MaxWorkers {
		desired = cfg.MaxWorkers
	}

	return desired
}

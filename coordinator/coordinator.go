package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lemmego/tasker"
)

type Config struct {
	NodeID        tasker.NodeID
	Host          string
	Port          int
	Queues        []tasker.QueueName
	WorkerCount   int
	HeartbeatTTL  time.Duration
	LeaderTTL     time.Duration
}

func DefaultConfig() Config {
	return Config{
		HeartbeatTTL: 10 * time.Second,
		LeaderTTL:    15 * time.Second,
	}
}

type Coordinator struct {
	mu      sync.Mutex
	manager *tasker.Manager
	config  Config
	running bool
	cancel  context.CancelFunc
	stopped chan struct{}
}

func New(mgr *tasker.Manager, cfg Config) *Coordinator {
	return &Coordinator{
		manager: mgr,
		config:  cfg,
		stopped: make(chan struct{}),
	}
}

func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return nil
	}

	c.running = true
	ctx, c.cancel = context.WithCancel(ctx)

	nodeInfo := tasker.NodeInfo{
		ID:        c.config.NodeID,
		Host:      c.config.Host,
		Port:      c.config.Port,
		Queues:    c.config.Queues,
		Workers:   c.config.WorkerCount,
		Status:    "active",
		StartedAt: time.Now(),
		Version:   "1.0.0",
	}

	if err := c.manager.Driver().RegisterNode(ctx, nodeInfo, c.config.HeartbeatTTL); err != nil {
		return err
	}

	go c.loop(ctx)

	slog.Info("coordinator started", "node", c.config.NodeID)

	return nil
}

func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	c.running = false
	c.cancel()

	if err := c.manager.Driver().DeregisterNode(ctx, c.config.NodeID); err != nil {
		slog.Error("failed to deregister node", "error", err)
	}

	close(c.stopped)
	return nil
}

func (c *Coordinator) Wait() {
	<-c.stopped
}

func (c *Coordinator) loop(ctx context.Context) {
	heartbeatTick := time.NewTicker(c.config.HeartbeatTTL / 2)
	leaderTick := time.NewTicker(c.config.LeaderTTL)
	defer heartbeatTick.Stop()
	defer leaderTick.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-heartbeatTick.C:
			if err := c.manager.Driver().Heartbeat(ctx, c.config.NodeID, c.config.HeartbeatTTL); err != nil {
				slog.Error("coordinator heartbeat failed", "error", err)
			}

		case <-leaderTick.C:
			c.tryBecomeLeader(ctx)
		}
	}
}

func (c *Coordinator) tryBecomeLeader(ctx context.Context) {
	driver := c.manager.Driver()
	isLeader, err := driver.IsLeader(ctx, c.config.NodeID)
	if err != nil {
		return
	}

	if !isLeader {
		becameLeader, err := driver.LeaderElection(ctx, c.config.NodeID, c.config.LeaderTTL)
		if err != nil {
			return
		}
		if becameLeader {
			slog.Info("became leader", "node", c.config.NodeID)
		}
	}
}

func (c *Coordinator) IsLeader(ctx context.Context) (bool, error) {
	return c.manager.Driver().IsLeader(ctx, c.config.NodeID)
}

func (c *Coordinator) ListNodes(ctx context.Context) ([]tasker.NodeInfo, error) {
	return c.manager.Driver().ListNodes(ctx)
}

func (c *Coordinator) AcquireLock(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return c.manager.Driver().AcquireLock(ctx, key, c.config.NodeID, ttl)
}

func (c *Coordinator) ReleaseLock(ctx context.Context, key string) error {
	return c.manager.Driver().ReleaseLock(ctx, key, c.config.NodeID)
}

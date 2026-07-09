package redisdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/lemmego/tasker"
)

type Config struct {
	Addr        string
	Password    string
	DB          int
	PoolSize    int
	KeyPrefix   string
}

func DefaultConfig() Config {
	return Config{
		Addr:      "localhost:6379",
		KeyPrefix: "tasker:",
	}
}

type Driver struct {
	client *redis.Client
	config Config
}

func NewDriver(cfg Config) (*Driver, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &Driver{
		client: client,
		config: cfg,
	}, nil
}

func (d *Driver) key(suffix string) string {
	return d.config.KeyPrefix + suffix
}

func (d *Driver) Ping(ctx context.Context) error {
	return d.client.Ping(ctx).Err()
}

func (d *Driver) Close() error {
	return d.client.Close()
}

func (d *Driver) Enqueue(ctx context.Context, job *tasker.JobRow) error {
	return fmt.Errorf("redis driver: Enqueue not implemented")
}

func (d *Driver) EnqueueBatch(ctx context.Context, jobs []*tasker.JobRow) error {
	return fmt.Errorf("redis driver: EnqueueBatch not implemented")
}

func (d *Driver) Claim(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	return nil, fmt.Errorf("redis driver: Claim not implemented")
}

func (d *Driver) Complete(ctx context.Context, id tasker.JobID, output []byte) error {
	return fmt.Errorf("redis driver: Complete not implemented")
}

func (d *Driver) Fail(ctx context.Context, id tasker.JobID, err error) error {
	return fmt.Errorf("redis driver: Fail not implemented")
}

func (d *Driver) Retry(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	return nil, fmt.Errorf("redis driver: Retry not implemented")
}

func (d *Driver) RetryBatch(ctx context.Context, ids []tasker.JobID) error {
	return fmt.Errorf("redis driver: RetryBatch not implemented")
}

func (d *Driver) Cancel(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	return nil, fmt.Errorf("redis driver: Cancel not implemented")
}

func (d *Driver) CancelBatch(ctx context.Context, ids []tasker.JobID) error {
	return fmt.Errorf("redis driver: CancelBatch not implemented")
}

func (d *Driver) GetByID(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	return nil, fmt.Errorf("redis driver: GetByID not implemented")
}

func (d *Driver) QueryJobs(ctx context.Context, filter tasker.JobFilter) ([]*tasker.JobRow, int64, error) {
	return nil, 0, fmt.Errorf("redis driver: QueryJobs not implemented")
}

func (d *Driver) QueueStats(ctx context.Context, queue tasker.QueueName) (*tasker.QueueStats, error) {
	return nil, fmt.Errorf("redis driver: QueueStats not implemented")
}

func (d *Driver) GlobalStats(ctx context.Context) (*tasker.GlobalStats, error) {
	return &tasker.GlobalStats{}, nil
}

func (d *Driver) JobStats(ctx context.Context, kind string) (*tasker.JobTypeStats, error) {
	return nil, fmt.Errorf("redis driver: JobStats not implemented")
}

func (d *Driver) RegisterNode(ctx context.Context, node tasker.NodeInfo, ttl time.Duration) error {
	return d.client.HSet(ctx, d.key("nodes:"+string(node.ID)), map[string]interface{}{
		"host":           node.Host,
		"port":           node.Port,
		"status":         node.Status,
		"started_at":     node.StartedAt.Format(time.RFC3339),
		"last_heartbeat": time.Now().Format(time.RFC3339),
		"version":        node.Version,
	}).Err()
}

func (d *Driver) DeregisterNode(ctx context.Context, nodeID tasker.NodeID) error {
	return d.client.Del(ctx, d.key("nodes:"+string(nodeID))).Err()
}

func (d *Driver) Heartbeat(ctx context.Context, nodeID tasker.NodeID, ttl time.Duration) error {
	return d.client.Expire(ctx, d.key("nodes:"+string(nodeID)), ttl).Err()
}

func (d *Driver) ListNodes(ctx context.Context) ([]tasker.NodeInfo, error) {
	return nil, fmt.Errorf("redis driver: ListNodes not implemented")
}

func (d *Driver) LeaderElection(ctx context.Context, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	return d.client.SetNX(ctx, d.key("leader"), string(nodeID), ttl).Result()
}

func (d *Driver) IsLeader(ctx context.Context, nodeID tasker.NodeID) (bool, error) {
	val, err := d.client.Get(ctx, d.key("leader")).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return val == string(nodeID), nil
}

func (d *Driver) ResignLeadership(ctx context.Context, nodeID tasker.NodeID) error {
	return d.client.Del(ctx, d.key("leader")).Err()
}

func (d *Driver) AcquireLock(ctx context.Context, key string, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	return d.client.SetNX(ctx, d.key("lock:"+key), string(nodeID), ttl).Result()
}

func (d *Driver) ReleaseLock(ctx context.Context, key string, nodeID tasker.NodeID) error {
	luaScript := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`
	return d.client.Eval(ctx, luaScript, []string{d.key("lock:" + key)}, string(nodeID)).Err()
}

func (d *Driver) RequeueStale(ctx context.Context, timeout time.Duration) (int64, error) {
	return 0, nil
}

func (d *Driver) Prune(ctx context.Context, before time.Time, states []tasker.State) (int64, error) {
	return 0, nil
}

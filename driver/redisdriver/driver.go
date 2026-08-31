package redisdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/lemmego/tasker"
)

type Config struct {
	Addr      string
	Password  string
	DB        int
	PoolSize  int
	KeyPrefix string
}

func DefaultConfig() Config {
	return Config{Addr: "localhost:6379", KeyPrefix: "tasker:"}
}

type Driver struct {
	client *redis.Client
	config Config
}

func NewDriver(cfg Config) (*Driver, error) {
	client := redis.NewClient(&redis.Options{
		Addr: cfg.Addr, Password: cfg.Password, DB: cfg.DB, PoolSize: cfg.PoolSize,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	return &Driver{client: client, config: cfg}, nil
}

func (d *Driver) key(suffix string) string       { return d.config.KeyPrefix + suffix }
func (d *Driver) Ping(ctx context.Context) error { return d.client.Ping(ctx).Err() }
func (d *Driver) Close() error                   { return d.client.Close() }

var enqueueScript = redis.NewScript(`
local seen = {}
for i = 1, #ARGV, 4 do
  local uuid = ARGV[i]
  if uuid == '' or seen[uuid] or redis.call('HEXISTS', KEYS[2], uuid) == 1 then
    return redis.error_reply('TASKER_DUPLICATE_UUID')
  end
  seen[uuid] = true
end
local result = {}
for i = 1, #ARGV, 4 do
  local id = redis.call('INCR', KEYS[1])
  local job = cjson.decode(ARGV[i + 1])
  job.id = id
  redis.call('HSET', KEYS[2], ARGV[i], tostring(id))
  redis.call('HSET', KEYS[3], tostring(id), cjson.encode(job))
  redis.call('ZADD', KEYS[4], id, tostring(id))
  redis.call('ZADD', KEYS[5], ARGV[i + 2], tostring(id))
  if job.state == 'available' or job.state == 'scheduled' or job.state == 'retryable' then
    redis.call('ZADD', KEYS[6] .. job.queue, ARGV[i + 3], tostring(id))
  end
  result[#result + 1] = id
end
return result
`)

func (d *Driver) Enqueue(ctx context.Context, job *tasker.JobRow) error {
	return d.EnqueueBatch(ctx, []*tasker.JobRow{job})
}

func (d *Driver) EnqueueBatch(ctx context.Context, jobs []*tasker.JobRow) error {
	if len(jobs) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(jobs)*4)
	for _, job := range jobs {
		if job == nil {
			return fmt.Errorf("redis driver: nil job")
		}
		encoded, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("encode job: %w", err)
		}
		args = append(args, job.UUID, encoded, unixMicros(job.CreatedAt), unixMicros(job.ScheduledAt))
	}
	keys := []string{d.key("job:id"), d.key("job:uuids"), d.key("jobs"), d.key("job:ids"), d.key("job:created"), d.key("job:due:")}
	result, err := enqueueScript.Run(ctx, d.client, keys, args...).Result()
	if err != nil {
		if strings.Contains(err.Error(), "TASKER_DUPLICATE_UUID") {
			return tasker.ErrJobAlreadyExists
		}
		return err
	}
	ids, ok := result.([]interface{})
	if !ok || len(ids) != len(jobs) {
		return fmt.Errorf("redis driver: invalid enqueue result")
	}
	for i, raw := range ids {
		id, err := redisInt(raw)
		if err != nil {
			return err
		}
		jobs[i].ID = tasker.JobID(id)
	}
	return nil
}

var claimScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'WITHSCORES')
local candidates = {}
for i = 1, #due, 2 do
  local raw = redis.call('HGET', KEYS[1], due[i])
  if raw then
    local job = cjson.decode(raw)
    if (job.state == 'available' or job.state == 'scheduled' or job.state == 'retryable')
        and (tonumber(job.attempt) or 0) < (tonumber(job.max_attempts) or 0) then
	  candidates[#candidates + 1] = {id=due[i], score=tonumber(due[i + 1]), job=job}
	else
      redis.call('ZREM', KEYS[2], due[i])
    end
  else
    redis.call('ZREM', KEYS[2], due[i])
  end
end
table.sort(candidates, function(a, b)
  local ap = tonumber(a.job.priority) or 0
  local bp = tonumber(b.job.priority) or 0
  if ap ~= bp then return ap > bp end
  if a.score ~= b.score then return a.score < b.score end
  return tonumber(a.id) < tonumber(b.id)
end)
local result = {}
local maximum = tonumber(ARGV[2])
for i = 1, math.min(maximum, #candidates) do
  local item = candidates[i]
  local job = item.job
  job.state = 'running'
  job.attempt = (tonumber(job.attempt) or 0) + 1
  job.attempted_at = ARGV[3]
  if job.started_at == nil or job.started_at == cjson.null then job.started_at = ARGV[3] end
  job.node_id = ARGV[4]
  if job.attempted_by == nil or job.attempted_by == cjson.null then job.attempted_by = {} end
  if #job.attempted_by >= 50 then table.remove(job.attempted_by, 1) end
  job.attempted_by[#job.attempted_by + 1] = ARGV[4]
  local encoded = cjson.encode(job)
  redis.call('HSET', KEYS[1], item.id, encoded)
  redis.call('ZREM', KEYS[2], item.id)
  redis.call('ZADD', KEYS[3], ARGV[1], item.id)
  result[#result + 1] = encoded
end
return result
`)

func (d *Driver) Claim(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	if max <= 0 {
		return []*tasker.JobRow{}, nil
	}
	now := time.Now()
	result, err := claimScript.Run(ctx, d.client,
		[]string{d.key("jobs"), d.key("job:due:" + string(queue)), d.key("job:running")},
		unixMicros(now), max, formatTime(now), string(nodeID)).StringSlice()
	if err != nil {
		return nil, err
	}
	return decodeJobs(result)
}

var finishScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local job = cjson.decode(raw)
if job.state ~= 'running' then return 0 end
job.state = 'completed'
job.output = ARGV[2]
job.completed_at = ARGV[3]
job.finalized_at = ARGV[3]
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(job))
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`)

func (d *Driver) Complete(ctx context.Context, id tasker.JobID, output []byte) error {
	now := time.Now()
	encodedOutput, _ := json.Marshal(output)
	var outputValue interface{}
	_ = json.Unmarshal(encodedOutput, &outputValue)
	// cjson expects the base64 JSON representation produced by Go's []byte encoder.
	outputString, _ := outputValue.(string)
	n, err := finishScript.Run(ctx, d.client, []string{d.key("jobs"), d.key("job:running")}, uint64(id), outputString, formatTime(now)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return tasker.ErrJobNotFound
	}
	return nil
}

var failScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local job = cjson.decode(raw)
if job.state ~= 'running' and job.state ~= 'retryable' then return 0 end
job.state = 'failed'
if job.errors == nil or job.errors == cjson.null then job.errors = {} end
local failure = cjson.decode(ARGV[2])
failure.attempt = tonumber(job.attempt) or 0
job.errors[#job.errors + 1] = failure
job.completed_at = ARGV[3]
job.finalized_at = ARGV[3]
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(job))
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3] .. job.queue, ARGV[1])
return 1
`)

func (d *Driver) Fail(ctx context.Context, id tasker.JobID, jobErr error) error {
	if jobErr == nil {
		jobErr = fmt.Errorf("job failed")
	}
	now := time.Now()
	entry, _ := json.Marshal(tasker.AttemptError{Error: jobErr.Error(), Timestamp: now})
	n, err := failScript.Run(ctx, d.client,
		[]string{d.key("jobs"), d.key("job:running"), d.key("job:due:")},
		uint64(id), entry, formatTime(now)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return tasker.ErrJobNotFound
	}
	return nil
}

var scheduleRetryScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local job = cjson.decode(raw)
if job.state ~= 'running' then return 0 end
job.state = 'retryable'
job.scheduled_at = ARGV[2]
job.node_id = ''
job.attempted_at = cjson.null
job.started_at = cjson.null
job.completed_at = cjson.null
job.finalized_at = cjson.null
job.output = cjson.null
if job.errors == nil or job.errors == cjson.null then job.errors = {} end
local failure = cjson.decode(ARGV[3])
failure.attempt = tonumber(job.attempt) or 0
job.errors[#job.errors + 1] = failure
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(job))
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3] .. job.queue, ARGV[4], ARGV[1])
return 1
`)

func (d *Driver) ScheduleRetry(ctx context.Context, id tasker.JobID, jobErr error, scheduledAt time.Time) error {
	if jobErr == nil {
		jobErr = fmt.Errorf("job failed")
	}
	entry, err := json.Marshal(tasker.AttemptError{Error: jobErr.Error(), Timestamp: time.Now()})
	if err != nil {
		return err
	}
	n, err := scheduleRetryScript.Run(ctx, d.client,
		[]string{d.key("jobs"), d.key("job:running"), d.key("job:due:")},
		uint64(id), formatTime(scheduledAt), entry, unixMicros(scheduledAt)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return tasker.ErrJobNotFound
	}
	return nil
}

var retryScript = redis.NewScript(`
local allowed = {failed=true, completed=true, cancelled=true, retryable=true}
local jobs = {}
for i = 2, #ARGV - 1 do
  local raw = redis.call('HGET', KEYS[1], ARGV[i])
  if not raw then return redis.error_reply('TASKER_JOB_NOT_FOUND') end
  local job = cjson.decode(raw)
  if not allowed[job.state] then return redis.error_reply('TASKER_INVALID_TRANSITION') end
  jobs[#jobs + 1] = {id=ARGV[i], job=job}
end
for _, item in ipairs(jobs) do
  local job = item.job
  job.state = 'available'
  job.scheduled_at = ARGV[1]
  job.node_id = ''
  job.attempt = 0
  job.errors = {}
  job.started_at = cjson.null
  job.attempted_at = cjson.null
  job.completed_at = cjson.null
  job.finalized_at = cjson.null
  job.output = cjson.null
  redis.call('HSET', KEYS[1], item.id, cjson.encode(job))
  redis.call('ZADD', KEYS[2] .. job.queue, ARGV[#ARGV], item.id)
  redis.call('ZREM', KEYS[3], item.id)
end
return #jobs
`)

func (d *Driver) Retry(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	if err := d.retryBatch(ctx, []tasker.JobID{id}); err != nil {
		return nil, err
	}
	return d.GetByID(ctx, id)
}

func (d *Driver) RetryBatch(ctx context.Context, ids []tasker.JobID) error {
	return d.retryBatch(ctx, ids)
}

func (d *Driver) retryBatch(ctx context.Context, ids []tasker.JobID) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now()
	args := []interface{}{formatTime(now)}
	for _, id := range ids {
		args = append(args, uint64(id))
	}
	args = append(args, unixMicros(now))
	err := retryScript.Run(ctx, d.client, []string{d.key("jobs"), d.key("job:due:"), d.key("job:running")}, args...).Err()
	return transitionError(err)
}

var cancelScript = redis.NewScript(`
local allowed = {available=true, pending=true, scheduled=true, retryable=true}
local jobs = {}
for i = 2, #ARGV do
  local raw = redis.call('HGET', KEYS[1], ARGV[i])
  if not raw then return redis.error_reply('TASKER_JOB_NOT_FOUND') end
  local job = cjson.decode(raw)
  if not allowed[job.state] then return redis.error_reply('TASKER_INVALID_TRANSITION') end
  jobs[#jobs + 1] = {id=ARGV[i], job=job}
end
for _, item in ipairs(jobs) do
  item.job.state = 'cancelled'
  item.job.finalized_at = ARGV[1]
  redis.call('HSET', KEYS[1], item.id, cjson.encode(item.job))
  redis.call('ZREM', KEYS[2] .. item.job.queue, item.id)
end
return #jobs
`)

func (d *Driver) Cancel(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	if err := d.cancelBatch(ctx, []tasker.JobID{id}); err != nil {
		return nil, err
	}
	return d.GetByID(ctx, id)
}

func (d *Driver) CancelBatch(ctx context.Context, ids []tasker.JobID) error {
	return d.cancelBatch(ctx, ids)
}

func (d *Driver) cancelBatch(ctx context.Context, ids []tasker.JobID) error {
	if len(ids) == 0 {
		return nil
	}
	args := []interface{}{formatTime(time.Now())}
	for _, id := range ids {
		args = append(args, uint64(id))
	}
	err := cancelScript.Run(ctx, d.client, []string{d.key("jobs"), d.key("job:due:")}, args...).Err()
	return transitionError(err)
}

func (d *Driver) GetByID(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	raw, err := d.client.HGet(ctx, d.key("jobs"), fmt.Sprint(uint64(id))).Result()
	if err == redis.Nil {
		return nil, tasker.ErrJobNotFound
	}
	if err != nil {
		return nil, err
	}
	return decodeJob(raw)
}

func (d *Driver) QueryJobs(ctx context.Context, filter tasker.JobFilter) ([]*tasker.JobRow, int64, error) {
	values, err := d.client.HVals(ctx, d.key("jobs")).Result()
	if err != nil {
		return nil, 0, err
	}
	jobs, err := decodeJobs(values)
	if err != nil {
		return nil, 0, err
	}
	filtered := jobs[:0]
	for _, job := range jobs {
		if matchesFilter(job, filter) {
			filtered = append(filtered, job)
		}
	}
	total := int64(len(filtered))
	sortJobs(filtered, filter.OrderBy, filter.Order)
	limit, offset := filter.Limit, filter.Offset
	if limit <= 0 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(filtered) {
		return []*tasker.JobRow{}, total, nil
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	return filtered[offset:end], total, nil
}

func (d *Driver) QueueStats(ctx context.Context, queue tasker.QueueName) (*tasker.QueueStats, error) {
	jobs, err := d.allJobs(ctx)
	if err != nil {
		return nil, err
	}
	stats := &tasker.QueueStats{Queue: queue}
	now := time.Now()
	var runtimes, waits float64
	var runtimeCount, waitCount int64
	for _, job := range jobs {
		if job.Queue != queue {
			continue
		}
		switch job.State {
		case tasker.StateAvailable:
			stats.Available++
		case tasker.StateRunning:
			stats.Running++
		case tasker.StateCompleted:
			stats.Completed++
		case tasker.StateFailed:
			stats.Failed++
		case tasker.StateRetryable:
			stats.Retryable++
		case tasker.StateScheduled, tasker.StatePending:
			stats.Scheduled++
		}
		if job.StartedAt != nil {
			waits += float64(job.StartedAt.Sub(job.CreatedAt).Milliseconds())
			waitCount++
		}
		if job.StartedAt != nil && job.CompletedAt != nil {
			runtimes += float64(job.CompletedAt.Sub(*job.StartedAt).Milliseconds())
			runtimeCount++
		}
		if job.CreatedAt.After(now.Add(-time.Minute)) {
			stats.ThroughputPerMin++
		}
	}
	if runtimeCount > 0 {
		stats.AvgRuntimeMs = runtimes / float64(runtimeCount)
	}
	if waitCount > 0 {
		stats.WaitTimeMs = waits / float64(waitCount)
	}
	return stats, nil
}

func (d *Driver) GlobalStats(ctx context.Context) (*tasker.GlobalStats, error) {
	jobs, err := d.allJobs(ctx)
	if err != nil {
		return nil, err
	}
	stats := &tasker.GlobalStats{Status: "running"}
	now := time.Now()
	processes := map[tasker.NodeID]struct{}{}
	for _, job := range jobs {
		if job.CreatedAt.After(now.Add(-time.Hour)) {
			stats.RecentJobs++
		}
		if job.State == tasker.StateFailed {
			stats.FailedJobs++
		}
		if job.FinalizedAt != nil && job.FinalizedAt.After(now.Add(-time.Minute)) && (job.State == tasker.StateCompleted || job.State == tasker.StateFailed) {
			stats.JobsPerMinute++
		}
		if job.State == tasker.StateRunning && job.NodeID != "" {
			processes[job.NodeID] = struct{}{}
		}
	}
	stats.Processes = len(processes)
	return stats, nil
}

func (d *Driver) JobStats(ctx context.Context, kind string) (*tasker.JobTypeStats, error) {
	jobs, err := d.allJobs(ctx)
	if err != nil {
		return nil, err
	}
	stats := &tasker.JobTypeStats{Kind: kind}
	now := time.Now()
	var runtime float64
	for _, job := range jobs {
		if job.Kind != kind {
			continue
		}
		stats.TotalCount++
		if job.State == tasker.StateFailed {
			stats.FailedCount++
		}
		if job.CreatedAt.After(now.Add(-time.Minute)) {
			stats.Throughput++
		}
		if job.StartedAt != nil && job.CompletedAt != nil {
			runtime += float64(job.CompletedAt.Sub(*job.StartedAt).Milliseconds())
			stats.Samples++
		}
	}
	if stats.Samples > 0 {
		stats.AvgRuntimeMs = runtime / float64(stats.Samples)
	}
	return stats, nil
}

var registerNodeScript = redis.NewScript(`
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
redis.call('SADD', KEYS[2], ARGV[3])
return 1
`)

func (d *Driver) RegisterNode(ctx context.Context, node tasker.NodeInfo, ttl time.Duration) error {
	node.LastHeartbeat = time.Now()
	raw, err := json.Marshal(node)
	if err != nil {
		return err
	}
	return registerNodeScript.Run(ctx, d.client,
		[]string{d.key("node:" + string(node.ID)), d.key("nodes")}, raw, ttlMillis(ttl), string(node.ID)).Err()
}

func (d *Driver) DeregisterNode(ctx context.Context, nodeID tasker.NodeID) error {
	pipe := d.client.TxPipeline()
	pipe.Del(ctx, d.key("node:"+string(nodeID)))
	pipe.SRem(ctx, d.key("nodes"), string(nodeID))
	_, err := pipe.Exec(ctx)
	return err
}

var heartbeatScript = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local node = cjson.decode(raw)
node.last_heartbeat = ARGV[1]
redis.call('SET', KEYS[1], cjson.encode(node), 'PX', ARGV[2])
return 1
`)

func (d *Driver) Heartbeat(ctx context.Context, nodeID tasker.NodeID, ttl time.Duration) error {
	n, err := heartbeatScript.Run(ctx, d.client, []string{d.key("node:" + string(nodeID))}, formatTime(time.Now()), ttlMillis(ttl)).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return tasker.ErrNodeNotFound
	}
	return nil
}

func (d *Driver) ListNodes(ctx context.Context) ([]tasker.NodeInfo, error) {
	ids, err := d.client.SMembers(ctx, d.key("nodes")).Result()
	if err != nil {
		return nil, err
	}
	nodes := make([]tasker.NodeInfo, 0, len(ids))
	stale := make([]interface{}, 0)
	for _, id := range ids {
		raw, err := d.client.Get(ctx, d.key("node:"+id)).Result()
		if err == redis.Nil {
			stale = append(stale, id)
			continue
		}
		if err != nil {
			return nil, err
		}
		var node tasker.NodeInfo
		if err := json.Unmarshal([]byte(raw), &node); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	if len(stale) > 0 {
		_ = d.client.SRem(ctx, d.key("nodes"), stale...).Err()
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].StartedAt.Before(nodes[j].StartedAt) })
	return nodes, nil
}

var renewScript = redis.NewScript(`
local owner = redis.call('GET', KEYS[1])
if not owner then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
if owner == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`)

var ownerDeleteScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

func (d *Driver) LeaderElection(ctx context.Context, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	n, err := renewScript.Run(ctx, d.client, []string{d.key("leader")}, string(nodeID), ttlMillis(ttl)).Int()
	return n == 1, err
}

func (d *Driver) IsLeader(ctx context.Context, nodeID tasker.NodeID) (bool, error) {
	value, err := d.client.Get(ctx, d.key("leader")).Result()
	if err == redis.Nil {
		return false, nil
	}
	return value == string(nodeID), err
}

func (d *Driver) ResignLeadership(ctx context.Context, nodeID tasker.NodeID) error {
	return ownerDeleteScript.Run(ctx, d.client, []string{d.key("leader")}, string(nodeID)).Err()
}

func (d *Driver) AcquireLock(ctx context.Context, key string, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	n, err := renewScript.Run(ctx, d.client, []string{d.key("lock:" + key)}, string(nodeID), ttlMillis(ttl)).Int()
	return n == 1, err
}

func (d *Driver) ReleaseLock(ctx context.Context, key string, nodeID tasker.NodeID) error {
	return ownerDeleteScript.Run(ctx, d.client, []string{d.key("lock:" + key)}, string(nodeID)).Err()
}

var staleScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
local count = 0
for _, id in ipairs(ids) do
  local raw = redis.call('HGET', KEYS[1], id)
  if raw then
    local job = cjson.decode(raw)
    if job.state == 'running' then
      job.state = 'available'
      job.node_id = ''
      job.scheduled_at = ARGV[2]
      redis.call('HSET', KEYS[1], id, cjson.encode(job))
      redis.call('ZADD', KEYS[3] .. job.queue, ARGV[3], id)
      count = count + 1
    end
  end
  redis.call('ZREM', KEYS[2], id)
end
return count
`)

func (d *Driver) RequeueStale(ctx context.Context, timeout time.Duration) (int64, error) {
	now := time.Now()
	return staleScript.Run(ctx, d.client,
		[]string{d.key("jobs"), d.key("job:running"), d.key("job:due:")},
		unixMicros(now.Add(-timeout)), formatTime(now), unixMicros(now)).Int64()
}

var pruneScript = redis.NewScript(`
local allowed = {}
for i = 2, #ARGV do allowed[ARGV[i]] = true end
local ids = redis.call('ZRANGEBYSCORE', KEYS[4], '-inf', '(' .. ARGV[1])
local count = 0
for _, id in ipairs(ids) do
  local raw = redis.call('HGET', KEYS[1], id)
  if raw then
    local job = cjson.decode(raw)
    if allowed[job.state] then
      redis.call('HDEL', KEYS[1], id)
      redis.call('HDEL', KEYS[2], job.uuid)
      redis.call('ZREM', KEYS[3], id)
      redis.call('ZREM', KEYS[4], id)
      redis.call('ZREM', KEYS[5], id)
      redis.call('ZREM', KEYS[6] .. job.queue, id)
      count = count + 1
    end
  else
    redis.call('ZREM', KEYS[3], id)
    redis.call('ZREM', KEYS[4], id)
  end
end
return count
`)

func (d *Driver) Prune(ctx context.Context, before time.Time, states []tasker.State) (int64, error) {
	if len(states) == 0 {
		return 0, nil
	}
	args := []interface{}{unixMicros(before)}
	for _, state := range states {
		args = append(args, string(state))
	}
	return pruneScript.Run(ctx, d.client, []string{
		d.key("jobs"), d.key("job:uuids"), d.key("job:ids"), d.key("job:created"), d.key("job:running"), d.key("job:due:"),
	}, args...).Int64()
}

func (d *Driver) allJobs(ctx context.Context) ([]*tasker.JobRow, error) {
	values, err := d.client.HVals(ctx, d.key("jobs")).Result()
	if err != nil {
		return nil, err
	}
	return decodeJobs(values)
}

func decodeJob(raw string) (*tasker.JobRow, error) {
	var job tasker.JobRow
	if err := json.Unmarshal([]byte(raw), &job); err != nil {
		return nil, fmt.Errorf("decode job: %w", err)
	}
	return &job, nil
}

func decodeJobs(values []string) ([]*tasker.JobRow, error) {
	jobs := make([]*tasker.JobRow, 0, len(values))
	for _, value := range values {
		job, err := decodeJob(value)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func matchesFilter(job *tasker.JobRow, filter tasker.JobFilter) bool {
	if len(filter.States) > 0 && !contains(filter.States, job.State) {
		return false
	}
	if len(filter.Queues) > 0 && !contains(filter.Queues, job.Queue) {
		return false
	}
	if len(filter.Kinds) > 0 && !contains(filter.Kinds, job.Kind) {
		return false
	}
	for _, tag := range filter.Tags {
		if !contains(job.Tags, tag) {
			return false
		}
	}
	search := strings.ToLower(filter.Search)
	return search == "" || strings.Contains(strings.ToLower(job.Kind), search) || strings.Contains(strings.ToLower(job.UUID), search)
}

func sortJobs(jobs []*tasker.JobRow, orderBy, order string) {
	allowed := map[string]func(*tasker.JobRow, *tasker.JobRow) int{
		"id":           func(a, b *tasker.JobRow) int { return compare(a.ID, b.ID) },
		"uuid":         func(a, b *tasker.JobRow) int { return strings.Compare(a.UUID, b.UUID) },
		"queue":        func(a, b *tasker.JobRow) int { return strings.Compare(string(a.Queue), string(b.Queue)) },
		"kind":         func(a, b *tasker.JobRow) int { return strings.Compare(a.Kind, b.Kind) },
		"state":        func(a, b *tasker.JobRow) int { return strings.Compare(string(a.State), string(b.State)) },
		"priority":     func(a, b *tasker.JobRow) int { return compare(a.Priority, b.Priority) },
		"attempt":      func(a, b *tasker.JobRow) int { return compare(a.Attempt, b.Attempt) },
		"scheduled_at": func(a, b *tasker.JobRow) int { return a.ScheduledAt.Compare(b.ScheduledAt) },
		"created_at":   func(a, b *tasker.JobRow) int { return a.CreatedAt.Compare(b.CreatedAt) },
	}
	cmp, ok := allowed[orderBy]
	if !ok {
		cmp = allowed["created_at"]
	}
	desc := strings.ToLower(order) != "asc"
	sort.SliceStable(jobs, func(i, j int) bool {
		result := cmp(jobs[i], jobs[j])
		if result == 0 {
			result = compare(jobs[i].ID, jobs[j].ID)
		}
		if desc {
			return result > 0
		}
		return result < 0
	})
}

func transitionError(err error) error {
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "TASKER_JOB_NOT_FOUND") {
		return tasker.ErrJobNotFound
	}
	if strings.Contains(err.Error(), "TASKER_INVALID_TRANSITION") {
		return tasker.ErrInvalidTransition
	}
	return err
}

func contains[T comparable](values []T, value T) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func compare[T ~int | ~uint64](a, b T) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func redisInt(value interface{}) (int64, error) {
	switch value := value.(type) {
	case int64:
		return value, nil
	case string:
		var result int64
		_, err := fmt.Sscan(value, &result)
		return result, err
	default:
		return 0, fmt.Errorf("redis driver: unexpected integer %T", value)
	}
}

func unixMicros(value time.Time) int64  { return value.UnixNano() / int64(time.Microsecond) }
func formatTime(value time.Time) string { return value.Format(time.RFC3339Nano) }

func ttlMillis(ttl time.Duration) int64 {
	value := ttl.Milliseconds()
	if value < 1 {
		return 1
	}
	return value
}

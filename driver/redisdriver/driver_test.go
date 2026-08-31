package redisdriver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/lemmego/tasker"
)

var automaticRetryRuns atomic.Int32
var automaticRetryHooks atomic.Int32

type automaticRetryJob struct{}

func (*automaticRetryJob) Handle(context.Context) error {
	if automaticRetryRuns.Add(1) == 1 {
		return errors.New("temporary")
	}
	return nil
}

func (*automaticRetryJob) MaxAttempts() int { return 2 }

func (*automaticRetryJob) RetryBackoff() tasker.BackoffConfig {
	return tasker.BackoffConfig{Strategy: tasker.BackoffFixed, Base: 10 * time.Millisecond, MaxDelay: time.Second}
}

func (*automaticRetryJob) BeforeRetry(context.Context, int, error) error {
	automaticRetryHooks.Add(1)
	return nil
}

func testDriver(t *testing.T) (*Driver, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	driver, err := NewDriver(Config{Addr: server.Addr(), KeyPrefix: "test:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })
	return driver, server
}

func testJob(uuid string, scheduled time.Time) *tasker.JobRow {
	return &tasker.JobRow{
		UUID: uuid, Queue: "default", Kind: "mail.send", Payload: []byte{0, 1, 2, 255},
		State: tasker.StateAvailable, Priority: 2, Attempt: 0, MaxAttempts: 4,
		Tags: []string{"mail", "important"}, ScheduledAt: scheduled,
		CreatedAt: scheduled.Add(-time.Minute), BatchID: "batch-1", Timeout: 3 * time.Second,
		Metadata: map[string]string{"tenant": "42"}, UniqueKey: "mail:42",
	}
}

func TestEnqueueRoundTripAndDuplicateBatchRollback(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	want := testJob("full-row", now.Add(-time.Second))
	want.AttemptedBy = []tasker.NodeID{"old-node"}
	attempted := now.Add(-time.Minute)
	want.AttemptedAt = &attempted
	want.Errors = []tasker.AttemptError{{Attempt: 1, Error: "old error", Timestamp: attempted, Stack: "stack"}}
	want.Output = []byte("old output")
	want.NodeID = "old-node"
	want.StartedAt, want.CompletedAt, want.FinalizedAt = &attempted, &attempted, &attempted

	if err := d.Enqueue(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := d.GetByID(ctx, want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch\n got: %#v\nwant: %#v", got, want)
	}

	batch := []*tasker.JobRow{testJob("new-uuid", now), testJob("full-row", now)}
	if err := d.EnqueueBatch(ctx, batch); !errors.Is(err, tasker.ErrJobAlreadyExists) {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if batch[0].ID != 0 || batch[1].ID != 0 {
		t.Fatal("failed batch assigned IDs")
	}
	jobs, total, err := d.QueryJobs(ctx, tasker.JobFilter{})
	if err != nil || total != 1 || len(jobs) != 1 {
		t.Fatalf("batch was not rolled back: jobs=%d total=%d err=%v", len(jobs), total, err)
	}

	withinBatch := []*tasker.JobRow{testJob("same", now), testJob("same", now)}
	if err := d.EnqueueBatch(ctx, withinBatch); !errors.Is(err, tasker.ErrJobAlreadyExists) {
		t.Fatalf("expected within-batch duplicate error, got %v", err)
	}
}

func TestClaimOrderingDelayedAndConcurrent(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	now := time.Now()
	jobs := []*tasker.JobRow{
		testJob("low", now.Add(-3*time.Second)),
		testJob("high-later", now.Add(-time.Second)),
		testJob("high-earlier", now.Add(-2*time.Second)),
		testJob("delayed", now.Add(time.Hour)),
	}
	jobs[0].Priority = 1
	jobs[1].Priority, jobs[2].Priority, jobs[3].Priority = 9, 9, 100
	jobs[1].State, jobs[2].State, jobs[3].State = tasker.StateScheduled, tasker.StateRetryable, tasker.StateScheduled
	if err := d.EnqueueBatch(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(ctx, "default", "node-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{claimed[0].UUID, claimed[1].UUID, claimed[2].UUID}; !reflect.DeepEqual(got, []string{"high-earlier", "high-later", "low"}) {
		t.Fatalf("unexpected claim order: %v", got)
	}
	if len(claimed) != 3 {
		t.Fatalf("claimed delayed job: %d jobs", len(claimed))
	}
	for _, job := range claimed {
		if job.State != tasker.StateRunning || job.Attempt != 1 || job.NodeID != "node-a" || len(job.AttemptedBy) != 1 {
			t.Fatalf("invalid claimed job: %#v", job)
		}
	}

	concurrent := make([]*tasker.JobRow, 20)
	for i := range concurrent {
		concurrent[i] = testJob(fmt.Sprintf("concurrent-%d", i), now.Add(-time.Second))
	}
	if err := d.EnqueueBatch(ctx, concurrent); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[tasker.JobID]bool{}
	var claimErr error
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rows, err := d.Claim(ctx, "default", tasker.NodeID(fmt.Sprintf("node-%d", i)), 5)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				claimErr = err
				return
			}
			for _, row := range rows {
				if seen[row.ID] {
					t.Errorf("job %d claimed twice", row.ID)
				}
				seen[row.ID] = true
			}
		}(i)
	}
	wg.Wait()
	if claimErr != nil || len(seen) != 20 {
		t.Fatalf("concurrent claims: unique=%d err=%v", len(seen), claimErr)
	}
}

func TestClaimExcludesExhaustedJob(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	job := testJob("exhausted", time.Now().Add(-time.Second))
	job.Attempt = job.MaxAttempts
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	claimed, err := d.Claim(ctx, job.Queue, "worker", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed exhausted job: %#v", claimed[0])
	}
}

func TestScheduleRetryUpdatesExistingJobAndReclaimsWhenDue(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	job := testJob("automatic-retry", time.Now().Add(-time.Second))
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(ctx, job.Queue, "worker-a", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("initial claim: jobs=%d err=%v", len(claimed), err)
	}

	due := time.Now().Add(100 * time.Millisecond)
	if err := d.ScheduleRetry(ctx, job.ID, errors.New("temporary"), due); err != nil {
		t.Fatal(err)
	}
	retried, err := d.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.ID != job.ID || retried.UUID != job.UUID || retried.State != tasker.StateRetryable || retried.Attempt != 1 {
		t.Fatalf("retry changed job identity or attempt: %#v", retried)
	}
	if retried.NodeID != "" || retried.AttemptedAt != nil || retried.StartedAt != nil || len(retried.Errors) != 1 || retried.Errors[0].Attempt != 1 {
		t.Fatalf("retry did not clear ownership or append error: %#v", retried)
	}
	_, total, err := d.QueryJobs(ctx, tasker.JobFilter{})
	if err != nil || total != 1 {
		t.Fatalf("retry inserted another job: total=%d err=%v", total, err)
	}
	if claimed, err := d.Claim(ctx, job.Queue, "worker-b", 1); err != nil || len(claimed) != 0 {
		t.Fatalf("retry claimed before due: jobs=%d err=%v", len(claimed), err)
	}
	time.Sleep(110 * time.Millisecond)
	claimed, err = d.Claim(ctx, job.Queue, "worker-b", 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != job.ID || claimed[0].Attempt != 2 {
		t.Fatalf("retry not reclaimed: jobs=%#v err=%v", claimed, err)
	}
}

func TestWorkerAutomaticRetryUpdatesOneJob(t *testing.T) {
	d, _ := testDriver(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	automaticRetryRuns.Store(0)
	automaticRetryHooks.Store(0)
	kind := fmt.Sprintf("%T", &automaticRetryJob{})
	tasker.RegisterJob(kind, func() tasker.Job { return &automaticRetryJob{} })
	mgr := tasker.NewManager(tasker.WithDriver(d), tasker.WithNodeID("worker-node"))
	row, err := mgr.Dispatch(ctx, &automaticRetryJob{})
	if err != nil {
		t.Fatal(err)
	}
	pool := tasker.NewPool(mgr, "default", 1)
	if err := pool.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
		defer stopCancel()
		_ = pool.Stop(stopCtx)
	})

	deadline := time.Now().Add(3 * time.Second)
	var got *tasker.JobRow
	for time.Now().Before(deadline) {
		got, err = d.GetByID(ctx, row.ID)
		if err == nil && got.State == tasker.StateCompleted {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got == nil || got.State != tasker.StateCompleted {
		t.Fatalf("job did not complete after retry: job=%#v err=%v", got, err)
	}
	if got.ID != row.ID || got.UUID != row.UUID || got.Attempt != 2 || len(got.Errors) != 1 {
		t.Fatalf("automatic retry changed identity or history: %#v", got)
	}
	_, total, err := d.QueryJobs(ctx, tasker.JobFilter{})
	if err != nil || total != 1 {
		t.Fatalf("automatic retry inserted another job: total=%d err=%v", total, err)
	}
	if automaticRetryHooks.Load() != 1 {
		t.Fatalf("BeforeRetry called %d times", automaticRetryHooks.Load())
	}
}

func TestTransitionsAndAtomicBatches(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	now := time.Now().Add(-time.Second)
	jobs := []*tasker.JobRow{testJob("complete", now), testJob("fail", now), testJob("cancel", now)}
	if err := d.EnqueueBatch(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(ctx, "default", "worker", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Complete(ctx, claimed[0].ID, []byte{0, 255, 1}); err != nil {
		t.Fatal(err)
	}
	if err := d.Fail(ctx, claimed[1].ID, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	completed, _ := d.GetByID(ctx, claimed[0].ID)
	failed, _ := d.GetByID(ctx, claimed[1].ID)
	if completed.State != tasker.StateCompleted || !reflect.DeepEqual(completed.Output, []byte{0, 255, 1}) || completed.FinalizedAt == nil {
		t.Fatalf("bad completed row: %#v", completed)
	}
	if failed.State != tasker.StateFailed || len(failed.Errors) != 1 || failed.Errors[0].Error != "boom" {
		t.Fatalf("bad failed row: %#v", failed)
	}

	cancelled, err := d.Cancel(ctx, jobs[2].ID)
	if err != nil || cancelled.State != tasker.StateCancelled {
		t.Fatalf("cancel: row=%#v err=%v", cancelled, err)
	}
	if err := d.RetryBatch(ctx, []tasker.JobID{completed.ID, failed.ID, cancelled.ID}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []tasker.JobID{completed.ID, failed.ID, cancelled.ID} {
		row, _ := d.GetByID(ctx, id)
		if row.State != tasker.StateAvailable || row.Attempt != 0 || row.Output != nil || len(row.Errors) != 0 || row.NodeID != "" {
			t.Fatalf("bad retried row: %#v", row)
		}
	}

	running, err := d.Claim(ctx, "default", "worker", 1)
	if err != nil || len(running) != 1 {
		t.Fatalf("claim after retry: %v %v", running, err)
	}
	validID := completed.ID
	if validID == running[0].ID {
		validID = failed.ID
	}
	if err := d.CancelBatch(ctx, []tasker.JobID{running[0].ID, validID}); !errors.Is(err, tasker.ErrInvalidTransition) {
		t.Fatalf("expected atomic transition rejection, got %v", err)
	}
	row, _ := d.GetByID(ctx, validID)
	if row.State != tasker.StateAvailable {
		t.Fatal("valid member of rejected batch was changed")
	}
}

func TestQueryAndStats(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	now := time.Now()
	jobs := []*tasker.JobRow{
		testJob("alpha-1", now.Add(-time.Minute)),
		testJob("alpha-2", now.Add(-time.Minute)),
		testJob("beta", now.Add(-time.Minute)),
	}
	jobs[0].Kind, jobs[1].Kind, jobs[2].Kind = "alpha", "alpha", "beta"
	jobs[0].Priority, jobs[1].Priority = 1, 10
	jobs[2].Queue, jobs[2].State = "other", tasker.StateFailed
	if err := d.EnqueueBatch(ctx, jobs); err != nil {
		t.Fatal(err)
	}
	rows, total, err := d.QueryJobs(ctx, tasker.JobFilter{Kinds: []string{"alpha"}, Tags: []string{"important"}, Search: "ALPHA", OrderBy: "priority", Order: "asc", Limit: 1})
	if err != nil || total != 2 || len(rows) != 1 || rows[0].Priority != 1 {
		t.Fatalf("query result rows=%v total=%d err=%v", rows, total, err)
	}
	// An unknown order field must fall back to a fixed safe field.
	if _, _, err := d.QueryJobs(ctx, tasker.JobFilter{OrderBy: "created_at; FLUSHALL"}); err != nil {
		t.Fatal(err)
	}
	queueStats, err := d.QueueStats(ctx, "default")
	if err != nil || queueStats.Available != 2 {
		t.Fatalf("queue stats: %#v err=%v", queueStats, err)
	}
	global, _ := d.GlobalStats(ctx)
	if global.Status != "running" || global.FailedJobs != 1 {
		t.Fatalf("global stats: %#v", global)
	}
	kind, _ := d.JobStats(ctx, "alpha")
	if kind.TotalCount != 2 {
		t.Fatalf("job stats: %#v", kind)
	}
}

func TestNodeTTLLeaderAndLockOwnership(t *testing.T) {
	d, server := testDriver(t)
	ctx := context.Background()
	node := tasker.NodeInfo{ID: "node-a", Host: "host", Port: 123, Queues: []tasker.QueueName{"default"}, Workers: 2, Status: "active", StartedAt: time.Now(), Version: "v1"}
	if err := d.RegisterNode(ctx, node, time.Second); err != nil {
		t.Fatal(err)
	}
	server.FastForward(700 * time.Millisecond)
	if err := d.Heartbeat(ctx, node.ID, time.Second); err != nil {
		t.Fatal(err)
	}
	server.FastForward(700 * time.Millisecond)
	nodes, err := d.ListNodes(ctx)
	if err != nil || len(nodes) != 1 || !nodes[0].LastHeartbeat.After(node.StartedAt) {
		t.Fatalf("nodes after heartbeat: %#v err=%v", nodes, err)
	}
	server.FastForward(time.Second)
	nodes, _ = d.ListNodes(ctx)
	if len(nodes) != 0 {
		t.Fatal("expired node was listed")
	}

	won, _ := d.LeaderElection(ctx, "node-a", time.Second)
	if !won {
		t.Fatal("initial leader lost")
	}
	server.FastForward(700 * time.Millisecond)
	won, _ = d.LeaderElection(ctx, "node-a", time.Second)
	if !won {
		t.Fatal("owner could not renew leadership")
	}
	_ = d.ResignLeadership(ctx, "node-b")
	if leader, _ := d.IsLeader(ctx, "node-a"); !leader {
		t.Fatal("non-owner resigned leader")
	}
	server.FastForward(700 * time.Millisecond)
	if won, _ := d.LeaderElection(ctx, "node-b", time.Second); won {
		t.Fatal("renewed leadership expired early")
	}
	_ = d.ResignLeadership(ctx, "node-a")
	if won, _ := d.LeaderElection(ctx, "node-b", time.Second); !won {
		t.Fatal("leadership unavailable after owner resignation")
	}

	acquired, _ := d.AcquireLock(ctx, "resource", "node-a", time.Second)
	if !acquired {
		t.Fatal("lock not acquired")
	}
	_ = d.ReleaseLock(ctx, "resource", "node-b")
	if acquired, _ := d.AcquireLock(ctx, "resource", "node-b", time.Second); acquired {
		t.Fatal("non-owner released lock")
	}
	if acquired, _ := d.AcquireLock(ctx, "resource", "node-a", time.Second); !acquired {
		t.Fatal("owner could not renew lock")
	}
}

func TestNodeLeaseSurvivesHeartbeatJitterAndMissingHeartbeatFails(t *testing.T) {
	d, server := testDriver(t)
	ctx := context.Background()
	interval := time.Second
	node := tasker.NodeInfo{ID: "jitter-node", StartedAt: time.Now()}
	if err := d.RegisterNode(ctx, node, 3*interval); err != nil {
		t.Fatal(err)
	}
	server.FastForward(interval + interval/2)
	if err := d.Heartbeat(ctx, node.ID, 3*interval); err != nil {
		t.Fatalf("heartbeat after scheduling jitter: %v", err)
	}
	server.FastForward(interval + interval/2)
	nodes, err := d.ListNodes(ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("live node expired during jitter: nodes=%#v err=%v", nodes, err)
	}
	if err := d.Heartbeat(ctx, "missing-node", 3*interval); !errors.Is(err, tasker.ErrNodeNotFound) {
		t.Fatalf("missing node heartbeat error = %v", err)
	}
}

func TestRequeueStaleAndPrune(t *testing.T) {
	d, _ := testDriver(t)
	ctx := context.Background()
	now := time.Now()
	stale := testJob("stale", now.Add(-time.Second))
	if err := d.Enqueue(ctx, stale); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(ctx, "default", "dead-node", 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != stale.ID {
		t.Fatalf("claim stale candidate: %v %v", claimed, err)
	}

	oldCompleted := testJob("old-completed", now.Add(-2*time.Hour))
	oldCompleted.State = tasker.StateCompleted
	oldAvailable := testJob("old-available", now.Add(-2*time.Hour))
	if err := d.EnqueueBatch(ctx, []*tasker.JobRow{oldCompleted, oldAvailable}); err != nil {
		t.Fatal(err)
	}
	count, err := d.RequeueStale(ctx, -time.Second)
	if err != nil || count != 1 {
		t.Fatalf("requeue stale count=%d err=%v", count, err)
	}
	requeued, _ := d.GetByID(ctx, stale.ID)
	if requeued.State != tasker.StateAvailable || requeued.NodeID != "" {
		t.Fatalf("bad stale row: %#v", requeued)
	}

	count, err = d.Prune(ctx, now.Add(-time.Hour), []tasker.State{tasker.StateCompleted})
	if err != nil || count != 1 {
		t.Fatalf("prune count=%d err=%v", count, err)
	}
	if _, err := d.GetByID(ctx, oldCompleted.ID); !errors.Is(err, tasker.ErrJobNotFound) {
		t.Fatalf("pruned job still exists: %v", err)
	}
	if _, err := d.GetByID(ctx, oldAvailable.ID); err != nil {
		t.Fatalf("wrong-state job pruned: %v", err)
	}
	// UUID ownership is removed with the record, allowing deliberate reuse after prune.
	reused := testJob("old-completed", now)
	if err := d.Enqueue(ctx, reused); err != nil {
		t.Fatalf("could not reuse pruned UUID: %v", err)
	}
}

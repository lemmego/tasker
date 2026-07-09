package sqldriver

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/lemmego/tasker"
)

func pastTime() time.Time {
	return time.Now().Add(-time.Hour)
}

func setupTestDB(t *testing.T) *Driver {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Create tables
	schema := `
	CREATE TABLE IF NOT EXISTS tasker_jobs (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		uuid        TEXT NOT NULL UNIQUE,
		queue       TEXT NOT NULL DEFAULT 'default',
		kind        TEXT NOT NULL,
		payload     BLOB NOT NULL,
		state       TEXT NOT NULL DEFAULT 'available',
		priority    INTEGER NOT NULL DEFAULT 0,
		attempt     INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		attempted_by TEXT NOT NULL DEFAULT '{}',
		attempted_at TIMESTAMP,
		errors      BLOB NOT NULL DEFAULT '[]',
		output      BLOB,
		tags        TEXT NOT NULL DEFAULT '{}',
		scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		started_at  TIMESTAMP,
		completed_at TIMESTAMP,
		finalized_at TIMESTAMP,
		node_id     TEXT,
		batch_id    TEXT,
		timeout     INTEGER NOT NULL DEFAULT 0,
		metadata    BLOB NOT NULL DEFAULT '{}',
		unique_key  TEXT
	);

	CREATE TABLE IF NOT EXISTS tasker_nodes (
		node_id     TEXT PRIMARY KEY,
		host        TEXT NOT NULL,
		port        INTEGER NOT NULL DEFAULT 0,
		queues      TEXT NOT NULL DEFAULT '{}',
		workers     INTEGER NOT NULL DEFAULT 0,
		status      TEXT NOT NULL DEFAULT 'active',
		started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		version     TEXT NOT NULL DEFAULT '1.0.0'
	);

	CREATE TABLE IF NOT EXISTS tasker_failed_jobs (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		uuid        TEXT NOT NULL,
		queue       TEXT NOT NULL,
		kind        TEXT NOT NULL,
		payload     BLOB NOT NULL,
		attempt     INTEGER NOT NULL,
		max_attempts INTEGER NOT NULL,
		errors      BLOB NOT NULL,
		failed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		tags        TEXT NOT NULL DEFAULT '{}'
	);

	CREATE TABLE IF NOT EXISTS tasker_leader (
		id          INTEGER PRIMARY KEY CHECK (id = 1),
		node_id     TEXT NOT NULL DEFAULT '',
		host        TEXT NOT NULL DEFAULT 'leader',
		port        INTEGER NOT NULL DEFAULT 0,
		status      TEXT NOT NULL DEFAULT 'leader',
		started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		version     TEXT NOT NULL DEFAULT '1.0.0'
	);

	CREATE TABLE IF NOT EXISTS tasker_locks (
		lock_key    TEXT PRIMARY KEY,
		node_id     TEXT NOT NULL,
		expires_at  TIMESTAMP NOT NULL
	);`

	if _, err := db.Exec(schema); err != nil {
		t.Fatal(err)
	}

	d := &Driver{
		db:      db,
		config:  Config{TablePrefix: "tasker_"},
		dialect: &sqliteDialect{},
	}

	return d
}

func TestEnqueue(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()
	now := time.Now()
	job := &tasker.JobRow{
		UUID:        "test-uuid-1",
		Queue:       "default",
		Kind:        "*tasker.testJob",
		Payload:     []byte(`{"id":"1"}`),
		State:       tasker.StateAvailable,
		MaxAttempts: 3,
		ScheduledAt: now.Add(-time.Hour),
		CreatedAt:   now,
	}

	err := d.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	if job.ID == 0 {
		t.Error("expected job ID to be set")
	}
}

func TestEnqueueBatch(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()
	jobs := []*tasker.JobRow{
		{
			UUID:        "batch-uuid-1",
			Queue:       "default",
			Kind:        "*tasker.testJob",
			Payload:     []byte(`{"id":"1"}`),
			State:       tasker.StateAvailable,
			MaxAttempts: 3,
			ScheduledAt: pastTime(),
			CreatedAt:   time.Now(),
		},
		{
			UUID:        "batch-uuid-2",
			Queue:       "default",
			Kind:        "*tasker.testJob",
			Payload:     []byte(`{"id":"2"}`),
			State:       tasker.StateAvailable,
			MaxAttempts: 3,
			ScheduledAt: pastTime(),
			CreatedAt:   time.Now(),
		},
	}

	err := d.EnqueueBatch(ctx, jobs)
	if err != nil {
		t.Fatal(err)
	}

	for _, j := range jobs {
		if j.ID == 0 {
			t.Error("expected job ID to be set")
		}
	}
}

func TestClaim(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	for i := 0; i < 5; i++ {
		job := &tasker.JobRow{
			UUID:        fmt.Sprintf("claim-uuid-%d", i),
			Queue:       "default",
			Kind:        "*tasker.testJob",
			Payload:     []byte(`{"id":"1"}`),
			State:       tasker.StateAvailable,
			MaxAttempts: 3,
			ScheduledAt: pastTime(),
			CreatedAt:   time.Now(),
		}
		if err := d.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
	}

	jobs, err := d.Claim(ctx, "default", "test-node", 3)
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) != 3 {
		t.Errorf("expected 3 claimed jobs, got %d", len(jobs))
	}

	for _, j := range jobs {
		if j.State != tasker.StateRunning {
			t.Errorf("expected running state, got %s", j.State)
		}
		if j.Attempt != 1 {
			t.Errorf("expected attempt 1, got %d", j.Attempt)
		}
	}
}

func TestComplete(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	job := &tasker.JobRow{
		UUID:        "complete-uuid-1",
		Queue:       "default",
		Kind:        "*tasker.testJob",
		Payload:     []byte(`{"id":"1"}`),
		State:       tasker.StateAvailable,
		MaxAttempts: 3,
		ScheduledAt: pastTime(),
		CreatedAt:   time.Now(),
	}
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	claimed, err := d.Claim(ctx, "default", "test-node", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 {
		t.Fatal("expected to claim a job")
	}

	err = d.Complete(ctx, claimed[0].ID, []byte(`{"result":"ok"}`))
	if err != nil {
		t.Fatal(err)
	}

	retrieved, err := d.GetByID(ctx, claimed[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.State != tasker.StateCompleted {
		t.Errorf("expected completed state, got %s", retrieved.State)
	}
}

func TestFail(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	job := &tasker.JobRow{
		UUID:        "fail-uuid-1",
		Queue:       "default",
		Kind:        "*tasker.testJob",
		Payload:     []byte(`{"id":"1"}`),
		State:       tasker.StateAvailable,
		MaxAttempts: 3,
		ScheduledAt: pastTime(),
		CreatedAt:   time.Now(),
	}
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	claimed, err := d.Claim(ctx, "default", "test-node", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 {
		t.Fatal("expected to claim a job")
	}

	err = d.Fail(ctx, claimed[0].ID, fmt.Errorf("something went wrong"))
	if err != nil {
		t.Fatal(err)
	}

	retrieved, err := d.GetByID(ctx, claimed[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.State != tasker.StateFailed {
		t.Errorf("expected failed state, got %s", retrieved.State)
	}

	var count int
	d.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasker_failed_jobs").Scan(&count)
	if count != 1 {
		t.Errorf("expected 1 failed job record, got %d", count)
	}
}

func TestRetry(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	job := &tasker.JobRow{
		UUID:        "retry-uuid-1",
		Queue:       "default",
		Kind:        "*tasker.testJob",
		Payload:     []byte(`{"id":"1"}`),
		State:       tasker.StateAvailable,
		MaxAttempts: 3,
		ScheduledAt: pastTime(),
		CreatedAt:   time.Now(),
	}
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	claimed, err := d.Claim(ctx, "default", "test-node", 1)
	if err != nil {
		t.Fatal(err)
	}

	err = d.Fail(ctx, claimed[0].ID, fmt.Errorf("error"))
	if err != nil {
		t.Fatal(err)
	}

	retried, err := d.Retry(ctx, claimed[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.State != tasker.StateAvailable {
		t.Errorf("expected available state after retry, got %s", retried.State)
	}
}

func TestCancel(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	job := &tasker.JobRow{
		UUID:        "cancel-uuid-1",
		Queue:       "default",
		Kind:        "*tasker.testJob",
		Payload:     []byte(`{"id":"1"}`),
		State:       tasker.StateAvailable,
		MaxAttempts: 3,
		ScheduledAt: pastTime(),
		CreatedAt:   time.Now(),
	}
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}

	cancelled, err := d.Cancel(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.State != tasker.StateCancelled {
		t.Errorf("expected cancelled state, got %s", cancelled.State)
	}
}

func TestQueryJobs(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		state := tasker.StateAvailable
		if i >= 5 {
			state = tasker.StateCompleted
		}
		d.Enqueue(ctx, &tasker.JobRow{
			UUID:        fmt.Sprintf("query-uuid-%d", i),
			Queue:       "default",
			Kind:        "*tasker.testJob",
			Payload:     []byte(`{}`),
			State:       state,
			MaxAttempts: 3,
			ScheduledAt: pastTime(),
			CreatedAt:   time.Now(),
		})
	}

	jobs, total, err := d.QueryJobs(ctx, tasker.JobFilter{
		Limit:  5,
		Offset: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 5 {
		t.Errorf("expected 5 jobs, got %d", len(jobs))
	}
	if total != 10 {
		t.Errorf("expected total 10, got %d", total)
	}

	filteredJobs, filteredTotal, err := d.QueryJobs(ctx, tasker.JobFilter{
		States: []tasker.State{tasker.StateCompleted},
		Limit:  10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(filteredJobs) != 5 {
		t.Errorf("expected 5 completed jobs, got %d", len(filteredJobs))
	}
	if filteredTotal != 5 {
		t.Errorf("expected total 5, got %d", filteredTotal)
	}
}

func TestQueueStats(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	states := []tasker.State{
		tasker.StateAvailable, tasker.StateRunning, tasker.StateCompleted,
		tasker.StateFailed, tasker.StateRetryable,
	}
	for i, s := range states {
		d.Enqueue(ctx, &tasker.JobRow{
			UUID:        fmt.Sprintf("stats-uuid-%d", i),
			Queue:       "default",
			Kind:        "*tasker.testJob",
			Payload:     []byte(`{}`),
			State:       s,
			MaxAttempts: 3,
			ScheduledAt: pastTime(),
			CreatedAt:   time.Now(),
		})
	}

	stats, err := d.QueueStats(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if stats.Available != 1 {
		t.Errorf("expected 1 available, got %d", stats.Available)
	}
	if stats.Running != 1 {
		t.Errorf("expected 1 running, got %d", stats.Running)
	}
	if stats.Completed != 1 {
		t.Errorf("expected 1 completed, got %d", stats.Completed)
	}
}

func TestNodeLifecycle(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	node := tasker.NodeInfo{
		ID:        "test-node-1",
		Host:      "localhost",
		Port:      8080,
		Status:    "active",
		StartedAt: time.Now(),
		Version:   "1.0.0",
	}

	err := d.RegisterNode(ctx, node, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	err = d.Heartbeat(ctx, "test-node-1", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	nodes, err := d.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}

	err = d.DeregisterNode(ctx, "test-node-1")
	if err != nil {
		t.Fatal(err)
	}

	nodes, _ = d.ListNodes(ctx)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes after deregister, got %d", len(nodes))
	}
}

func TestLeaderElection(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	won, err := d.LeaderElection(ctx, "node-a", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !won {
		t.Error("expected node-a to win first election")
	}

	won, err = d.LeaderElection(ctx, "node-b", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if won {
		t.Error("expected node-b to lose when node-a is leader")
	}

	isLeader, err := d.IsLeader(ctx, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if !isLeader {
		t.Error("expected node-a to be leader")
	}

	err = d.ResignLeadership(ctx, "node-a")
	if err != nil {
		t.Fatal(err)
	}

	isLeader, _ = d.IsLeader(ctx, "node-a")
	if isLeader {
		t.Error("expected node-a to not be leader after resign")
	}
}

func TestLocking(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	ctx := context.Background()

	acquired, err := d.AcquireLock(ctx, "resource-1", "node-a", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Error("expected to acquire lock")
	}

	acquired, err = d.AcquireLock(ctx, "resource-1", "node-b", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Error("expected not to acquire already-held lock")
	}

	err = d.ReleaseLock(ctx, "resource-1", "node-a")
	if err != nil {
		t.Fatal(err)
	}

	acquired, err = d.AcquireLock(ctx, "resource-1", "node-b", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Error("expected to acquire lock after release")
	}
}

func TestGetByIDNotFound(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	_, err := d.GetByID(context.Background(), 999)
	if err != tasker.ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestClaimEmptyQueue(t *testing.T) {
	d := setupTestDB(t)
	defer d.Close()

	jobs, err := d.Claim(context.Background(), "empty-queue", "test-node", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Errorf("expected 0 jobs from empty queue, got %d", len(jobs))
	}
}

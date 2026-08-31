package sqldriver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lemmego/tasker"
)

func setupMigratedSQLite(t *testing.T) *Driver {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", t.TempDir()+"/tasker.db")
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(8)
	d := &Driver{db: db, config: Config{TablePrefix: "contract_"}, dialect: &sqliteDialect{}}
	if err := d.Migrate(context.Background()); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

func contractJob(uuid string, state tasker.State, scheduledAt time.Time) *tasker.JobRow {
	return &tasker.JobRow{
		UUID: uuid, Queue: "default", Kind: "contract.job", Payload: []byte(`{"ok":true}`),
		State: state, MaxAttempts: 3, ScheduledAt: scheduledAt, CreatedAt: time.Now(),
	}
}

func TestSQLiteMigrateAndPortableEncoding(t *testing.T) {
	d := setupMigratedSQLite(t)
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("idempotent migrate: %v", err)
	}

	job := contractJob("encoding", tasker.StateAvailable, time.Now())
	job.AttemptedBy = []tasker.NodeID{"old-node"}
	job.Tags = []string{"one", "with,comma", `with"quote`}
	job.Metadata = map[string]string{"source": "contract"}
	job.UniqueKey = "account:42"
	if err := d.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	got, err := d.GetByID(context.Background(), job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.UniqueKey != job.UniqueKey || got.Metadata["source"] != "contract" {
		t.Fatalf("portable fields not preserved: %#v", got)
	}
	if strings.Join(got.Tags, "|") != strings.Join(job.Tags, "|") || len(got.AttemptedBy) != 1 {
		t.Fatalf("array fields not preserved: tags=%q attempted_by=%q", got.Tags, got.AttemptedBy)
	}
}

func TestSQLiteEnqueueBatchRollsBackIDsAndRows(t *testing.T) {
	d := setupMigratedSQLite(t)
	jobs := []*tasker.JobRow{
		contractJob("duplicate", tasker.StateAvailable, time.Now()),
		contractJob("duplicate", tasker.StateAvailable, time.Now()),
	}
	if err := d.EnqueueBatch(context.Background(), jobs); err == nil {
		t.Fatal("expected duplicate UUID error")
	}
	if jobs[0].ID != 0 || jobs[1].ID != 0 {
		t.Fatalf("rolled back batch leaked IDs: %d, %d", jobs[0].ID, jobs[1].ID)
	}
	var count int
	if err := d.db.QueryRow(`SELECT COUNT(*) FROM contract_jobs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("batch was not atomic: count=%d err=%v", count, err)
	}
}

func TestSQLiteClaimDueStatesAndAttemptLimit(t *testing.T) {
	d := setupMigratedSQLite(t)
	now := time.Now()
	jobs := []*tasker.JobRow{
		contractJob("available", tasker.StateAvailable, now.Add(-time.Minute)),
		contractJob("scheduled-due", tasker.StateScheduled, now.Add(-time.Minute)),
		contractJob("retryable-due", tasker.StateRetryable, now.Add(-time.Minute)),
		contractJob("future", tasker.StateScheduled, now.Add(time.Hour)),
		contractJob("exhausted", tasker.StateRetryable, now.Add(-time.Minute)),
	}
	jobs[4].Attempt = jobs[4].MaxAttempts
	if err := d.EnqueueBatch(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	// Older SQLite rows used PostgreSQL-style empty array text.
	if _, err := d.db.Exec(`UPDATE contract_jobs SET attempted_by = '{}' WHERE id = ?`, jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(context.Background(), "default", "node-a", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 3 {
		t.Fatalf("expected three due claimable jobs, got %d", len(claimed))
	}
	for _, job := range claimed {
		if job.State != tasker.StateRunning || job.NodeID != "node-a" || len(job.AttemptedBy) != 1 {
			t.Fatalf("invalid claimed job: %#v", job)
		}
	}
}

func TestSQLiteConcurrentClaimsDoNotDuplicate(t *testing.T) {
	d := setupMigratedSQLite(t)
	for i := 0; i < 20; i++ {
		if err := d.Enqueue(context.Background(), contractJob(fmt.Sprintf("concurrent-%d", i), tasker.StateAvailable, time.Now().Add(-time.Minute))); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	results := make(chan []*tasker.JobRow, 2)
	errs := make(chan error, 2)
	for _, node := range []tasker.NodeID{"node-a", "node-b"} {
		wg.Add(1)
		go func(node tasker.NodeID) {
			defer wg.Done()
			jobs, err := d.Claim(context.Background(), "default", node, 20)
			results <- jobs
			errs <- err
		}(node)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[tasker.JobID]bool{}
	for jobs := range results {
		for _, job := range jobs {
			if seen[job.ID] {
				t.Fatalf("job %d claimed twice", job.ID)
			}
			seen[job.ID] = true
		}
	}
	if len(seen) != 20 {
		t.Fatalf("expected all jobs claimed once, got %d", len(seen))
	}
}

func TestSQLiteTransitionsFiltersStaleAndPrune(t *testing.T) {
	d := setupMigratedSQLite(t)
	ctx := context.Background()
	job := contractJob("transition-search", tasker.StateRetryable, time.Now().Add(-time.Minute))
	job.Tags = []string{"important"}
	job.Errors = []tasker.AttemptError{{Attempt: 1, Error: "first", Timestamp: time.Now()}}
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(ctx, "default", "node", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: jobs=%d err=%v", len(claimed), err)
	}
	if err := d.Fail(ctx, job.ID, errors.New("second")); err != nil {
		t.Fatal(err)
	}
	failed, err := d.GetByID(ctx, job.ID)
	if err != nil || len(failed.Errors) != 2 || failed.Errors[1].Attempt != 1 {
		t.Fatalf("failure history: job=%#v err=%v", failed, err)
	}
	if _, err := d.Retry(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err = d.Claim(ctx, "default", "node", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("reclaim: jobs=%d err=%v", len(claimed), err)
	}
	old := time.Now().Add(-time.Hour)
	if _, err := d.db.Exec(`UPDATE contract_jobs SET attempted_at = ?, started_at = ? WHERE id = ?`, old, old, job.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := d.RequeueStale(ctx, time.Minute); err != nil || n != 1 {
		t.Fatalf("requeue stale: n=%d err=%v", n, err)
	}
	listed, total, err := d.QueryJobs(ctx, tasker.JobFilter{Search: "transition", Tags: []string{"important"}, OrderBy: "id; DROP TABLE contract_jobs", Limit: 10})
	if err != nil || total != 1 || len(listed) != 1 {
		t.Fatalf("safe filtered query: total=%d jobs=%d err=%v", total, len(listed), err)
	}
	if n, err := d.Prune(ctx, time.Now().Add(time.Hour), nil); err != nil || n != 0 {
		t.Fatalf("empty prune: n=%d err=%v", n, err)
	}
	if _, err := d.Cancel(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if n, err := d.Prune(ctx, time.Now().Add(time.Hour), []tasker.State{tasker.StateCancelled}); err != nil || n != 1 {
		t.Fatalf("prune cancelled: n=%d err=%v", n, err)
	}
}

func TestSQLiteScheduleRetryUpdatesExistingRow(t *testing.T) {
	d := setupMigratedSQLite(t)
	ctx := context.Background()
	job := contractJob("schedule-retry", tasker.StateAvailable, time.Now().Add(-time.Second))
	job.Metadata = map[string]string{"tenant": "42"}
	if err := d.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	claimed, err := d.Claim(ctx, job.Queue, "worker-a", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: jobs=%d err=%v", len(claimed), err)
	}
	due := time.Now().Add(100 * time.Millisecond)
	if err := d.ScheduleRetry(ctx, job.ID, errors.New("temporary"), due); err != nil {
		t.Fatal(err)
	}

	retried, err := d.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.UUID != job.UUID || retried.Payload == nil || retried.Metadata["tenant"] != "42" || retried.Attempt != 1 {
		t.Fatalf("retry did not preserve job data: %#v", retried)
	}
	if retried.State != tasker.StateRetryable || retried.NodeID != "" || retried.AttemptedAt != nil || retried.StartedAt != nil || len(retried.Errors) != 1 || retried.Errors[0].Attempt != 1 {
		t.Fatalf("invalid retry transition: %#v", retried)
	}
	var count int
	if err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM contract_jobs`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retry changed row count: count=%d err=%v", count, err)
	}
	if jobs, err := d.Claim(ctx, job.Queue, "worker-b", 1); err != nil || len(jobs) != 0 {
		t.Fatalf("retry claimed before due: jobs=%d err=%v", len(jobs), err)
	}
	time.Sleep(110 * time.Millisecond)
	claimed, err = d.Claim(ctx, job.Queue, "worker-b", 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != job.ID || claimed[0].Attempt != 2 {
		t.Fatalf("retry not reclaimed: jobs=%#v err=%v", claimed, err)
	}
}

func TestScanRejectsMalformedJSON(t *testing.T) {
	d := setupMigratedSQLite(t)
	job := contractJob("malformed", tasker.StateAvailable, time.Now())
	if err := d.Enqueue(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if _, err := d.db.Exec(`UPDATE contract_jobs SET metadata = '{' WHERE id = ?`, job.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := d.GetByID(context.Background(), job.ID); err == nil || !strings.Contains(err.Error(), "metadata") {
		t.Fatalf("expected metadata decoding error, got %v", err)
	}
}

func TestMySQLClaimSQLIsMySQLNative(t *testing.T) {
	query := (&mysqlDialect{}).ClaimQuery("tasker_jobs")
	for _, forbidden := range []string{"$1", "RETURNING", "array_", "ON CONFLICT", "::"} {
		if strings.Contains(query, forbidden) {
			t.Fatalf("MySQL claim query contains %q: %s", forbidden, query)
		}
	}
	for _, required := range []string{"?", "FOR UPDATE SKIP LOCKED", "scheduled", "retryable"} {
		if !strings.Contains(query, required) {
			t.Fatalf("MySQL claim query missing %q: %s", required, query)
		}
	}
}

func TestMySQLGeneratedSchemaAndInsertAreMySQLNative(t *testing.T) {
	d := &Driver{config: Config{TablePrefix: "tasker_"}, dialect: &mysqlDialect{}}
	queries := append(d.migrationTableStatements(), d.jobInsertQuery())
	joined := strings.Join(queries, "\n")
	for _, forbidden := range []string{"$1", "RETURNING", "BIGSERIAL", "AUTOINCREMENT", "TEXT[]", "BYTEA", "ON CONFLICT", "::"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("generated MySQL SQL contains %q:\n%s", forbidden, joined)
		}
	}
	for _, required := range []string{"AUTO_INCREMENT", "VARCHAR(255)", "LONGBLOB", "JSON", "VALUES (?, ?, ?"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("generated MySQL SQL missing %q:\n%s", required, joined)
		}
	}
	if got := d.durationMillisSQL("started_at", "completed_at"); !strings.Contains(got, "TIMESTAMPDIFF") {
		t.Fatalf("unexpected MySQL duration expression: %s", got)
	}
}

func TestPostgresArrayEncodingRoundTrip(t *testing.T) {
	want := []string{"plain", "with,comma", `with"quote`, `with\slash`, ""}
	if got := decodeTextArray(encodeTextArray(want)); strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("array round trip: got %q want %q", got, want)
	}
}

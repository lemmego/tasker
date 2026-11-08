package redisdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/lemmego/tasker"
)

// emptyList matches a list-valued field of a stored job holding an empty
// collection, in either spelling. `{}` is the one cjson writes and the one
// that made the job undecodable; `[]` is what miniredis writes, and it is
// matched too so this test says the same thing under both engines.
var emptyList = regexp.MustCompile(`"(errors|tags|attempted_by)":\s*(\{\}|\[\])`)

func assertNoEmptyList(t *testing.T, stage, raw string) {
	t.Helper()
	if match := emptyList.FindString(raw); match != "" {
		t.Errorf("after %s the job was stored with %s, which Go cannot read back into a slice: %s", stage, match, raw)
	}
	if _, err := decodeJob(raw); err != nil {
		t.Errorf("after %s the stored job no longer decodes: %v", stage, err)
	}
}

// walkEveryTransition drives one job through every state change the driver
// has, asserting after each that the job it left behind can still be read.
// Each transition re-encodes the whole job in Lua, so each one is a chance to
// turn a list into an object.
func walkEveryTransition(t *testing.T, driver *Driver, stored func(id tasker.JobID) string) {
	t.Helper()
	ctx := context.Background()

	// Tags are empty rather than absent: an empty slice is what a job whose
	// Tags() returns none produces, and Go writes it as `[]`.
	queued := func(uuid string) tasker.JobID {
		t.Helper()
		job := testJob(uuid, time.Now().Add(-time.Minute))
		job.Tags = []string{}
		if err := driver.Enqueue(ctx, job); err != nil {
			t.Fatal(err)
		}
		assertNoEmptyList(t, "enqueue", stored(job.ID))
		return job.ID
	}

	id := queued("transitions-1")

	if _, err := driver.Claim(ctx, "default", "node-1", 10); err != nil {
		t.Fatal(err)
	}
	assertNoEmptyList(t, "claim", stored(id))

	if err := driver.ScheduleRetry(ctx, id, fmt.Errorf("first failure"), time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	assertNoEmptyList(t, "schedule retry", stored(id))

	if _, err := driver.Claim(ctx, "default", "node-1", 10); err != nil {
		t.Fatal(err)
	}
	if err := driver.Fail(ctx, id, fmt.Errorf("second failure")); err != nil {
		t.Fatal(err)
	}
	assertNoEmptyList(t, "fail", stored(id))

	// The reported bug: retrying clears the attempt history, and an empty
	// history is what cjson encodes as an object.
	if _, err := driver.Retry(ctx, id); err != nil {
		t.Fatalf("retry a failed job: %v", err)
	}
	assertNoEmptyList(t, "retry", stored(id))

	if _, err := driver.Claim(ctx, "default", "node-1", 10); err != nil {
		t.Fatal(err)
	}
	if err := driver.Complete(ctx, id, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	assertNoEmptyList(t, "complete", stored(id))

	cancelled := queued("transitions-2")
	if _, err := driver.Cancel(ctx, cancelled); err != nil {
		t.Fatal(err)
	}
	assertNoEmptyList(t, "cancel", stored(cancelled))

	// Retrying from the cancelled list is the click that was reported.
	if _, err := driver.Retry(ctx, cancelled); err != nil {
		t.Fatalf("retry a cancelled job: %v", err)
	}
	assertNoEmptyList(t, "retry after cancel", stored(cancelled))

	stale := queued("transitions-3")
	if _, err := driver.Claim(ctx, "default", "node-1", 10); err != nil {
		t.Fatal(err)
	}
	if _, err := driver.RequeueStale(ctx, 0); err != nil {
		t.Fatal(err)
	}
	assertNoEmptyList(t, "requeue stale", stored(stale))

	// And the dashboard reads the whole hash at once, so one unreadable job
	// is the whole list.
	if _, _, err := driver.QueryJobs(ctx, tasker.JobFilter{}); err != nil {
		t.Fatalf("list every job: %v", err)
	}
}

func TestNoTransitionStoresAnEmptyList(t *testing.T) {
	driver, server := testDriver(t)
	walkEveryTransition(t, driver, func(id tasker.JobID) string {
		t.Helper()
		return server.HGet("test:jobs", fmt.Sprint(uint64(id)))
	})
}

// TestNoTransitionStoresAnEmptyListOnRealRedis is the only test here that runs
// against the cjson the driver's scripts actually execute against. miniredis
// encodes an empty Lua table as `[]`, which is readable, so every test above
// passed throughout the lifetime of this bug.
func TestNoTransitionStoresAnEmptyListOnRealRedis(t *testing.T) {
	addr := os.Getenv("TASKER_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TASKER_REDIS_ADDR to run against a real Redis")
	}
	prefix := fmt.Sprintf("tasker-test-%d:", time.Now().UnixNano())
	driver, err := NewDriver(Config{Addr: addr, DB: 15, KeyPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = driver.Close() })

	client := redis.NewClient(&redis.Options{Addr: addr, DB: 15})
	t.Cleanup(func() { _ = client.Close() })
	t.Cleanup(func() {
		keys, err := client.Keys(context.Background(), prefix+"*").Result()
		if err == nil && len(keys) > 0 {
			_ = client.Del(context.Background(), keys...).Err()
		}
	})

	walkEveryTransition(t, driver, func(id tasker.JobID) string {
		t.Helper()
		raw, err := client.HGet(context.Background(), prefix+"jobs", fmt.Sprint(uint64(id))).Result()
		if err != nil {
			t.Fatal(err)
		}
		return raw
	})
}

// TestAJobAlreadyStoredWithAnEmptyObjectStillDecodes covers the jobs written
// before the fix. They are in production queues now, and without this the only
// repair is editing Redis by hand.
func TestAJobAlreadyStoredWithAnEmptyObjectStillDecodes(t *testing.T) {
	corrupt := `{"id":7,"uuid":"corrupt","queue":"default","kind":"mail.send",` +
		`"state":"available","attempt":0,"max_attempts":3,"errors":{},"tags":{},` +
		`"attempted_by":{},"scheduled_at":"2026-01-01T00:00:00Z",` +
		`"created_at":"2026-01-01T00:00:00Z"}`

	job, err := decodeJob(corrupt)
	if err != nil {
		t.Fatalf("a job stored before the fix must still be readable: %v", err)
	}
	if job.Errors != nil || job.Tags != nil || job.AttemptedBy != nil {
		t.Errorf("an empty object is an empty list: %+v", job)
	}

	driver, server := testDriver(t)
	server.HSet("test:jobs", "7", corrupt)
	if _, err := driver.GetByID(context.Background(), 7); err != nil {
		t.Errorf("GetByID: %v", err)
	}
	jobs, _, err := driver.QueryJobs(context.Background(), tasker.JobFilter{})
	if err != nil {
		t.Fatalf("one unreadable job must not take down the list: %v", err)
	}
	if len(jobs) != 1 {
		t.Errorf("want the corrupt job listed, got %d", len(jobs))
	}
}

// TestARecordedErrorIsStillRead guards the shadowing in storedJob: the fields
// are read through json.RawMessage, so a real attempt history has to survive
// that and not only an empty one.
func TestARecordedErrorIsStillRead(t *testing.T) {
	want := tasker.JobRow{
		ID: 9, UUID: "kept", Queue: "default", Kind: "mail.send",
		State: tasker.StateFailed, MaxAttempts: 3,
		AttemptedBy: []tasker.NodeID{"node-1"},
		Tags:        []string{"mail"},
		Errors: []tasker.AttemptError{{
			Attempt: 1, Error: `boom {"errors":{}}`,
			Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}},
	}
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeJob(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Errors) != 1 || got.Errors[0].Error != want.Errors[0].Error {
		t.Errorf("errors: got %+v, want %+v", got.Errors, want.Errors)
	}
	if len(got.Tags) != 1 || len(got.AttemptedBy) != 1 {
		t.Errorf("tags %v and attempted_by %v must survive", got.Tags, got.AttemptedBy)
	}
}

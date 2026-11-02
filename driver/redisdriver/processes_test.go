package redisdriver

import (
	"testing"
	"time"

	"github.com/lemmego/tasker"
)

// TestAnIdleWorkerCountsAsAProcess.
//
// The same bug the SQL driver had, in the other implementation: Processes
// counted distinct node ids over jobs in the running state, so a worker that
// was up with nothing to do counted zero — which is what a healthy worker is
// almost all the time. A dashboard reading it said "No worker is reporting"
// directly above a table listing that worker.
//
// Both drivers answer from the node registry now, which is the thing that
// knows.
func TestAnIdleWorkerCountsAsAProcess(t *testing.T) {
	d, _ := testDriver(t)
	ctx := t.Context()

	if err := d.RegisterNode(ctx, tasker.NodeInfo{
		ID:            "node-idle",
		Host:          "worker-1",
		Workers:       3,
		Status:        "active",
		StartedAt:     time.Now().Add(-time.Hour),
		LastHeartbeat: time.Now(),
		Version:       "1.0.0",
	}, time.Minute); err != nil {
		t.Fatal(err)
	}

	// Deliberately no running job. That is the whole case.
	stats, err := d.GlobalStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Processes != 1 {
		t.Errorf("Processes = %d with one idle worker registered, want 1 — "+
			"an idle worker is a worker", stats.Processes)
	}

	// And the two sources must agree, which is the actual invariant.
	nodes, err := d.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != stats.Processes {
		t.Errorf("ListNodes returned %d nodes but Processes says %d; a dashboard "+
			"showing both contradicts itself", len(nodes), stats.Processes)
	}
}

// TestAnExpiredNodeStopsCountingAsAProcess.
//
// Liveness here is the key's TTL rather than a timestamp comparison, so this
// expires it the way time would and asserts both answers move together.
func TestAnExpiredNodeStopsCountingAsAProcess(t *testing.T) {
	d, server := testDriver(t)
	ctx := t.Context()

	if err := d.RegisterNode(ctx, tasker.NodeInfo{
		ID:        "node-gone",
		Host:      "worker-gone",
		Status:    "active",
		StartedAt: time.Now().Add(-time.Hour),
	}, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if stats, err := d.GlobalStats(ctx); err != nil {
		t.Fatal(err)
	} else if stats.Processes != 1 {
		t.Fatalf("Processes = %d before expiry, want 1 — the fixture never "+
			"registered, so the assertion below would prove nothing",
			stats.Processes)
	}

	// miniredis does not expire on a wall clock, so time is moved instead.
	server.FastForward(time.Minute)

	stats, err := d.GlobalStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Processes != 0 {
		t.Errorf("Processes = %d after the node's key expired, want 0",
			stats.Processes)
	}
	nodes, err := d.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != stats.Processes {
		t.Errorf("ListNodes returned %d nodes but Processes says %d",
			len(nodes), stats.Processes)
	}
}

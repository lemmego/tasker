package sqldriver

import (
	"testing"
	"time"

	"github.com/lemmego/tasker"
)

// TestAnIdleWorkerCountsAsAProcess.
//
// GlobalStats.Processes counted DISTINCT node_id over jobs in the *running*
// state, so a worker that was up with nothing to do counted zero — which is
// what a healthy worker is almost all the time. A dashboard reading it
// printed "No worker is reporting" directly above a table listing that
// worker with a heartbeat seconds old.
//
// Seen on a live deployment, not in a test, and not by anything watching the
// numbers — by a person noticing the page disagreed with itself.
func TestAnIdleWorkerCountsAsAProcess(t *testing.T) {
	d := setupTestDB(t)
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

	// And the two sources must agree, which is the actual invariant: the
	// overview's claim and the table under it come from one answer.
	nodes, err := d.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != stats.Processes {
		t.Errorf("ListNodes returned %d nodes but Processes says %d; a dashboard "+
			"showing both contradicts itself", len(nodes), stats.Processes)
	}
}

// age backdates a node's heartbeat.
//
// Written straight to the table because RegisterNode binds time.Now() for
// last_heartbeat and ignores the value passed — registering is itself a
// heartbeat, which is right, and means a fixture cannot create a stale node
// through the public call. The first version of these tests tried and they
// failed against correct code.
func age(t *testing.T, d *Driver, id string, by time.Duration) {
	t.Helper()
	if _, err := d.db.Exec(
		`UPDATE tasker_nodes SET last_heartbeat = ? WHERE node_id = ?`,
		time.Now().Add(-by), id); err != nil {
		t.Fatal(err)
	}
}

// TestADeadWorkerStopsCountingAsAProcess, which is the half that keeps the
// fix from being "always report at least one". A node that has stopped
// reporting must leave both answers together.
func TestADeadWorkerStopsCountingAsAProcess(t *testing.T) {
	d := setupTestDB(t)
	ctx := t.Context()

	if err := d.RegisterNode(ctx, tasker.NodeInfo{
		ID:        "node-dead",
		Host:      "worker-gone",
		Workers:   3,
		Status:    "active",
		StartedAt: time.Now().Add(-2 * time.Hour),
		Version:   "1.0.0",
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	age(t, d, "node-dead", tasker.NodeLivenessWindow+time.Minute)

	stats, err := d.GlobalStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Processes != 0 {
		t.Errorf("Processes = %d for a node that stopped reporting, want 0",
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

// TestLivenessIsOneWindow.
//
// ListNodes filtered on a locally-written 30 seconds and GlobalStats had no
// notion of liveness at all. Both read the constant now, so the two cannot
// drift into answering differently — which is the bug rather than a detail
// of it.
func TestLivenessIsOneWindow(t *testing.T) {
	d := setupTestDB(t)
	ctx := t.Context()

	// Just inside the window, and just outside it.
	for id, beat := range map[string]time.Duration{
		"node-fresh": tasker.NodeLivenessWindow - 5*time.Second,
		"node-stale": tasker.NodeLivenessWindow + 5*time.Second,
	} {
		if err := d.RegisterNode(ctx, tasker.NodeInfo{
			ID:        tasker.NodeID(id),
			Host:      id,
			Status:    "active",
			StartedAt: time.Now().Add(-time.Hour),
		}, time.Minute); err != nil {
			t.Fatal(err)
		}
		age(t, d, id, beat)
	}

	stats, err := d.GlobalStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := d.ListNodes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Processes != 1 || len(nodes) != 1 {
		t.Errorf("Processes = %d and ListNodes = %d across the liveness "+
			"boundary; both should see exactly the fresh node",
			stats.Processes, len(nodes))
	}
	if len(nodes) == 1 && nodes[0].ID != "node-fresh" {
		t.Errorf("the surviving node is %q, want node-fresh", nodes[0].ID)
	}
}

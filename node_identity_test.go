package tasker

import "testing"

// A Config assembled field by field carries no NodeID, and WithNodeID used to
// apply it anyway — overwriting the unique one DefaultConfig had generated.
//
// Every process in a deployment then ran as the same node. That is not a
// cosmetic duplicate: the node id is the owner token for the leader lease,
// for every mutual-exclusion lock and for job claims, and the driver's
// compare-and-renew script reads owner == ARGV[1] as "this is mine". With ""
// on both sides it is true for everyone.
func TestAConfiguredManagerGetsItsOwnNodeID(t *testing.T) {
	m := NewConfiguredManager(Config{DefaultQueue: "default", DefaultMaxAttempts: 3})

	if m.Config().NodeID == "" {
		t.Fatal("NewConfiguredManager produced an empty NodeID; every process " +
			"in the deployment then shares one identity, so each one holds the " +
			"leader lease and can release the others' locks")
	}
}

// Two processes are two nodes. The ids come from DefaultConfig, so this is
// really asserting that the default is per-manager rather than a package
// constant.
func TestTwoConfiguredManagersAreDifferentNodes(t *testing.T) {
	first := NewConfiguredManager(Config{})
	second := NewConfiguredManager(Config{})

	if first.Config().NodeID == second.Config().NodeID {
		t.Fatalf("both managers are node %q; a shared id means one process's "+
			"shutdown deregisters the other, and every later heartbeat fails "+
			"with ErrNodeNotFound", first.Config().NodeID)
	}
}

// An id that was actually chosen still wins — the guard must not swallow a
// deliberate value, which is how a deployment pins a node to a hostname.
func TestAnExplicitNodeIDIsHonoured(t *testing.T) {
	m := NewConfiguredManager(Config{NodeID: "worker-eu-1"})

	if got := m.Config().NodeID; got != "worker-eu-1" {
		t.Errorf("NodeID = %q, want worker-eu-1", got)
	}
}

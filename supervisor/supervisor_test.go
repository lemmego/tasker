package supervisor

import (
	"testing"
	"time"
)

func TestHeartbeatTTLAllowsSchedulingJitter(t *testing.T) {
	s := &Supervisor{config: Config{HeartbeatInterval: 5 * time.Second}}
	if got, want := s.heartbeatTTL(), 15*time.Second; got != want {
		t.Fatalf("heartbeat TTL = %s, want %s", got, want)
	}
	if s.heartbeatTTL() <= s.config.HeartbeatInterval {
		t.Fatal("heartbeat TTL must exceed the heartbeat interval")
	}
}

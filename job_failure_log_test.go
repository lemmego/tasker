package tasker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// capture records every slog record at every level.
type capture struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *capture) Enabled(context.Context, slog.Level) bool { return true }
func (c *capture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r.Clone())
	return nil
}
func (c *capture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capture) WithGroup(string) slog.Handler      { return c }

func (c *capture) find(msg string) (slog.Record, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if strings.Contains(r.Message, msg) {
			return r, true
		}
	}
	return slog.Record{}, false
}

type alwaysFailingJob struct{}

func (j *alwaysFailingJob) Handle(context.Context) error {
	return errors.New("smtp: dialing smtp.example.test:465: i/o timeout")
}

// A job the system accepted and will now never do is the most important thing
// a queue has to say, and it was said at Info — below the level a worker
// prints on a container platform, where api/logger raises a command's console
// to warn. So the one line naming *why* the work was abandoned, error
// included, could not reach the logs: the queue dashboard showed jobs cycling
// and the deployment's log was empty.
func TestAPermanentFailureIsLoggedAtError(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })
	recorder := &capture{}
	slog.SetDefault(slog.New(recorder))

	RegisterJob("test.always_failing", func() Job { return &alwaysFailingJob{} })

	mgr := NewManager(WithDriver(poolTestDriver{}), WithNodeID("test-node"))
	w := newWorker("w1", mgr, "default")

	payload, _ := json.Marshal(&alwaysFailingJob{})
	// Already at its last attempt, so the next outcome is permanent.
	job := &JobRow{
		ID: 1, Queue: "default", Kind: "test.always_failing", Payload: payload,
		State: StateRunning, Attempt: 3, MaxAttempts: 3,
	}

	w.execute(context.Background(), job)

	record, ok := recorder.find("job failed permanently")
	if !ok {
		t.Fatal("nothing was logged when a job failed permanently")
	}
	if record.Level < slog.LevelError {
		t.Errorf("a permanent failure logged at %s; it has to be at least ERROR, "+
			"or a worker whose console sits at warn — which is every worker run "+
			"as a CLI command — reports abandoned work as silence", record.Level)
	}
}

package tasker

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

// recordingDriver captures what actually reached the driver, which is the
// only way to tell "the middleware saw the rows" from "the middleware saw
// the rows and they were sent".
type recordingDriver struct {
	poolTestDriver
	sent  []*JobRow
	fail  error
	calls int
}

func (d *recordingDriver) Enqueue(_ context.Context, row *JobRow) error {
	d.calls++
	if d.fail != nil {
		return d.fail
	}
	d.sent = append(d.sent, row)
	return nil
}

func (d *recordingDriver) EnqueueBatch(_ context.Context, rows []*JobRow) error {
	d.calls++
	if d.fail != nil {
		return d.fail
	}
	d.sent = append(d.sent, rows...)
	return nil
}

type dispatchTestJob struct {
	Name string
}

func (j *dispatchTestJob) Handle(context.Context) error { return nil }

func init() {
	RegisterJob("tasker.dispatch-middleware-test", func() Job { return &dispatchTestJob{} })
}

// clearDispatch keeps the process-wide registration from leaking into the
// next test, which is the known cost of matching AddGlobalMiddleware's scope.
func clearDispatch(t *testing.T) {
	t.Helper()
	t.Cleanup(func() { SetDispatchMiddlewares(nil) })
}

// Every enqueue path, not just Dispatch. A middleware installed to watch
// enqueues and missing one of the three is worse than none: the gap is
// silent, and a job nobody saw being queued looks like one the worker
// invented.
func TestEveryEnqueuePathIsSeen(t *testing.T) {
	cases := map[string]func(m *Manager) error{
		"Dispatch": func(m *Manager) error {
			_, err := m.Dispatch(context.Background(), &dispatchTestJob{Name: "one"})
			return err
		},
		"DispatchBatch": func(m *Manager) error {
			_, err := m.DispatchBatch(context.Background(), []Job{
				&dispatchTestJob{Name: "one"}, &dispatchTestJob{Name: "two"},
			})
			return err
		},
		"Chain": func(m *Manager) error {
			_, err := m.Chain(context.Background(), []Job{
				&dispatchTestJob{Name: "one"}, &dispatchTestJob{Name: "two"},
			})
			return err
		},
	}

	for name, dispatch := range cases {
		t.Run(name, func(t *testing.T) {
			clearDispatch(t)

			var seen [][]*JobRow
			AddDispatchMiddleware(func(ctx context.Context, rows []*JobRow, next func(context.Context) error) error {
				seen = append(seen, rows)
				return next(ctx)
			})

			driver := &recordingDriver{}
			if err := dispatch(NewManager(WithDriver(driver), WithNodeID("test-node"))); err != nil {
				t.Fatal(err)
			}

			if len(seen) != 1 {
				t.Fatalf("the middleware ran %d times for one %s, want 1", len(seen), name)
			}
			if len(seen[0]) != len(driver.sent) {
				t.Errorf("the middleware saw %d rows, the driver was sent %d",
					len(seen[0]), len(driver.sent))
			}
			if len(seen[0]) == 0 {
				t.Error("the middleware was handed no rows")
			}
		})
	}
}

// Writing to the rows is the point rather than a side effect: Metadata is how
// a job carries the id of the request that queued it, and that is what makes
// an enqueue here and an execution in a worker process the same story. A hook
// that only reported the outcome could not put it there.
func TestAMiddlewareCanStampTheRowsBeforeTheyAreSent(t *testing.T) {
	clearDispatch(t)

	AddDispatchMiddleware(func(ctx context.Context, rows []*JobRow, next func(context.Context) error) error {
		for _, row := range rows {
			if row.Metadata == nil {
				row.Metadata = map[string]string{}
			}
			row.Metadata["request_id"] = "req-7"
		}
		return next(ctx)
	})

	driver := &recordingDriver{}
	manager := NewManager(WithDriver(driver), WithNodeID("test-node"))
	row, err := manager.Dispatch(context.Background(), &dispatchTestJob{Name: "one"})
	if err != nil {
		t.Fatal(err)
	}

	// Asserted on what the driver received, not on the row the middleware
	// held: the stamp has to survive into the enqueue to be worth anything.
	if len(driver.sent) != 1 {
		t.Fatalf("the driver received %d rows", len(driver.sent))
	}
	if got := driver.sent[0].Metadata["request_id"]; got != "req-7" {
		t.Errorf("the stamp did not reach the driver: metadata = %v", driver.sent[0].Metadata)
	}
	// And back out to the caller, so a handler can log the id it queued
	// under without reaching for the middleware's copy.
	if got := row.Metadata["request_id"]; got != "req-7" {
		t.Errorf("Dispatch returned a row without the stamp: %v", row.Metadata)
	}
}

// A failed enqueue is the one most worth seeing, so the middleware has to run
// around the driver call rather than after a successful one.
func TestAFailedEnqueueIsStillSeen(t *testing.T) {
	clearDispatch(t)

	refused := errors.New("the queue is unreachable")
	var observed error
	ran := false
	AddDispatchMiddleware(func(ctx context.Context, _ []*JobRow, next func(context.Context) error) error {
		ran = true
		observed = next(ctx)
		return observed
	})

	driver := &recordingDriver{fail: refused}
	manager := NewManager(WithDriver(driver), WithNodeID("test-node"))
	_, err := manager.Dispatch(context.Background(), &dispatchTestJob{Name: "one"})

	if !ran {
		t.Fatal("the middleware did not run for a failing enqueue")
	}
	if !errors.Is(observed, refused) {
		t.Errorf("the middleware saw %v, want the driver's error", observed)
	}
	if !errors.Is(err, refused) {
		t.Errorf("Dispatch returned %v; the middleware swallowed the error", err)
	}
}

// Refusing is real power, so it is asserted rather than left implied: a
// middleware that returns without calling next must stop the enqueue.
func TestAMiddlewareThatDoesNotCallNextRefusesTheEnqueue(t *testing.T) {
	clearDispatch(t)

	refused := errors.New("refused by policy")
	AddDispatchMiddleware(func(context.Context, []*JobRow, func(context.Context) error) error {
		return refused
	})

	driver := &recordingDriver{}
	manager := NewManager(WithDriver(driver), WithNodeID("test-node"))
	if _, err := manager.Dispatch(context.Background(), &dispatchTestJob{Name: "one"}); !errors.Is(err, refused) {
		t.Errorf("Dispatch returned %v, want the refusal", err)
	}
	if driver.calls != 0 {
		t.Errorf("the driver was called %d times despite the refusal", driver.calls)
	}
}

// Index 0 outermost, matching the execution middleware in pool.go. Stated by
// a test because the two chains being built the same way is the only reason
// somebody reading one can trust the other.
func TestTheOutermostMiddlewareIsTheFirstRegistered(t *testing.T) {
	clearDispatch(t)

	var order []string
	AddDispatchMiddleware(func(ctx context.Context, _ []*JobRow, next func(context.Context) error) error {
		order = append(order, "first in")
		err := next(ctx)
		order = append(order, "first out")
		return err
	})
	AddDispatchMiddleware(func(ctx context.Context, _ []*JobRow, next func(context.Context) error) error {
		order = append(order, "second in")
		err := next(ctx)
		order = append(order, "second out")
		return err
	})

	manager := NewManager(WithDriver(&recordingDriver{}), WithNodeID("test-node"))
	if _, err := manager.Dispatch(context.Background(), &dispatchTestJob{Name: "one"}); err != nil {
		t.Fatal(err)
	}

	want := []string{"first in", "second in", "second out", "first out"}
	if len(order) != len(want) {
		t.Fatalf("order = %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// Nobody watching is the normal case and must cost an enqueue nothing it can
// notice.
func TestAnEnqueueWorksWithNoMiddleware(t *testing.T) {
	clearDispatch(t)

	driver := &recordingDriver{}
	manager := NewManager(WithDriver(driver), WithNodeID("test-node"))
	if _, err := manager.Dispatch(context.Background(), &dispatchTestJob{Name: "one"}); err != nil {
		t.Fatal(err)
	}
	if len(driver.sent) != 1 {
		t.Errorf("the driver received %d rows, want 1", len(driver.sent))
	}
}

// The round trip that is the whole point of both halves: a dispatch
// middleware writes to Metadata, and the execution side reads it back off the
// context.
//
// Asserted through the real worker rather than by calling ContextWithRow
// directly, because what is under test is that the worker installs it before
// the middleware chain runs — a ContextWithRow nobody calls proves nothing.
func TestAStampWrittenAtDispatchIsReadableAtExecution(t *testing.T) {
	clearDispatch(t)

	kind := "tasker.dispatch-roundtrip-test"
	RegisterJob(kind, func() Job { return &dispatchTestJob{} })

	AddDispatchMiddleware(func(ctx context.Context, rows []*JobRow, next func(context.Context) error) error {
		for _, row := range rows {
			if row.Metadata == nil {
				row.Metadata = map[string]string{}
			}
			row.Metadata["request_id"] = "req-7"
		}
		return next(ctx)
	})

	driver := &recordingDriver{}
	manager := NewManager(WithDriver(driver), WithNodeID("test-node"))
	row, err := manager.Dispatch(context.Background(), &dispatchTestJob{Name: "one"})
	if err != nil {
		t.Fatal(err)
	}
	row.Kind = kind

	var seen string
	SetGlobalMiddlewares([]JobMiddleware{
		func(ctx context.Context, _ Job, _ []byte, next func(context.Context) error) error {
			if executing, ok := RowFromContext(ctx); ok {
				seen = executing.Metadata["request_id"]
			}
			return next(ctx)
		},
	})
	t.Cleanup(func() { SetGlobalMiddlewares(nil) })

	worker := &worker{manager: manager}
	if result := worker.processJob(context.Background(), row, slog.Default()); result.err != nil {
		t.Fatalf("running the job: %v", result.err)
	}

	if seen != "req-7" {
		t.Errorf("the execution side read %q off the row, want the id stamped at dispatch", seen)
	}
}

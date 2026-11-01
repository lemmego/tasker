package scheduler

import (
	"context"
	"testing"

	"github.com/lemmego/tasker"
)

type dispatchContextDriver struct {
	tasker.Driver
	ctx context.Context
}

func (d *dispatchContextDriver) Enqueue(ctx context.Context, _ *tasker.JobRow) error {
	d.ctx = ctx
	return nil
}

func TestRegisterInvalidScheduleDoesNotPoisonJobID(t *testing.T) {
	s := New(tasker.NewManager())
	job := ScheduledJob{ID: "invalid", Schedule: "not a cron schedule"}

	if err := s.Register(job); err == nil {
		t.Fatal("Register returned nil for invalid schedule")
	}
	if got := len(s.List()); got != 0 {
		t.Fatalf("registered jobs = %d, want 0", got)
	}

	job.Schedule = "@every 1h"
	if err := s.Register(job); err != nil {
		t.Fatalf("registering corrected schedule: %v", err)
	}
}

func TestDispatchUsesSchedulerContext(t *testing.T) {
	driver := &dispatchContextDriver{}
	s := New(tasker.NewManager(tasker.WithDriver(driver)))
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	s.dispatchJob(ScheduledJob{ID: "job", Job: schedulerTestJob{}})
	if driver.ctx == nil {
		t.Fatal("scheduled job was not dispatched")
	}
	select {
	case <-driver.ctx.Done():
	default:
		t.Fatal("scheduled dispatch context was not cancelled")
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type schedulerTestJob struct{}

func (schedulerTestJob) Handle(context.Context) error { return nil }

type failingDriver struct {
	tasker.Driver
	err error
}

func (d *failingDriver) Enqueue(context.Context, *tasker.JobRow) error { return d.err }

func TestARunIsReportedToAListener(t *testing.T) {
	var seen []Run
	s := New(tasker.NewManager(tasker.WithDriver(&dispatchContextDriver{})))
	s.Observe(func(_ context.Context, r Run) { seen = append(seen, r) })

	s.dispatchJob(ScheduledJob{
		ID: "prune-observe-entries", Schedule: "20 4 * * *",
		Queue: "maintenance", Job: schedulerTestJob{},
	})

	if len(seen) != 1 {
		t.Fatalf("observed %d runs, want 1", len(seen))
	}
	run := seen[0]
	if run.ID != "prune-observe-entries" || run.Schedule != "20 4 * * *" {
		t.Errorf("run = %+v, want the registration it came from", run)
	}
	if run.Queue != "maintenance" {
		t.Errorf("queue = %q", run.Queue)
	}
	if run.Err != nil {
		t.Errorf("Err = %v for a successful dispatch", run.Err)
	}
	if run.At.IsZero() {
		t.Error("At is zero, so a schedule view cannot say when it fired")
	}
}

// A run that failed to enqueue is the case nothing else can report: there is
// no job row to look at, so without the listener the run did not happen as
// far as the queue is concerned.
func TestARunThatFailedToEnqueueIsReported(t *testing.T) {
	refused := errorString("the queue is unreachable")
	var seen []Run
	s := New(tasker.NewManager(tasker.WithDriver(&failingDriver{err: refused})))
	s.Observe(func(_ context.Context, r Run) { seen = append(seen, r) })

	s.dispatchJob(ScheduledJob{ID: "job", Schedule: "@every 1h", Job: schedulerTestJob{}})

	if len(seen) != 1 {
		t.Fatalf("observed %d runs, want 1 — a run that failed is the one worth seeing", len(seen))
	}
	if seen[0].Err == nil {
		t.Error("the dispatch error did not reach the listener")
	}
}

// The cron this scheduler builds has no recovery chain, so an unguarded panic
// on the firing goroutine takes down the process that runs every scheduled
// job in the deployment.
func TestAPanickingListenerDoesNotTakeDownTheScheduler(t *testing.T) {
	ran := false
	s := New(tasker.NewManager(tasker.WithDriver(&dispatchContextDriver{})))
	s.Observe(func(context.Context, Run) { panic("the listener is broken") })
	s.Observe(func(context.Context, Run) { ran = true })

	s.dispatchJob(ScheduledJob{ID: "job", Schedule: "@every 1h", Job: schedulerTestJob{}})

	if !ran {
		t.Error("a panic in the first listener cost the second its run")
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }

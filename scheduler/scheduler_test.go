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

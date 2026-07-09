package tasker

import (
	"context"
	"testing"
	"time"
)

func TestStateValid(t *testing.T) {
	tests := []struct {
		state State
		valid bool
	}{
		{StatePending, true},
		{StateScheduled, true},
		{StateAvailable, true},
		{StateRunning, true},
		{StateCompleted, true},
		{StateRetryable, true},
		{StateFailed, true},
		{StateCancelled, true},
		{State("invalid"), false},
	}

	for _, tt := range tests {
		got := tt.state.Valid()
		if got != tt.valid {
			t.Errorf("State(%q).Valid() = %v, want %v", tt.state, got, tt.valid)
		}
	}
}

func TestStateTerminal(t *testing.T) {
	tests := []struct {
		state    State
		terminal bool
	}{
		{StatePending, false},
		{StateScheduled, false},
		{StateAvailable, false},
		{StateRunning, false},
		{StateCompleted, true},
		{StateRetryable, false},
		{StateFailed, true},
		{StateCancelled, true},
	}

	for _, tt := range tests {
		got := tt.state.Terminal()
		if got != tt.terminal {
			t.Errorf("State(%q).Terminal() = %v, want %v", tt.state, got, tt.terminal)
		}
	}
}

func TestValidateTransition(t *testing.T) {
	tests := []struct {
		from State
		to   State
		ok   bool
	}{
		{StateAvailable, StateRunning, true},
		{StateRunning, StateCompleted, true},
		{StateRunning, StateRetryable, true},
		{StateRunning, StateFailed, true},
		{StateRetryable, StateAvailable, true},
		{StateCompleted, StateAvailable, true},
		{StateFailed, StateAvailable, true},
		{StateAvailable, StateCancelled, true},
		{StateScheduled, StateAvailable, true},
		{StatePending, StateAvailable, true},
		{StateRunning, StateCancelled, false},
		{StatePending, StateFailed, false},
		{StateCompleted, StateRunning, false},
		{StateCancelled, StateAvailable, false},
	}

	for _, tt := range tests {
		err := ValidateTransition(tt.from, tt.to)
		if tt.ok && err != nil {
			t.Errorf("ValidateTransition(%q, %q) = %v, want nil", tt.from, tt.to, err)
		}
		if !tt.ok && err == nil {
			t.Errorf("ValidateTransition(%q, %q) = nil, want error", tt.from, tt.to)
		}
	}
}

func TestComputeNextState(t *testing.T) {
	job := &JobRow{
		MaxAttempts: 3,
		Attempt:     1,
	}

	state, backoff := ComputeNextState(job, nil)
	if state != StateCompleted {
		t.Errorf("expected completed, got %s", state)
	}
	if backoff != 0 {
		t.Errorf("expected 0 backoff, got %s", backoff)
	}

	state, backoff = ComputeNextState(job, context.DeadlineExceeded)
	if state != StateRetryable {
		t.Errorf("expected retryable, got %s", state)
	}
	if backoff <= 0 {
		t.Errorf("expected positive backoff, got %s", backoff)
	}

	exhaustedJob := &JobRow{
		MaxAttempts: 3,
		Attempt:     3,
	}
	state, backoff = ComputeNextState(exhaustedJob, context.DeadlineExceeded)
	if state != StateFailed {
		t.Errorf("expected failed, got %s", state)
	}
	if backoff != 0 {
		t.Errorf("expected 0 backoff, got %s", backoff)
	}
}

func TestComputeBackoff(t *testing.T) {
	job := &JobRow{MaxAttempts: 5}

	backoff1 := computeBackoff(job, 1)
	if backoff1 < 0 {
		t.Errorf("expected positive backoff, got %s", backoff1)
	}

	backoff2 := computeBackoff(job, 2)
	if backoff2 <= backoff1 {
		t.Errorf("expected backoff to increase, got %s <= %s", backoff2, backoff1)
	}
}

type testJob struct {
	ID string
}

func (j *testJob) Handle(ctx context.Context) error {
	return nil
}

type failingJob struct{}

func (j *failingJob) Handle(ctx context.Context) error {
	return context.DeadlineExceeded
}

type failingJobWithFailed struct {
	called bool
}

func (j *failingJobWithFailed) Handle(ctx context.Context) error {
	return context.DeadlineExceeded
}

func (j *failingJobWithFailed) Failed(ctx context.Context, payload []byte, err error) error {
	j.called = true
	return nil
}

type jobWithHooks struct {
	beforeCalled bool
	afterCalled  bool
}

func (j *jobWithHooks) Handle(ctx context.Context) error {
	return nil
}

func (j *jobWithHooks) BeforeHandle(ctx context.Context) error {
	j.beforeCalled = true
	return nil
}

func (j *jobWithHooks) AfterHandle(ctx context.Context) error {
	j.afterCalled = true
	return nil
}

type queuedJob struct{}

func (j *queuedJob) Handle(ctx context.Context) error {
	return nil
}

func (j *queuedJob) Queue() QueueName {
	return "critical"
}

type taggedJob struct{}

func (j *taggedJob) Handle(ctx context.Context) error {
	return nil
}

func (j *taggedJob) Tags() []string {
	return []string{"email", "user:1"}
}

type delayedJob struct{}

func (j *delayedJob) Handle(ctx context.Context) error {
	return nil
}

func (j *delayedJob) Delay() time.Duration {
	return 5 * time.Minute
}

type retryableJob struct{}

func (j *retryableJob) Handle(ctx context.Context) error {
	return nil
}

func (j *retryableJob) MaxAttempts() int {
	return 10
}

func TestBuildJobRow(t *testing.T) {
	cfg := DefaultConfig()

	t.Run("basic job", func(t *testing.T) {
		row, err := buildJobRow(&testJob{ID: "1"}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if row.Queue != "default" {
			t.Errorf("expected default queue, got %s", row.Queue)
		}
		if row.MaxAttempts != 3 {
			t.Errorf("expected 3 max attempts, got %d", row.MaxAttempts)
		}
		if row.State != StateAvailable {
			t.Errorf("expected available state, got %s", row.State)
		}
	})

	t.Run("queued job", func(t *testing.T) {
		row, err := buildJobRow(&queuedJob{}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if row.Queue != "critical" {
			t.Errorf("expected critical queue, got %s", row.Queue)
		}
	})

	t.Run("tagged job", func(t *testing.T) {
		row, err := buildJobRow(&taggedJob{}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if len(row.Tags) != 2 || row.Tags[0] != "email" {
			t.Errorf("expected [email user:1] tags, got %v", row.Tags)
		}
	})

	t.Run("delayed job", func(t *testing.T) {
		row, err := buildJobRow(&delayedJob{}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if row.State != StateScheduled {
			t.Errorf("expected scheduled state for delayed job, got %s", row.State)
		}
		if !row.ScheduledAt.After(time.Now()) {
			t.Error("expected scheduled_at in the future")
		}
	})

	t.Run("retryable job", func(t *testing.T) {
		row, err := buildJobRow(&retryableJob{}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		if row.MaxAttempts != 10 {
			t.Errorf("expected 10 max attempts, got %d", row.MaxAttempts)
		}
	})

	t.Run("dispatch opts override", func(t *testing.T) {
		row, err := buildJobRow(&testJob{ID: "1"}, cfg,
			OnQueue("high"),
			WithDelay(10*time.Minute),
			WithPriority(1),
			WithTags("custom"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if row.Queue != "high" {
			t.Errorf("expected high queue, got %s", row.Queue)
		}
		if row.Priority != 1 {
			t.Errorf("expected priority 1, got %d", row.Priority)
		}
		if len(row.Tags) != 1 || row.Tags[0] != "custom" {
			t.Errorf("expected [custom] tags, got %v", row.Tags)
		}
	})
}

func TestFibonacci(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{1, 1},
		{2, 1},
		{3, 2},
		{4, 3},
		{5, 5},
		{6, 8},
		{7, 13},
	}

	for _, tt := range tests {
		got := fibonacci(tt.n)
		if got != tt.want {
			t.Errorf("fibonacci(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestJobRegistry(t *testing.T) {
	RegisterJob("*tasker.testJob", func() Job { return &testJob{} })

	_, ok := GetRegisteredJob("*tasker.testJob")
	if !ok {
		t.Error("expected registered job to be found")
	}

	jobs := ListRegisteredJobs()
	if len(jobs) == 0 {
		t.Error("expected at least one registered job")
	}
}

func TestGlobalMiddlewares(t *testing.T) {
	mw := func(ctx context.Context, job Job, payload []byte, next func(ctx context.Context) error) error {
		return next(ctx)
	}

	AddGlobalMiddleware(mw)

	mws := GetGlobalMiddlewares()
	if len(mws) != 1 {
		t.Errorf("expected 1 global middleware, got %d", len(mws))
	}

	SetGlobalMiddlewares(nil)
	mws = GetGlobalMiddlewares()
	if len(mws) != 0 {
		t.Errorf("expected 0 global middlewares, got %d", len(mws))
	}
}

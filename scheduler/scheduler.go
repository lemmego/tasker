package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/lemmego/tasker"
)

type ScheduledJob struct {
	ID       string
	Schedule string
	Job      tasker.Job
	Queue    tasker.QueueName
	Opts     []tasker.DispatchOpt
}

type Scheduler struct {
	mu        sync.Mutex
	manager   *tasker.Manager
	jobs      map[string]*ScheduledJob
	cron      *cron.Cron
	entryIDs  map[string]cron.EntryID
	running   bool
	cancel    context.CancelFunc
	ctx       context.Context
	listeners []Listener
}

// Run is one firing of a scheduled job.
//
// It is not the same thing as the enqueue it causes, which is why it is
// reported separately. A tasker.DispatchMiddleware sees the job go onto the
// queue and cannot tell a scheduled run from a job queued by a request —
// and the questions asked of a schedule are about the schedule: did 4:20
// fire, did it fire twice, how long has it been failing. A row on the queue
// answers none of those, and a run that failed to enqueue leaves no row at
// all.
type Run struct {
	// ID and Schedule are the registration, so a run can be matched to the
	// entry that caused it rather than to a job kind that several entries
	// might share.
	ID       string
	Schedule string
	Queue    tasker.QueueName

	// Job is the value that was dispatched, for an observer that wants to
	// name it. It is the registered instance, shared across runs: read it,
	// do not write to it.
	Job tasker.Job

	At       time.Time
	Duration time.Duration

	// Err is the dispatch error, if the run failed to enqueue. This is the
	// case nothing else can report: there is no job row to look at, so
	// without this the run simply did not happen as far as the queue is
	// concerned.
	Err error
}

// Listener is told about every scheduled run, after it has been dispatched
// or failed to be.
//
// It runs on the cron goroutine that fired the entry, so a slow listener
// delays the next run of that entry. A panic is contained: this scheduler's
// cron is built without a recovery chain, so a panic here would otherwise
// take down the process that runs every scheduled job in the deployment.
type Listener func(ctx context.Context, r Run)

// Observe registers a listener. Additive.
//
// Per-scheduler rather than process-wide, unlike tasker's own middleware
// registries: a deployment runs one scheduler in one process by design, so
// there is nothing for a global to reach that this does not.
func (s *Scheduler) Observe(l Listener) {
	if l == nil {
		return
	}
	s.mu.Lock()
	s.listeners = append(s.listeners, l)
	s.mu.Unlock()
}

func New(mgr *tasker.Manager) *Scheduler {
	return &Scheduler{
		manager:  mgr,
		jobs:     make(map[string]*ScheduledJob),
		cron:     cron.New(cron.WithParser(cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor))),
		entryIDs: make(map[string]cron.EntryID),
	}
}

func (s *Scheduler) Register(job ScheduledJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[job.ID]; exists {
		return nil
	}

	s.jobs[job.ID] = &job

	entryID, err := s.cron.AddFunc(job.Schedule, func() {
		s.dispatchJob(job)
	})
	if err != nil {
		delete(s.jobs, job.ID)
		return err
	}

	s.entryIDs[job.ID] = entryID
	slog.Info("scheduled job registered", "id", job.ID, "schedule", job.Schedule)
	return nil
}

func (s *Scheduler) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entryID, ok := s.entryIDs[id]; ok {
		s.cron.Remove(entryID)
		delete(s.entryIDs, id)
		delete(s.jobs, id)
	}
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.running = true
	s.cron.Start()

	slog.Info("scheduler started", "jobs", len(s.jobs))
	return nil
}

func (s *Scheduler) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	s.cancel()
	stopped := s.cron.Stop()
	select {
	case <-stopped.Done():
	case <-ctx.Done():
		return ctx.Err()
	}

	slog.Info("scheduler stopped")
	return nil
}

func (s *Scheduler) List() []ScheduledJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs := make([]ScheduledJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, *j)
	}
	return jobs
}

func (s *Scheduler) dispatchJob(job ScheduledJob) {
	s.mu.Lock()
	ctx := s.ctx
	listeners := s.listeners
	s.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	opts := append([]tasker.DispatchOpt{
		tasker.OnQueue(job.Queue),
	}, job.Opts...)

	started := time.Now()
	_, err := s.manager.Dispatch(ctx, job.Job, opts...)
	if err != nil {
		slog.Error("failed to dispatch scheduled job", "id", job.ID, "error", err)
	}

	if len(listeners) == 0 {
		return
	}
	run := Run{
		ID:       job.ID,
		Schedule: job.Schedule,
		Queue:    job.Queue,
		Job:      job.Job,
		At:       started,
		Duration: time.Since(started),
		Err:      err,
	}
	for _, listen := range listeners {
		deliver(ctx, listen, run)
	}
}

// deliver isolates a listener's panic.
//
// The cron this scheduler builds has no recovery chain, so a panic on the
// firing goroutine takes down the process that runs every scheduled job in
// the deployment. Something watching the schedule must not be able to stop
// it.
func deliver(ctx context.Context, listen Listener, run Run) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.ErrorContext(ctx, "scheduler: a run listener panicked",
				"id", run.ID, "panic", recovered)
		}
	}()
	listen(ctx, run)
}

package scheduler

import (
	"context"
	"log/slog"
	"sync"

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
	mu       sync.Mutex
	manager  *tasker.Manager
	jobs     map[string]*ScheduledJob
	cron     *cron.Cron
	entryIDs map[string]cron.EntryID
	running  bool
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
	ctx = s.cron.Stop()
	<-ctx.Done()

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
	ctx := context.Background()
	opts := append([]tasker.DispatchOpt{
		tasker.OnQueue(job.Queue),
	}, job.Opts...)

	if _, err := s.manager.Dispatch(ctx, job.Job, opts...); err != nil {
		slog.Error("failed to dispatch scheduled job", "id", job.ID, "error", err)
	}
}

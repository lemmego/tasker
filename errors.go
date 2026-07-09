package tasker

import "errors"

var (
	ErrJobNotFound        = errors.New("job not found")
	ErrJobAlreadyExists   = errors.New("job already exists")
	ErrInvalidTransition  = errors.New("invalid state transition")
	ErrJobNotClaimed      = errors.New("job not claimed by this worker")
	ErrQueueNotFound      = errors.New("queue not found")
	ErrWorkerPoolStopped  = errors.New("worker pool is stopped")
	ErrInvalidPayload     = errors.New("invalid job payload")
	ErrJobTimedOut        = errors.New("job execution timed out")
	ErrJobPanicked        = errors.New("job execution panicked")
	ErrMaxRetriesExceeded = errors.New("max retries exceeded")
)

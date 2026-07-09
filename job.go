package tasker

import (
	"context"
	"time"
)

type Job interface {
	Handle(ctx context.Context) error
}

type ShouldQueue interface {
	Queue() QueueName
}

type ShouldDelay interface {
	Delay() time.Duration
}

type ShouldRetry interface {
	MaxAttempts() int
}

type ShouldRetryWithBackoff interface {
	ShouldRetry
	RetryBackoff() BackoffConfig
}

type ShouldRetryUntil interface {
	RetryUntil() time.Time
}

type ShouldFail interface {
	Failed(ctx context.Context, payload []byte, err error) error
}

type ShouldTag interface {
	Tags() []string
}

type ShouldBatch interface {
	BatchID() string
}

type ShouldLock interface {
	LockKey() string
	LockTimeout() time.Duration
}

type ShouldTimeout interface {
	Timeout() time.Duration
}

type ShouldMiddleware interface {
	Middleware() []JobMiddleware
}

type BeforeHandleHook interface {
	BeforeHandle(ctx context.Context) error
}

type AfterHandleHook interface {
	AfterHandle(ctx context.Context) error
}

type BeforeRetryHook interface {
	BeforeRetry(ctx context.Context, attempt int, err error) error
}

type BackoffStrategy string

const (
	BackoffExponential BackoffStrategy = "exponential"
	BackoffFibonacci   BackoffStrategy = "fibonacci"
	BackoffFixed       BackoffStrategy = "fixed"
)

type BackoffConfig struct {
	Strategy BackoffStrategy `json:"strategy"`
	Base     time.Duration   `json:"base"`
	MaxDelay time.Duration   `json:"max_delay"`
}

func DefaultBackoff() BackoffConfig {
	return BackoffConfig{
		Strategy: BackoffExponential,
		Base:     2 * time.Second,
		MaxDelay: 24 * time.Hour,
	}
}

func FibonacciBackoff() BackoffConfig {
	return BackoffConfig{
		Strategy: BackoffFibonacci,
		Base:     1 * time.Second,
		MaxDelay: 24 * time.Hour,
	}
}

type DispatchOpt func(*dispatchOptions)

type dispatchOptions struct {
	queue    QueueName
	delay    time.Duration
	priority int
	batchID  string
	tags     []string
	metadata map[string]string
}

func OnQueue(name QueueName) DispatchOpt {
	return func(o *dispatchOptions) {
		o.queue = name
	}
}

func WithDelay(d time.Duration) DispatchOpt {
	return func(o *dispatchOptions) {
		o.delay = d
	}
}

func WithPriority(p int) DispatchOpt {
	return func(o *dispatchOptions) {
		o.priority = p
	}
}

func WithBatchID(id string) DispatchOpt {
	return func(o *dispatchOptions) {
		o.batchID = id
	}
}

func WithTags(tags ...string) DispatchOpt {
	return func(o *dispatchOptions) {
		o.tags = tags
	}
}

func WithMetadata(k, v string) DispatchOpt {
	return func(o *dispatchOptions) {
		if o.metadata == nil {
			o.metadata = make(map[string]string)
		}
		o.metadata[k] = v
	}
}

type JobMiddleware func(ctx context.Context, job Job, payload []byte, next func(ctx context.Context) error) error

package tasker

import (
	"fmt"
	"time"
)

type Transition struct {
	From State
	To   State
}

func (t Transition) Valid() bool {
	validTransitions := map[State][]State{
		StatePending:   {StateAvailable, StateCancelled},
		StateScheduled: {StateAvailable, StateCancelled},
		StateAvailable: {StateRunning, StateCancelled},
		StateRunning:   {StateCompleted, StateRetryable, StateFailed},
		StateRetryable: {StateAvailable, StateCancelled},
		StateCompleted: {StateAvailable},
		StateFailed:    {StateAvailable},
		StateCancelled: {},
	}

	allowed, ok := validTransitions[t.From]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == t.To {
			return true
		}
	}
	return false
}

func ComputeNextState(job *JobRow, err error) (State, time.Duration) {
	if err == nil {
		return StateCompleted, 0
	}

	maxAttempts := job.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	nextAttempt := job.Attempt + 1

	if nextAttempt > maxAttempts {
		return StateFailed, 0
	}

	backoff := computeBackoff(job, nextAttempt)
	return StateRetryable, backoff
}

func computeBackoff(job *JobRow, nextAttempt int) time.Duration {
	cfg := DefaultBackoff()
	maxDelay := cfg.MaxDelay
	base := cfg.Base

	switch cfg.Strategy {
	case BackoffExponential:
		delay := base * (1 << (nextAttempt - 1))
		if delay > maxDelay {
			delay = maxDelay
		}
		return delay
	case BackoffFibonacci:
		fib := fibonacci(nextAttempt)
		delay := time.Duration(fib) * base
		if delay > maxDelay {
			delay = maxDelay
		}
		return delay
	case BackoffFixed:
		if base > maxDelay {
			return maxDelay
		}
		return base
	default:
		delay := base * (1 << (nextAttempt - 1))
		if delay > maxDelay {
			delay = maxDelay
		}
		return delay
	}
}

func fibonacci(n int) int {
	if n <= 1 {
		return n
	}
	a, b := 0, 1
	for i := 2; i <= n; i++ {
		a, b = b, a+b
	}
	return b
}

func ValidateTransition(from, to State) error {
	t := Transition{From: from, To: to}
	if !t.Valid() {
		return fmt.Errorf("invalid state transition: %s -> %s", from, to)
	}
	return nil
}

func ShouldRequeue(nodeTimeout time.Duration) bool {
	return nodeTimeout > 0
}

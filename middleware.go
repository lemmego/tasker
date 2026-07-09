package tasker

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func LoggingMiddleware() JobMiddleware {
	return func(ctx context.Context, job Job, payload []byte, next func(ctx context.Context) error) error {
		start := time.Now()
		slog.Debug("job started", "kind", fmt.Sprintf("%T", job))

		err := next(ctx)

		if err != nil {
			slog.Warn("job failed", "kind", fmt.Sprintf("%T", job),
				"duration", time.Since(start), "error", err)
		} else {
			slog.Debug("job completed", "kind", fmt.Sprintf("%T", job),
				"duration", time.Since(start))
		}

		return err
	}
}

func RecoveryMiddleware() JobMiddleware {
	return func(ctx context.Context, job Job, payload []byte, next func(ctx context.Context) error) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic recovered: %v", r)
			}
		}()
		return next(ctx)
	}
}

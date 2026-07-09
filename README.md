# Tasker — Background Job System for Lemmego

**tasker** is a distributed, clusterable, multi-node background job management system for Go. It powers the **queue** plugin in the Lemmego framework.

## Features

- **State machine**: `pending → scheduled → available → running → completed / retryable / failed / cancelled`
- **Job hooks**: `BeforeHandle`, `AfterHandle`, `BeforeRetry`, `Failed` (failure callback)
- **Retry strategies**: Exponential, Fibonacci, Fixed backoff with configurable max attempts
- **Multi-node**: Leader election, node registry, heartbeats, distributed locking, stale job re-queue
- **SQL driver**: PostgreSQL, MySQL, SQLite support with dialect-aware queries
- **Redis driver**: Node management, leader election, distributed locking
- **Atomic claiming**: `SELECT ... FOR UPDATE SKIP LOCKED` (Postgres/MySQL), transactional (SQLite)
- **Multiple queues**: Independent worker pools per queue with priority ordering
- **Auto-scaling**: Horizon-style dynamic worker scaling based on queue backlog
- **Web dashboard**: Real-time stats, job management, queue controls, worker monitoring
- **SSE events**: Live stat updates via Server-Sent Events
- **CLI commands**: Work, dispatch, job management

## Architecture

```
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│   Node A     │  │   Node B     │  │   Node C     │
│  (Server)    │  │  (Worker)    │  │  (Worker)    │
│  Supervisor  │  │  Supervisor  │  │  Supervisor  │
│  └─Pool (3)  │  │  └─Pool (5)  │  │  └─Pool (10) │
│  └─Web UI    │  │              │  │              │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       │                 │                 │
       └─────────────┬───┴─────────────┬───┘
                     │                 │
             ┌───────▼─────────┐ ┌─────▼─────────┐
             │  PostgreSQL/SQL │ │   Redis        │
             │  (jobs table)   │ │ (pub/sub,      │
             │  SELECT FOR     │ │  locks,        │
             │  UPDATE SKIP    │ │  heartbeat)    │
             │  LOCKED         │ │                │
             └─────────────────┘ └───────────────┘
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/lemmego/tasker"
    "github.com/lemmego/tasker/driver/sqldriver"
)

type SendEmail struct {
    Email string `json:"email"`
    UserID int   `json:"user_id"`
}

func (j *SendEmail) Handle(ctx context.Context) error {
    fmt.Printf("Sending email to %s\n", j.Email)
    return nil
}

func main() {
    driver, _ := sqldriver.NewDriver(sqldriver.Config{
        DSN:        "postgres://localhost:5432/mydb?sslmode=disable",
        DriverName: "postgres",
    })

    mgr := tasker.NewConfiguredManager(tasker.Config{
        Driver:             driver,
        DefaultQueue:       "default",
        DefaultMaxAttempts: 3,
    })
    tasker.SetGlobal(mgr)

    tasker.RegisterJob("*main.SendEmail", func() tasker.Job {
        return &SendEmail{}
    })

    tasker.Dispatch(context.Background(), &SendEmail{
        Email:  "user@test.com",
        UserID: 1,
    })
}
```

## Package Structure

```
tasker/
├── tasker.go           # Manager — public API entry point
├── types.go            # Core types: JobRow, State, JobFilter, QueueStats, etc.
├── job.go              # Job interface + optional interfaces
├── driver.go           # Driver interface — implemented by sqldriver/redisdriver
├── state_machine.go    # State transitions + backoff computation
├── pool.go             # Pool + Worker — job claiming and execution
├── registry.go         # Job type registry + global middlewares
├── middleware.go       # Built-in middleware (logging, recovery)
├── provider.go         # Config struct + NewConfiguredManager
├── errors.go           # Sentinel errors
│
├── driver/sqldriver/   # SQL driver (Postgres, MySQL, SQLite)
│   ├── driver.go       #    Full implementation — enqueue, claim, complete, stats, etc.
│   ├── dialect.go      #    Dialect abstraction for Postgres/MySQL/SQLite
│   └── migrations/     #    SQL migration files
│
├── driver/redisdriver/ # Redis driver (node mgmt, leader election, locks)
│   └── driver.go
│
├── supervisor/         # Process lifecycle + auto-scaling
│   └── supervisor.go
│
├── coordinator/        # Multi-node coordination
│   └── coordinator.go
│
├── scheduler/          # Cron-based recurring jobs
│   └── scheduler.go
│
├── batch/              # Batch operations
│   └── batch.go
│
├── web/                # Web UI dashboard
│   ├── web.go          #    HTTP handlers + API routes
│   └── dashboard.go    #    Dashboard HTML template
│
└── cmd/                # CLI commands
    ├── work.go         #    tasker:work
    ├── queue.go        #    tasker:queue
    └── job.go          #    tasker:job
```

## Job Interface

```go
// Required — every job must implement Handle
type Job interface {
    Handle(ctx context.Context) error
}

// Optional interfaces:
type ShouldQueue interface { Queue() QueueName }
type ShouldDelay interface { Delay() time.Duration }
type ShouldRetry interface { MaxAttempts() int }
type ShouldFail interface { Failed(ctx, payload, err) error }
type ShouldTag interface { Tags() []string }
type ShouldBatch interface { BatchID() string }
type ShouldLock interface { LockKey() string; LockTimeout() time.Duration }
type ShouldTimeout interface { Timeout() time.Duration }
type ShouldMiddleware interface { Middleware() []JobMiddleware }

// Lifecycle hooks:
type BeforeHandleHook interface { BeforeHandle(ctx) error }
type AfterHandleHook interface { AfterHandle(ctx) error }
type BeforeRetryHook interface { BeforeRetry(ctx, attempt, err) error }
```

## Job States

```
pending ──► available ──► running ──► completed
                 ▲            │
                 │            ├──► retryable ──► available (backoff)
                 │            │
                 │            └──► failed (max attempts)
                 │
                 └── cancelled
```

## Driver Configuration

### SQL (Postgres / MySQL / SQLite)

```go
sqldriver.Config{
    DSN:          "postgres://localhost:5432/mydb?sslmode=disable",
    DriverName:   "postgres",
    TablePrefix:  "tasker_",
    MaxOpenConns: 25,
    MaxIdleConns: 10,
}
```

### Redis

```go
redisdriver.Config{
    Addr:      "localhost:6379",
    Password:  "",
    DB:        0,
    KeyPrefix: "tasker:",
}
```

## Web UI API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/tasker/` | Dashboard HTML |
| `GET` | `/tasker/api/stats` | Global stats |
| `GET` | `/tasker/api/jobs` | Paginated job list |
| `GET` | `/tasker/api/jobs/{id}` | Job detail |
| `POST` | `/tasker/api/jobs/{id}/retry` | Retry a job |
| `POST` | `/tasker/api/jobs/{id}/cancel` | Cancel a job |
| `POST` | `/tasker/api/jobs/batch/retry` | Batch retry |
| `POST` | `/tasker/api/jobs/batch/cancel` | Batch cancel |
| `GET` | `/tasker/api/queues` | Queue metrics |
| `POST` | `/tasker/api/queues/{queue}/pause` | Pause a queue |
| `POST` | `/tasker/api/queues/{queue}/resume` | Resume a queue |
| `GET` | `/tasker/api/workers` | Connected nodes |
| `GET` | `/tasker/api/metrics/jobs` | Per-job-type metrics |
| `GET` | `/tasker/api/metrics/queues` | Per-queue metrics |
| `GET` | `/tasker/api/events` | SSE real-time events |
| `POST` | `/tasker/api/prune` | Prune old jobs |

## CLI Commands

```bash
tasker:work             # Start worker process
  --queue=default,email #   Queues to listen on
  --workers=3           #   Workers per queue

tasker:dispatch         # Dispatch test jobs
  --count=10            #   Number of jobs
  --queue=default       #   Target queue
  --fail-rate=30        #   % chance of failure
  --delay=500           #   Simulated processing delay (ms)

tasker:job retry <id>   # Retry a specific job
tasker:job cancel <id>  # Cancel a specific job
tasker:job clear        # Clear jobs by state
  --states=failed,cancelled

tasker:queue list       # List queue stats
```

## Running Tests

```bash
# Core tests
go test ./...

# SQL driver tests (uses SQLite in-memory)
go test ./driver/sqldriver/...

# With race detector
go test -race ./...
```

## License

MIT

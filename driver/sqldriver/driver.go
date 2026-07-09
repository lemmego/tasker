package sqldriver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lemmego/tasker"
)

type Config struct {
	DSN             string
	DriverName      string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	TablePrefix     string
}

func DefaultConfig() Config {
	return Config{
		DriverName:   "postgres",
		MaxOpenConns: 25,
		MaxIdleConns: 10,
		TablePrefix:  "tasker_",
	}
}

type Driver struct {
	mu      sync.RWMutex
	db      *sql.DB
	config  Config
	dialect Dialect
}

func NewDriver(cfg Config) (*Driver, error) {
	db, err := sql.Open(cfg.DriverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	dialect := dialectFor(cfg.DriverName)

	return &Driver{
		db:      db,
		config:  cfg,
		dialect: dialect,
	}, nil
}

func dialectFor(driverName string) Dialect {
	switch driverName {
	case "postgres", "pgx":
		return &postgresDialect{}
	case "mysql", "mariadb":
		return &mysqlDialect{}
	case "sqlite3", "sqlite", "modernc.org/sqlite":
		return &sqliteDialect{}
	default:
		return &postgresDialect{}
	}
}

func (d *Driver) table(name string) string {
	return d.config.TablePrefix + name
}

func (d *Driver) nowSQL() string {
	return d.dialect.Placeholder(1)
}

func (d *Driver) nowArgs(n int) []interface{} {
	args := make([]interface{}, n)
	now := time.Now()
	for i := range args {
		args[i] = now
	}
	return args
}

func (d *Driver) nowArgsList() []interface{} {
	return []interface{}{time.Now()}
}

func (d *Driver) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}

func (d *Driver) Close() error {
	return d.db.Close()
}

func (d *Driver) Migrate(ctx context.Context) error {
	t := d.dialect.ArrayType()
	schema := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		uuid        TEXT NOT NULL UNIQUE,
		queue       TEXT NOT NULL DEFAULT 'default',
		kind        TEXT NOT NULL,
		payload     BLOB NOT NULL,
		state       TEXT NOT NULL DEFAULT 'available',
		priority    INTEGER NOT NULL DEFAULT 0,
		attempt     INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		attempted_by %s NOT NULL DEFAULT '{}',
		attempted_at TIMESTAMP,
		errors      BLOB NOT NULL DEFAULT '[]',
		output      BLOB,
		tags        %s NOT NULL DEFAULT '{}',
		scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		started_at  TIMESTAMP,
		completed_at TIMESTAMP,
		finalized_at TIMESTAMP,
		node_id     TEXT,
		batch_id    TEXT,
		timeout     INTEGER NOT NULL DEFAULT 0,
		metadata    BLOB NOT NULL DEFAULT '{}',
		unique_key  TEXT
	);

	CREATE INDEX IF NOT EXISTS %s ON %s (queue, state, scheduled_at, priority DESC, id);
	CREATE INDEX IF NOT EXISTS %s ON %s (state);
	CREATE INDEX IF NOT EXISTS %s ON %s (kind);
	CREATE INDEX IF NOT EXISTS %s ON %s (batch_id);
	CREATE INDEX IF NOT EXISTS %s ON %s (node_id);
	CREATE INDEX IF NOT EXISTS %s ON %s (scheduled_at);
	CREATE INDEX IF NOT EXISTS %s ON %s (unique_key);

	CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY CHECK (id = 1),
		node_id     TEXT NOT NULL DEFAULT '',
		host        TEXT NOT NULL DEFAULT '',
		port        INTEGER NOT NULL DEFAULT 0,
		status      TEXT NOT NULL DEFAULT 'inactive',
		started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		version     TEXT NOT NULL DEFAULT '1.0.0'
	);

	CREATE TABLE IF NOT EXISTS %s (
		node_id     TEXT PRIMARY KEY,
		host        TEXT NOT NULL,
		port        INTEGER NOT NULL DEFAULT 0,
		queues      %s NOT NULL DEFAULT '{}',
		workers     INTEGER NOT NULL DEFAULT 0,
		status      TEXT NOT NULL DEFAULT 'active',
		started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		version     TEXT NOT NULL DEFAULT '1.0.0'
	);

	CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		uuid        TEXT NOT NULL,
		queue       TEXT NOT NULL,
		kind        TEXT NOT NULL,
		payload     BLOB NOT NULL,
		attempt     INTEGER NOT NULL,
		max_attempts INTEGER NOT NULL,
		errors      BLOB NOT NULL,
		failed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		tags        %s NOT NULL DEFAULT '{}'
	);

	CREATE TABLE IF NOT EXISTS %s (
		lock_key    TEXT PRIMARY KEY,
		node_id     TEXT NOT NULL,
		expires_at  TIMESTAMP NOT NULL
	);`,
		d.table("jobs"), t, t,
		fmt.Sprintf("idx_%s_queue_state", d.config.TablePrefix), d.table("jobs"),
		fmt.Sprintf("idx_%s_state", d.config.TablePrefix), d.table("jobs"),
		fmt.Sprintf("idx_%s_kind", d.config.TablePrefix), d.table("jobs"),
		fmt.Sprintf("idx_%s_batch_id", d.config.TablePrefix), d.table("jobs"),
		fmt.Sprintf("idx_%s_node_id", d.config.TablePrefix), d.table("jobs"),
		fmt.Sprintf("idx_%s_scheduled_at", d.config.TablePrefix), d.table("jobs"),
		fmt.Sprintf("idx_%s_unique_key", d.config.TablePrefix), d.table("jobs"),
		d.table("leader"),
		d.table("nodes"), t,
		d.table("failed_jobs"), t,
		d.table("locks"))

	_, err := d.db.ExecContext(ctx, schema)
	if err != nil {
		return err
	}

	if _, ok := d.dialect.(*sqliteDialect); ok {
		d.db.ExecContext(ctx, "PRAGMA journal_mode=WAL")
		d.db.ExecContext(ctx, "PRAGMA busy_timeout=5000")
	}
	return nil
}

func (d *Driver) Enqueue(ctx context.Context, job *tasker.JobRow) error {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		INSERT INTO %s (uuid, queue, kind, payload, state, priority, attempt, max_attempts,
		                tags, scheduled_at, created_at, batch_id, timeout, metadata)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		RETURNING id`,
		d.table("jobs"),
		p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8),
		p(9), p(10), p(11), p(12), p(13), p(14))

	payload := job.Payload
	tags := encodeTextArray(job.Tags)
	metadata := job.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metaJSON, _ := json.Marshal(metadata)

	return d.db.QueryRowContext(ctx, query,
		job.UUID, string(job.Queue), job.Kind, payload, string(job.State),
		job.Priority, job.Attempt, job.MaxAttempts,
		tags, job.ScheduledAt, job.CreatedAt,
		job.BatchID, int64(job.Timeout), metaJSON,
	).Scan(&job.ID)
}

func (d *Driver) EnqueueBatch(ctx context.Context, jobs []*tasker.JobRow) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		INSERT INTO %s (uuid, queue, kind, payload, state, priority, attempt, max_attempts,
		                tags, scheduled_at, created_at, batch_id, timeout, metadata)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
		RETURNING id`,
		d.table("jobs"),
		p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8),
		p(9), p(10), p(11), p(12), p(13), p(14))

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, job := range jobs {
		tags := encodeTextArray(job.Tags)
		metadata := job.Metadata
		if metadata == nil {
			metadata = map[string]string{}
		}
		metaJSON, _ := json.Marshal(metadata)

		err := stmt.QueryRowContext(ctx,
			job.UUID, string(job.Queue), job.Kind, job.Payload, string(job.State),
			job.Priority, job.Attempt, job.MaxAttempts,
			tags, job.ScheduledAt, job.CreatedAt,
			job.BatchID, int64(job.Timeout), metaJSON,
		).Scan(&job.ID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Driver) Claim(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	if d.dialect.SupportsSkipLocked() {
		return d.claimSkipLocked(ctx, queue, nodeID, max)
	}
	return d.claimTransaction(ctx, queue, nodeID, max)
}

func (d *Driver) claimSkipLocked(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	nv := d.dialect.Now()
	nowSQL := nv.SQL
	if nowSQL == "" {
		nowSQL = d.dialect.Placeholder(1)
	}
	query := fmt.Sprintf(`
		WITH locked AS (
			SELECT id FROM %s
			WHERE queue = $1
			  AND state = 'available'
			  AND scheduled_at <= %s
			ORDER BY priority DESC, scheduled_at ASC, id ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s j
		SET state = 'running',
		    attempt = j.attempt + 1,
		    attempted_at = %s,
		    started_at = COALESCE(j.started_at, %s),
		    node_id = $3,
		    attempted_by = array_append(
		        CASE WHEN array_length(j.attempted_by, 1) >= 50
		             THEN j.attempted_by[array_length(j.attempted_by, 1) - 48:]
		             ELSE j.attempted_by
		        END,
		        $3::TEXT
		    )
		FROM locked l
		WHERE j.id = l.id
		RETURNING j.id, j.uuid, j.queue, j.kind, j.payload, j.state, j.priority,
		          j.attempt, j.max_attempts, j.attempted_by, j.attempted_at,
		          j.errors, j.output, j.tags, j.scheduled_at, j.created_at,
		          j.started_at, j.completed_at, j.finalized_at, j.node_id,
		          j.batch_id, j.timeout, j.metadata, j.unique_key`,
		d.table("jobs"), nowSQL, d.table("jobs"), nowSQL, nowSQL)

	args := []interface{}{string(queue), max, string(nodeID)}
	if nv.Value != nil {
		args = append(args, nv.Value, nv.Value, nv.Value)
	}
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to claim jobs: %w", err)
	}
	defer rows.Close()

	return scanJobs(rows)
}

func (d *Driver) claimTransaction(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	p := d.dialect.Placeholder
	nowSQL := d.nowSQL()
	nowTime := time.Now()

	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'running',
		    attempt = attempt + 1,
		    attempted_at = %s,
		    started_at = COALESCE(started_at, %s),
		    node_id = %s,
		    attempted_by = %s
		WHERE id IN (
			SELECT id FROM %s
			WHERE queue = %s AND state = 'available' AND scheduled_at <= %s
			ORDER BY priority DESC, scheduled_at ASC, id ASC
			LIMIT %s
		)
		RETURNING id, uuid, queue, kind, payload, state, priority,
		          attempt, max_attempts, attempted_by, attempted_at,
		          errors, output, tags, scheduled_at, created_at,
		          started_at, completed_at, finalized_at, node_id,
		          batch_id, timeout, metadata, unique_key`,
		d.table("jobs"), nowSQL, nowSQL, p(1), p(2),
		d.table("jobs"), p(3), nowSQL, p(4))

	rows, err := d.db.QueryContext(ctx, query,
		nowTime, nowTime, string(nodeID), encodeTextArray([]string{string(nodeID)}),
		string(queue), nowTime, max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanJobs(rows)
}

func (d *Driver) Complete(ctx context.Context, id tasker.JobID, output []byte) error {
	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'completed', output = %s, completed_at = %s, finalized_at = %s
		WHERE id = %s AND state = 'running'`,
		d.table("jobs"), p(2), now, now, p(1))

	result, err := d.db.ExecContext(ctx, query, output, time.Now(), time.Now(), id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return tasker.ErrJobNotFound
	}
	return nil
}

func (d *Driver) Fail(ctx context.Context, id tasker.JobID, jobErr error) error {
	p := d.dialect.Placeholder
	errEntry := tasker.AttemptError{
		Error:     jobErr.Error(),
		Timestamp: time.Now(),
	}
	errJSON, _ := json.Marshal([]tasker.AttemptError{errEntry})

	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'failed',
		    errors = CASE WHEN errors IS NULL OR errors = '' THEN %s
		                 ELSE json_patch(errors, %s) END,
		    finalized_at = %s,
		    completed_at = %s
		WHERE id = %s AND state IN ('running', 'retryable')`,
		d.table("jobs"), p(2), p(2), d.nowSQL(), d.nowSQL(), p(1))

	_, err := d.db.ExecContext(ctx, query, errJSON, errJSON, time.Now(), time.Now(), id)
	if err != nil {
		return err
	}

	d.insertFailedJob(ctx, id)
	return nil
}

func (d *Driver) Retry(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'available', scheduled_at = %s, node_id = NULL, attempt = 0,
		    errors = '%s', started_at = NULL, completed_at = NULL,
		    finalized_at = NULL, output = NULL
		WHERE id = %s AND state IN ('failed', 'completed', 'cancelled', 'retryable')
		RETURNING *`, d.table("jobs"), now, "[]", p(1))

	return scanJob(d.db.QueryRowContext(ctx, query, time.Now(), id))
}

func (d *Driver) RetryBatch(ctx context.Context, ids []tasker.JobID) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'available', scheduled_at = %s, node_id = NULL, attempt = 0,
		    errors = '%s', started_at = NULL, completed_at = NULL,
		    finalized_at = NULL, output = NULL
		WHERE id = %s AND state IN ('failed', 'completed', 'cancelled', 'retryable')`,
		d.table("jobs"), now, "[]", p(1))

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, time.Now(), id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Driver) Cancel(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'cancelled', finalized_at = %s
		WHERE id = %s AND state IN ('available', 'pending', 'scheduled', 'retryable')
		RETURNING *`, d.table("jobs"), now, p(1))

	return scanJob(d.db.QueryRowContext(ctx, query, time.Now(), id))
}

func (d *Driver) CancelBatch(ctx context.Context, ids []tasker.JobID) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'cancelled', finalized_at = %s
		WHERE id = %s AND state IN ('available', 'pending', 'scheduled', 'retryable')`,
		d.table("jobs"), now, p(1))

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Driver) GetByID(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	query := fmt.Sprintf(`SELECT * FROM %s WHERE id = %s`, d.table("jobs"), d.dialect.Placeholder(1))
	return scanJob(d.db.QueryRowContext(ctx, query, id))
}

func (d *Driver) QueryJobs(ctx context.Context, filter tasker.JobFilter) ([]*tasker.JobRow, int64, error) {
	var conditions []string
	var args []interface{}
	argIdx := 1
	p := d.dialect.Placeholder

	if len(filter.States) > 0 {
		placeholders := make([]string, len(filter.States))
		for i, s := range filter.States {
			placeholders[i] = p(argIdx)
			args = append(args, string(s))
			argIdx++
		}
		conditions = append(conditions, fmt.Sprintf("state IN (%s)", strings.Join(placeholders, ",")))
	}

	if len(filter.Queues) > 0 {
		placeholders := make([]string, len(filter.Queues))
		for i, q := range filter.Queues {
			placeholders[i] = p(argIdx)
			args = append(args, string(q))
			argIdx++
		}
		conditions = append(conditions, fmt.Sprintf("queue IN (%s)", strings.Join(placeholders, ",")))
	}

	if len(filter.Kinds) > 0 {
		placeholders := make([]string, len(filter.Kinds))
		for i, k := range filter.Kinds {
			placeholders[i] = p(argIdx)
			args = append(args, k)
			argIdx++
		}
		conditions = append(conditions, fmt.Sprintf("kind IN (%s)", strings.Join(placeholders, ",")))
	}

	if filter.Search != "" {
		conditions = append(conditions, fmt.Sprintf("(kind %s %s OR uuid::TEXT %s %s)",
			d.dialect.ILike(), p(argIdx), d.dialect.ILike(), p(argIdx)))
		args = append(args, "%"+filter.Search+"%")
		argIdx++
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", d.table("jobs"), whereClause)
	var total int64
	if err := d.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	if filter.Limit <= 0 {
		filter.Limit = 50
	}
	if filter.Offset < 0 {
		filter.Offset = 0
	}

	orderBy := fmt.Sprintf("created_at DESC")
	if filter.OrderBy != "" {
		dir := "DESC"
		if filter.Order == "asc" {
			dir = "ASC"
		}
		orderBy = fmt.Sprintf("%s %s", filter.OrderBy, dir)
	}

	dataQuery := fmt.Sprintf(`SELECT * FROM %s %s ORDER BY %s LIMIT %s OFFSET %s`,
		d.table("jobs"), whereClause, orderBy, p(argIdx), p(argIdx+1))
	args = append(args, filter.Limit, filter.Offset)

	rows, err := d.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}

func (d *Driver) QueueStats(ctx context.Context, queue tasker.QueueName) (*tasker.QueueStats, error) {
	stats := &tasker.QueueStats{Queue: queue}
	p := d.dialect.Placeholder

	query := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(CASE WHEN state = 'available' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'retryable' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state IN ('scheduled','pending') THEN 1 ELSE 0 END), 0)
		FROM %s WHERE queue = %s`, d.table("jobs"), p(1))

	row := d.db.QueryRowContext(ctx, query, string(queue))
	err := row.Scan(&stats.Available, &stats.Running, &stats.Completed,
		&stats.Failed, &stats.Retryable, &stats.Scheduled)
	if err != nil {
		return nil, err
	}

	nowTime := time.Now()
	runtimeQuery := fmt.Sprintf(`
		SELECT COALESCE(AVG(
			(strftime('%%%%s', COALESCE(completed_at, %s)) - strftime('%%%%s', started_at)) * 1000
		), 0) FROM %s WHERE queue = %s AND state = 'completed' AND started_at IS NOT NULL
		AND completed_at > %s - 3600`,
		d.nowSQL(), d.table("jobs"), p(1), d.nowSQL())
	d.db.QueryRowContext(ctx, runtimeQuery, string(queue), nowTime, nowTime).Scan(&stats.AvgRuntimeMs)

	throughputQuery := fmt.Sprintf(`
		SELECT COALESCE(COUNT(*), 0) FROM %s
		WHERE queue = %s AND created_at > %s - 60`,
		d.table("jobs"), p(1), d.nowSQL())
	d.db.QueryRowContext(ctx, throughputQuery, string(queue), nowTime).Scan(&stats.ThroughputPerMin)

	return stats, nil
}

func (d *Driver) GlobalStats(ctx context.Context) (*tasker.GlobalStats, error) {
	stats := &tasker.GlobalStats{}
	now := d.nowSQL()

	stats.Status = "running"

	query := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(CASE WHEN state IN ('completed','failed') AND created_at > %s - 60 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN created_at > %s - 3600 THEN 1 ELSE 0 END), 0)
		FROM %s`, now, now, d.table("jobs"))

	var jobsPerMin, failedJobs, recentJobs int64
	if err := d.db.QueryRowContext(ctx, query, time.Now(), time.Now()).Scan(&jobsPerMin, &failedJobs, &recentJobs); err != nil {
		return nil, err
	}
	stats.JobsPerMinute = jobsPerMin
	stats.FailedJobs = failedJobs
	stats.RecentJobs = recentJobs

	var processes int
	d.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COALESCE(COUNT(DISTINCT node_id), 0) FROM %s WHERE state = 'running'`, d.table("jobs"))).Scan(&processes)
	stats.Processes = processes

	return stats, nil
}

func (d *Driver) JobStats(ctx context.Context, kind string) (*tasker.JobTypeStats, error) {
	stats := &tasker.JobTypeStats{Kind: kind}
	p := d.dialect.Placeholder
	now := d.nowSQL()

	query := fmt.Sprintf(`
		SELECT
			COALESCE(COUNT(*), 0),
			COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0),
			COALESCE(AVG(CASE WHEN state = 'completed' AND started_at IS NOT NULL
			                  THEN (strftime('%%%%s', COALESCE(completed_at, %s)) - strftime('%%%%s', started_at)) * 1000 END), 0)
		FROM %s WHERE kind = %s`, now, d.table("jobs"), p(1))

	if err := d.db.QueryRowContext(ctx, query, kind, time.Now()).Scan(&stats.TotalCount, &stats.FailedCount, &stats.AvgRuntimeMs); err != nil {
		return nil, err
	}

	d.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(COUNT(*), 0) FROM %s
		WHERE kind = %s AND created_at > %s - 60`, d.table("jobs"), p(1), now),
		kind, time.Now()).Scan(&stats.Throughput)

	return stats, nil
}

func (d *Driver) RegisterNode(ctx context.Context, node tasker.NodeInfo, ttl time.Duration) error {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		INSERT INTO %s (node_id, host, port, queues, workers, status, started_at, last_heartbeat, version)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
		ON CONFLICT (node_id) DO UPDATE SET
			host = %s, port = %s, queues = %s, workers = %s, status = %s,
			started_at = %s, last_heartbeat = %s, version = %s`,
		d.table("nodes"),
		p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9),
		p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17))

	queues := make([]string, len(node.Queues))
	for i, q := range node.Queues {
		queues[i] = string(q)
	}

	_, err := d.db.ExecContext(ctx, query,
		string(node.ID), node.Host, node.Port,
		encodeTextArray(queues), node.Workers,
		node.Status, node.StartedAt, time.Now(),
		node.Version,
		node.Host, node.Port,
		encodeTextArray(queues), node.Workers,
		node.Status, node.StartedAt, time.Now(),
		node.Version)
	return err
}

func (d *Driver) DeregisterNode(ctx context.Context, nodeID tasker.NodeID) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE node_id = %s`, d.table("nodes"), d.dialect.Placeholder(1))
	_, err := d.db.ExecContext(ctx, query, string(nodeID))
	return err
}

func (d *Driver) Heartbeat(ctx context.Context, nodeID tasker.NodeID, ttl time.Duration) error {
	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`UPDATE %s SET last_heartbeat = %s WHERE node_id = %s`, d.table("nodes"), now, p(1))
	_, err := d.db.ExecContext(ctx, query, time.Now(), string(nodeID))
	return err
}

func (d *Driver) ListNodes(ctx context.Context) ([]tasker.NodeInfo, error) {
	query := fmt.Sprintf(`
		SELECT node_id, host, port, queues, workers, status, started_at, last_heartbeat, version
		FROM %s
		WHERE last_heartbeat > %s - 30
		ORDER BY started_at`, d.table("nodes"), d.nowSQL())

	rows, err := d.db.QueryContext(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var nodes []tasker.NodeInfo
	for rows.Next() {
		var n tasker.NodeInfo
		var queuesStr string
		if err := rows.Scan(&n.ID, &n.Host, &n.Port, &queuesStr,
			&n.Workers, &n.Status, &n.StartedAt, &n.LastHeartbeat, &n.Version); err != nil {
			return nil, err
		}
		queueNames := decodeTextArray(queuesStr)
		n.Queues = make([]tasker.QueueName, len(queueNames))
		for i, q := range queueNames {
			n.Queues[i] = tasker.QueueName(q)
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func (d *Driver) LeaderElection(ctx context.Context, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	p := d.dialect.Placeholder

	result, err := d.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, node_id, host, port, status, started_at, last_heartbeat, version)
		VALUES (1, %s, 'leader', 0, 'leader', %s, %s, '1.0.0')
		ON CONFLICT (id) DO NOTHING`,
		d.table("leader"), p(1), p(2), p(3)),
		string(nodeID), time.Now(), time.Now())
	if err != nil {
		return false, err
	}

	rows, _ := result.RowsAffected()
	if rows > 0 {
		return true, nil
	}

	var currentLeader string
	var lastHeartbeat time.Time
	err = d.db.QueryRowContext(ctx,
		fmt.Sprintf(`SELECT node_id, last_heartbeat FROM %s WHERE id = 1`, d.table("leader"))).Scan(&currentLeader, &lastHeartbeat)
	if err != nil {
		return false, err
	}

	if currentLeader == string(nodeID) {
		return true, nil
	}

	if time.Since(lastHeartbeat) < ttl {
		return false, nil
	}

	d.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, d.table("leader")))

	_, err = d.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, node_id, host, port, status, started_at, last_heartbeat, version)
		VALUES (1, %s, 'leader', 0, 'leader', %s, %s, '1.0.0')`,
		d.table("leader"), p(1), p(2), p(3)),
		string(nodeID), time.Now(), time.Now())
	return err == nil, nil
}

func (d *Driver) IsLeader(ctx context.Context, nodeID tasker.NodeID) (bool, error) {
	now := d.nowSQL()
	query := fmt.Sprintf(`
		SELECT EXISTS(SELECT 1 FROM %s WHERE id = 1 AND node_id = %s
		              AND last_heartbeat > %s - 15)`,
		d.table("leader"), d.dialect.Placeholder(1), now)
	var exists bool
	err := d.db.QueryRowContext(ctx, query, string(nodeID), time.Now()).Scan(&exists)
	return exists, err
}

func (d *Driver) ResignLeadership(ctx context.Context, nodeID tasker.NodeID) error {
	_, err := d.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE id = 1 AND node_id = %s`,
			d.table("leader"), d.dialect.Placeholder(1)), string(nodeID))
	return err
}

func (d *Driver) AcquireLock(ctx context.Context, key string, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		INSERT INTO %s (lock_key, node_id, expires_at)
		VALUES (%s, %s, %s)
		ON CONFLICT (lock_key) DO NOTHING`,
		d.table("locks"), p(1), p(2), now)

	result, err := d.db.ExecContext(ctx, query, key, string(nodeID), time.Now())
	if err != nil {
		return false, nil
	}
	n, _ := result.RowsAffected()
	return n > 0, nil
}

func (d *Driver) ReleaseLock(ctx context.Context, key string, nodeID tasker.NodeID) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE lock_key = %s AND node_id = %s`,
		d.table("locks"), d.dialect.Placeholder(1), d.dialect.Placeholder(2))
	_, err := d.db.ExecContext(ctx, query, key, string(nodeID))
	return err
}

func (d *Driver) RequeueStale(ctx context.Context, timeout time.Duration) (int64, error) {
	p := d.dialect.Placeholder
	now := d.nowSQL()
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'available', node_id = NULL, scheduled_at = %s
		WHERE state = 'running' AND started_at < %s - %s`,
		d.table("jobs"), now, now, p(1))

	result, err := d.db.ExecContext(ctx, query, int64(timeout.Seconds()))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (d *Driver) Prune(ctx context.Context, before time.Time, states []tasker.State) (int64, error) {
	p := d.dialect.Placeholder
	placeholders := make([]string, len(states))
	args := make([]interface{}, len(states)+1)
	args[0] = before
	for i, s := range states {
		placeholders[i] = p(i + 2)
		args[i+1] = string(s)
	}

	query := fmt.Sprintf(`DELETE FROM %s WHERE created_at < %s AND state IN (%s)`,
		d.table("jobs"), p(1), strings.Join(placeholders, ","))

	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (d *Driver) insertFailedJob(ctx context.Context, id tasker.JobID) error {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		INSERT INTO %s (uuid, queue, kind, payload, attempt, max_attempts, errors, tags)
		SELECT uuid, queue, kind, payload, attempt, max_attempts, errors, tags
		FROM %s WHERE id = %s`,
		d.table("failed_jobs"), d.table("jobs"), p(1))

	_, err := d.db.ExecContext(ctx, query, id)
	return err
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func scanJob(row scanner) (*tasker.JobRow, error) {
	job := &tasker.JobRow{}
	var queue, kind, state string
	var tagsStr string
	var metadataJSON []byte
	var errorsJSON []byte
	var attemptedByStr string
	var timeout int64
	var uniqueKey, nodeID sql.NullString
	var output []byte

	err := row.Scan(
		&job.ID, &job.UUID, &queue, &kind, &job.Payload,
		&state, &job.Priority, &job.Attempt, &job.MaxAttempts,
		&attemptedByStr, &job.AttemptedAt, &errorsJSON,
		&output, &tagsStr, &job.ScheduledAt, &job.CreatedAt,
		&job.StartedAt, &job.CompletedAt, &job.FinalizedAt,
		&nodeID, &job.BatchID, &timeout, &metadataJSON,
		&uniqueKey,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, tasker.ErrJobNotFound
		}
		return nil, err
	}

	job.Queue = tasker.QueueName(queue)
	job.Kind = kind
	job.State = tasker.State(state)
	job.Timeout = time.Duration(timeout)
	job.Tags = decodeTextArray(tagsStr)
	job.Output = output
	if nodeID.Valid {
		job.NodeID = tasker.NodeID(nodeID.String)
	}
	if uniqueKey.Valid {
		job.UniqueKey = uniqueKey.String
	}

	if len(errorsJSON) > 0 {
		json.Unmarshal(errorsJSON, &job.Errors)
	}

	if len(metadataJSON) > 0 {
		json.Unmarshal(metadataJSON, &job.Metadata)
	}

	return job, nil
}

func scanJobs(rows *sql.Rows) ([]*tasker.JobRow, error) {
	var jobs []*tasker.JobRow
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func encodeTextArray(items []string) string {
	if len(items) == 0 {
		return "{}"
	}
	escaped := make([]string, len(items))
	for i, item := range items {
		escaped[i] = `"` + strings.ReplaceAll(item, `"`, `\"`) + `"`
	}
	return "{" + strings.Join(escaped, ",") + "}"
}

func decodeTextArray(s string) []string {
	s = strings.Trim(s, "{}")
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.Trim(p, `"`)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

func placeholders(ids []uint64, start int, ph func(int) string) string {
	parts := make([]string, len(ids))
	for i := range ids {
		parts[i] = ph(start + i)
	}
	return strings.Join(parts, ",")
}

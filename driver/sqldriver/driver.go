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

const jobColumns = `id, uuid, queue, kind, payload, state, priority, attempt, max_attempts,
	attempted_by, attempted_at, errors, output, tags, scheduled_at, created_at,
	started_at, completed_at, finalized_at, node_id, batch_id, timeout, metadata, unique_key`

func NewDriver(cfg Config) (*Driver, error) {
	if !validIdentifierPrefix(cfg.TablePrefix) {
		return nil, fmt.Errorf("invalid table prefix %q", cfg.TablePrefix)
	}
	db, err := sql.Open(cfg.DriverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	if err := db.Ping(); err != nil {
		db.Close()
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
	statements := d.migrationTableStatements()

	for _, statement := range statements {
		if _, err := d.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate sqldriver: %w", err)
		}
	}
	indexes := []struct{ name, columns string }{
		{"queue_state", "queue, state, scheduled_at, priority DESC, id"},
		{"state", "state"}, {"kind", "kind"}, {"batch_id", "batch_id"},
		{"node_id", "node_id"}, {"scheduled_at", "scheduled_at"}, {"unique_key", "unique_key"},
	}
	for _, index := range indexes {
		name := fmt.Sprintf("idx_%sjobs_%s", d.config.TablePrefix, index.name)
		query := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", name, d.table("jobs"), index.columns)
		if _, mysql := d.dialect.(*mysqlDialect); mysql {
			var count int
			err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`, d.table("jobs"), name).Scan(&count)
			if err != nil {
				return err
			}
			if count > 0 {
				continue
			}
			query = fmt.Sprintf("CREATE INDEX %s ON %s (%s)", name, d.table("jobs"), index.columns)
		}
		if _, err := d.db.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("create index %s: %w", name, err)
		}
	}

	if _, ok := d.dialect.(*sqliteDialect); ok {
		if _, err := d.db.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
			return err
		}
	}
	return nil
}

func (d *Driver) migrationTableStatements() []string {
	arrayType := d.dialect.ArrayType()
	keyType := "TEXT"
	idType := "BIGSERIAL PRIMARY KEY"
	payloadType := "BYTEA"
	if _, ok := d.dialect.(*sqliteDialect); ok {
		idType = "INTEGER PRIMARY KEY AUTOINCREMENT"
		payloadType = "BLOB"
	} else if _, ok := d.dialect.(*mysqlDialect); ok {
		idType = "BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY"
		payloadType = "LONGBLOB"
		keyType = "VARCHAR(255)"
	}

	return []string{fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id          %s,
		uuid        %s NOT NULL UNIQUE,
		queue       %s NOT NULL DEFAULT 'default',
		kind        %s NOT NULL,
		payload     %s NOT NULL,
		state       %s NOT NULL DEFAULT 'available',
		priority    INTEGER NOT NULL DEFAULT 0,
		attempt     INTEGER NOT NULL DEFAULT 0,
		max_attempts INTEGER NOT NULL DEFAULT 3,
		attempted_by %s NOT NULL,
		attempted_at TIMESTAMP,
		errors      TEXT NOT NULL,
		output      %s,
		tags        %s NOT NULL,
		scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		started_at  TIMESTAMP,
		completed_at TIMESTAMP,
		finalized_at TIMESTAMP,
		node_id     %s,
		batch_id    %s,
		timeout     INTEGER NOT NULL DEFAULT 0,
		metadata    TEXT NOT NULL,
		unique_key  %s
	)`, d.table("jobs"), idType, keyType, keyType, keyType, payloadType, keyType, arrayType, payloadType, arrayType, keyType, keyType, keyType), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id          INTEGER PRIMARY KEY CHECK (id = 1),
		node_id     %s NOT NULL DEFAULT '',
		host        %s NOT NULL DEFAULT '',
		port        INTEGER NOT NULL DEFAULT 0,
		status      %s NOT NULL DEFAULT 'inactive',
		started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		version     %s NOT NULL DEFAULT '1.0.0'
	)`, d.table("leader"), keyType, keyType, keyType, keyType), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		node_id     %s PRIMARY KEY,
		host        %s NOT NULL,
		port        INTEGER NOT NULL DEFAULT 0,
		queues      %s NOT NULL,
		workers     INTEGER NOT NULL DEFAULT 0,
		status      %s NOT NULL DEFAULT 'active',
		started_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		version     %s NOT NULL DEFAULT '1.0.0'
	)`, d.table("nodes"), keyType, keyType, arrayType, keyType, keyType), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id          %s,
		uuid        %s NOT NULL,
		queue       %s NOT NULL,
		kind        %s NOT NULL,
		payload     %s NOT NULL,
		attempt     INTEGER NOT NULL,
		max_attempts INTEGER NOT NULL,
		errors      TEXT NOT NULL,
		failed_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		tags        %s NOT NULL
	)`, d.table("failed_jobs"), idType, keyType, keyType, keyType, payloadType, arrayType), fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		lock_key    %s PRIMARY KEY,
		node_id     %s NOT NULL,
		expires_at  TIMESTAMP NOT NULL
	)`, d.table("locks"), keyType, keyType)}
}

func (d *Driver) Enqueue(ctx context.Context, job *tasker.JobRow) error {
	if job == nil {
		return fmt.Errorf("enqueue nil job")
	}
	query := d.jobInsertQuery()

	attemptedBy, tags, errorsJSON, metaJSON, err := d.encodeJobFields(job)
	if err != nil {
		return err
	}
	args := []interface{}{
		job.UUID, string(job.Queue), job.Kind, job.Payload, string(job.State),
		job.Priority, job.Attempt, job.MaxAttempts,
		attemptedBy, string(errorsJSON), tags, job.ScheduledAt, job.CreatedAt,
		job.BatchID, int64(job.Timeout), string(metaJSON), nullableString(job.UniqueKey),
	}
	if _, postgres := d.dialect.(*postgresDialect); postgres {
		return d.db.QueryRowContext(ctx, query+" RETURNING id", args...).Scan(&job.ID)
	}
	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	job.ID = tasker.JobID(id)
	return nil
}

func (d *Driver) EnqueueBatch(ctx context.Context, jobs []*tasker.JobRow) error {
	for _, job := range jobs {
		if job == nil {
			return fmt.Errorf("enqueue batch contains nil job")
		}
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	originalIDs := make([]tasker.JobID, len(jobs))
	for i, job := range jobs {
		originalIDs[i] = job.ID
	}
	committed := false
	defer func() {
		tx.Rollback()
		if !committed {
			for i, job := range jobs {
				job.ID = originalIDs[i]
			}
		}
	}()

	query := d.jobInsertQuery()
	if _, postgres := d.dialect.(*postgresDialect); postgres {
		query += " RETURNING id"
	}

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, job := range jobs {
		attemptedBy, tags, errorsJSON, metaJSON, err := d.encodeJobFields(job)
		if err != nil {
			return err
		}
		args := []interface{}{
			job.UUID, string(job.Queue), job.Kind, job.Payload, string(job.State),
			job.Priority, job.Attempt, job.MaxAttempts,
			attemptedBy, string(errorsJSON), tags, job.ScheduledAt, job.CreatedAt,
			job.BatchID, int64(job.Timeout), string(metaJSON), nullableString(job.UniqueKey),
		}
		if _, postgres := d.dialect.(*postgresDialect); postgres {
			if err := stmt.QueryRowContext(ctx, args...).Scan(&job.ID); err != nil {
				return err
			}
		} else {
			result, err := stmt.ExecContext(ctx, args...)
			if err != nil {
				return err
			}
			id, err := result.LastInsertId()
			if err != nil {
				return err
			}
			job.ID = tasker.JobID(id)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (d *Driver) jobInsertQuery() string {
	p := d.dialect.Placeholder
	return fmt.Sprintf(`INSERT INTO %s (uuid, queue, kind, payload, state, priority, attempt, max_attempts,
		attempted_by, errors, tags, scheduled_at, created_at, batch_id, timeout, metadata, unique_key)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		d.table("jobs"), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9),
		p(10), p(11), p(12), p(13), p(14), p(15), p(16), p(17))
}

func (d *Driver) Claim(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	if max <= 0 {
		return nil, nil
	}
	if _, mysql := d.dialect.(*mysqlDialect); mysql {
		return d.claimMySQL(ctx, queue, nodeID, max)
	}
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
			  AND state IN ('available', 'scheduled', 'retryable')
			  AND scheduled_at <= %s
			  AND attempt < max_attempts
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
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'running',
		    attempt = attempt + 1, attempted_at = ?,
		    started_at = COALESCE(started_at, ?), node_id = ?,
		    attempted_by = CASE
		        WHEN attempted_by IS NULL OR attempted_by = '' OR attempted_by = '{}' THEN json_array(?)
		        ELSE json_insert(CASE WHEN json_array_length(attempted_by) >= 50
		             THEN json_remove(attempted_by, '$[0]') ELSE attempted_by END, '$[#]', ?)
		    END
		WHERE id IN (
			SELECT id FROM %s
			WHERE queue = ? AND state IN ('available', 'scheduled', 'retryable')
			  AND scheduled_at <= ? AND attempt < max_attempts
			ORDER BY priority DESC, scheduled_at ASC, id ASC
			LIMIT ?
		)
		RETURNING %s`, d.table("jobs"), d.table("jobs"), jobColumns)

	nowTime := time.Now()
	rows, err := d.db.QueryContext(ctx, query,
		nowTime, nowTime, string(nodeID), string(nodeID), string(nodeID),
		string(queue), nowTime, max)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanJobs(rows)
}

func (d *Driver) claimMySQL(ctx context.Context, queue tasker.QueueName, nodeID tasker.NodeID, max int) ([]*tasker.JobRow, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	rows, err := tx.QueryContext(ctx, d.dialect.ClaimQuery(d.table("jobs")), string(queue), now, max)
	if err != nil {
		return nil, fmt.Errorf("select jobs to claim: %w", err)
	}
	var ids []tasker.JobID
	for rows.Next() {
		var id tasker.JobID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, nil
	}

	args := []interface{}{now, now, string(nodeID), string(nodeID), string(nodeID)}
	for _, id := range ids {
		args = append(args, id)
	}
	query := fmt.Sprintf(`UPDATE %s SET state = 'running', attempt = attempt + 1,
		attempted_at = ?, started_at = COALESCE(started_at, ?), node_id = ?,
		attempted_by = CASE WHEN JSON_TYPE(attempted_by) = 'OBJECT' THEN JSON_ARRAY(?)
			ELSE JSON_ARRAY_APPEND(CASE WHEN JSON_LENGTH(attempted_by) >= 50
			THEN JSON_REMOVE(attempted_by, '$[0]') ELSE attempted_by END, '$', ?) END
		WHERE id IN (%s)`, d.table("jobs"), placeholdersJobIDs(ids, d.dialect.Placeholder))
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	selectArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		selectArgs[i] = id
	}
	claimedRows, err := tx.QueryContext(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE id IN (%s)
		ORDER BY priority DESC, scheduled_at ASC, id ASC`, jobColumns, d.table("jobs"), placeholdersJobIDs(ids, d.dialect.Placeholder)), selectArgs...)
	if err != nil {
		return nil, err
	}
	jobs, err := scanJobs(claimedRows)
	claimedRows.Close()
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (d *Driver) Complete(ctx context.Context, id tasker.JobID, output []byte) error {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'completed', output = %s, completed_at = %s, finalized_at = %s, node_id = NULL
		WHERE id = %s AND state = 'running'`,
		d.table("jobs"), p(1), p(2), p(3), p(4))

	now := time.Now()
	result, err := d.db.ExecContext(ctx, query, output, now, now, id)
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
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	query := fmt.Sprintf(`SELECT attempt, errors FROM %s WHERE id = %s AND state IN ('running', 'retryable')`,
		d.table("jobs"), d.dialect.Placeholder(1))
	if d.dialect.SupportsSkipLocked() {
		query += " FOR UPDATE"
	}
	var attempt int
	var existing []byte
	if err := tx.QueryRowContext(ctx, query, id).Scan(&attempt, &existing); err != nil {
		if err == sql.ErrNoRows {
			return tasker.ErrJobNotFound
		}
		return err
	}
	var attemptErrors []tasker.AttemptError
	if len(existing) > 0 && json.Unmarshal(existing, &attemptErrors) != nil {
		return fmt.Errorf("decode job errors")
	}
	attemptErrors = append(attemptErrors, tasker.AttemptError{Attempt: attempt, Error: fmt.Sprint(jobErr), Timestamp: time.Now()})
	errorsJSON, err := json.Marshal(attemptErrors)
	if err != nil {
		return fmt.Errorf("encode job errors: %w", err)
	}
	p := d.dialect.Placeholder
	now := time.Now()
	result, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET state = 'failed', errors = %s,
		finalized_at = %s, completed_at = %s, node_id = NULL WHERE id = %s AND state IN ('running', 'retryable')`,
		d.table("jobs"), p(1), p(2), p(3), p(4)), string(errorsJSON), now, now, id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return tasker.ErrJobNotFound
	}
	if err := d.insertFailedJobTx(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *Driver) ScheduleRetry(ctx context.Context, id tasker.JobID, jobErr error, scheduledAt time.Time) error {
	if jobErr == nil {
		jobErr = fmt.Errorf("job failed")
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	p := d.dialect.Placeholder
	query := fmt.Sprintf(`SELECT attempt, errors FROM %s WHERE id = %s AND state = 'running'`,
		d.table("jobs"), p(1))
	if d.dialect.SupportsSkipLocked() {
		query += " FOR UPDATE"
	}
	var attempt int
	var existing []byte
	if err := tx.QueryRowContext(ctx, query, id).Scan(&attempt, &existing); err != nil {
		if err == sql.ErrNoRows {
			return tasker.ErrJobNotFound
		}
		return err
	}
	var attemptErrors []tasker.AttemptError
	if len(existing) > 0 {
		if err := json.Unmarshal(existing, &attemptErrors); err != nil {
			return fmt.Errorf("decode job errors: %w", err)
		}
	}
	attemptErrors = append(attemptErrors, tasker.AttemptError{Attempt: attempt, Error: fmt.Sprint(jobErr), Timestamp: time.Now()})
	errorsJSON, err := json.Marshal(attemptErrors)
	if err != nil {
		return fmt.Errorf("encode job errors: %w", err)
	}
	result, err := tx.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET state = 'retryable', errors = %s,
		scheduled_at = %s, node_id = NULL, attempted_at = NULL, started_at = NULL,
		completed_at = NULL, finalized_at = NULL, output = NULL
		WHERE id = %s AND state = 'running'`, d.table("jobs"), p(1), p(2), p(3)),
		string(errorsJSON), scheduledAt, id)
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return tasker.ErrJobNotFound
	}
	return tx.Commit()
}

func (d *Driver) Retry(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'available', scheduled_at = %s, node_id = NULL, attempt = 0,
		    errors = %s, attempted_by = %s, attempted_at = NULL, started_at = NULL, completed_at = NULL,
		    finalized_at = NULL, output = NULL
		WHERE id = %s AND state IN ('failed', 'completed', 'cancelled', 'retryable')`,
		d.table("jobs"), p(1), p(2), p(3), p(4))

	result, err := d.db.ExecContext(ctx, query, time.Now(), "[]", d.emptyArray(), id)
	if err != nil {
		return nil, err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return nil, tasker.ErrJobNotFound
	}
	return d.GetByID(ctx, id)
}

func (d *Driver) RetryBatch(ctx context.Context, ids []tasker.JobID) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'available', scheduled_at = %s, node_id = NULL, attempt = 0,
		    errors = %s, attempted_by = %s, attempted_at = NULL, started_at = NULL, completed_at = NULL,
		    finalized_at = NULL, output = NULL
		WHERE id = %s AND state IN ('failed', 'completed', 'cancelled', 'retryable')`,
		d.table("jobs"), p(1), p(2), p(3), p(4))

	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, id := range ids {
		if _, err := stmt.ExecContext(ctx, time.Now(), "[]", d.emptyArray(), id); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *Driver) Cancel(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'cancelled', finalized_at = %s, node_id = NULL
		WHERE id = %s AND state IN ('available', 'pending', 'scheduled', 'retryable')
		`, d.table("jobs"), p(1), p(2))

	result, err := d.db.ExecContext(ctx, query, time.Now(), id)
	if err != nil {
		return nil, err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		return nil, tasker.ErrJobNotFound
	}
	return d.GetByID(ctx, id)
}

func (d *Driver) CancelBatch(ctx context.Context, ids []tasker.JobID) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'cancelled', finalized_at = %s, node_id = NULL
		WHERE id = %s AND state IN ('available', 'pending', 'scheduled', 'retryable')`,
		d.table("jobs"), p(1), p(2))

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

func (d *Driver) GetByID(ctx context.Context, id tasker.JobID) (*tasker.JobRow, error) {
	query := fmt.Sprintf(`SELECT %s FROM %s WHERE id = %s`, jobColumns, d.table("jobs"), d.dialect.Placeholder(1))
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
		conditions = append(conditions, fmt.Sprintf("(kind %s %s OR uuid %s %s)",
			d.dialect.ILike(), p(argIdx), d.dialect.ILike(), p(argIdx+1)))
		search := "%" + filter.Search + "%"
		args = append(args, search, search)
		argIdx += 2
	}

	for _, tag := range filter.Tags {
		switch d.dialect.(type) {
		case *postgresDialect:
			conditions = append(conditions, fmt.Sprintf("%s = ANY(tags)", p(argIdx)))
		case *mysqlDialect:
			conditions = append(conditions, fmt.Sprintf("JSON_CONTAINS(tags, JSON_QUOTE(%s))", p(argIdx)))
		default:
			conditions = append(conditions, fmt.Sprintf("EXISTS (SELECT 1 FROM json_each(tags) WHERE value = %s)", p(argIdx)))
		}
		args = append(args, tag)
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

	orderBy := "created_at DESC"
	allowedOrder := map[string]bool{
		"id": true, "uuid": true, "queue": true, "kind": true, "state": true,
		"priority": true, "attempt": true, "scheduled_at": true, "created_at": true,
		"started_at": true, "completed_at": true, "finalized_at": true,
	}
	if allowedOrder[filter.OrderBy] {
		dir := "DESC"
		if strings.EqualFold(filter.Order, "asc") {
			dir = "ASC"
		}
		orderBy = fmt.Sprintf("%s %s", filter.OrderBy, dir)
	}

	dataQuery := fmt.Sprintf(`SELECT %s FROM %s %s ORDER BY %s LIMIT %s OFFSET %s`,
		jobColumns, d.table("jobs"), whereClause, orderBy, p(argIdx), p(argIdx+1))
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

	cutoffHour := time.Now().Add(-time.Hour)
	durationExpr := d.durationMillisSQL("started_at", "completed_at")
	runtimeQuery := fmt.Sprintf(`
		SELECT COALESCE(AVG(%s), 0) FROM %s WHERE queue = %s AND state = 'completed'
		AND started_at IS NOT NULL AND completed_at > %s`,
		durationExpr, d.table("jobs"), p(1), p(2))
	if err := d.db.QueryRowContext(ctx, runtimeQuery, string(queue), cutoffHour).Scan(&stats.AvgRuntimeMs); err != nil {
		return nil, err
	}

	throughputQuery := fmt.Sprintf(`
		SELECT COALESCE(COUNT(*), 0) FROM %s
		WHERE queue = %s AND created_at > %s`, d.table("jobs"), p(1), p(2))
	if err := d.db.QueryRowContext(ctx, throughputQuery, string(queue), time.Now().Add(-time.Minute)).Scan(&stats.ThroughputPerMin); err != nil {
		return nil, err
	}

	return stats, nil
}

func (d *Driver) GlobalStats(ctx context.Context) (*tasker.GlobalStats, error) {
	stats := &tasker.GlobalStats{}
	stats.Status = "running"

	query := fmt.Sprintf(`
		SELECT
			COALESCE(SUM(CASE WHEN state IN ('completed','failed') AND created_at > %s THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN created_at > %s THEN 1 ELSE 0 END), 0)
		FROM %s`, d.dialect.Placeholder(1), d.dialect.Placeholder(2), d.table("jobs"))

	var jobsPerMin, failedJobs, recentJobs int64
	if err := d.db.QueryRowContext(ctx, query, time.Now().Add(-time.Minute), time.Now().Add(-time.Hour)).Scan(&jobsPerMin, &failedJobs, &recentJobs); err != nil {
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
	query := fmt.Sprintf(`
		SELECT
			COALESCE(COUNT(*), 0),
			COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0),
			COALESCE(AVG(CASE WHEN state = 'completed' AND started_at IS NOT NULL
			                  THEN %s END), 0)
		FROM %s WHERE kind = %s`, d.durationMillisSQL("started_at", "completed_at"), d.table("jobs"), p(1))

	if err := d.db.QueryRowContext(ctx, query, kind).Scan(&stats.TotalCount, &stats.FailedCount, &stats.AvgRuntimeMs); err != nil {
		return nil, err
	}

	d.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COALESCE(COUNT(*), 0) FROM %s
		WHERE kind = %s AND created_at > %s`, d.table("jobs"), p(1), p(2)),
		kind, time.Now().Add(-time.Minute)).Scan(&stats.Throughput)

	return stats, nil
}

func (d *Driver) RegisterNode(ctx context.Context, node tasker.NodeInfo, ttl time.Duration) error {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		INSERT INTO %s (node_id, host, port, queues, workers, status, started_at, last_heartbeat, version)
		VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)`,
		d.table("nodes"),
		p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9))
	if _, mysql := d.dialect.(*mysqlDialect); mysql {
		query += ` ON DUPLICATE KEY UPDATE host = VALUES(host), port = VALUES(port), queues = VALUES(queues),
			workers = VALUES(workers), status = VALUES(status), started_at = VALUES(started_at),
			last_heartbeat = VALUES(last_heartbeat), version = VALUES(version)`
	} else {
		query += ` ON CONFLICT (node_id) DO UPDATE SET host = excluded.host, port = excluded.port,
			queues = excluded.queues, workers = excluded.workers, status = excluded.status,
			started_at = excluded.started_at, last_heartbeat = excluded.last_heartbeat, version = excluded.version`
	}

	queues := make([]string, len(node.Queues))
	for i, q := range node.Queues {
		queues[i] = string(q)
	}

	queuesValue, err := d.encodeStrings(queues)
	if err != nil {
		return err
	}
	_, err = d.db.ExecContext(ctx, query,
		string(node.ID), node.Host, node.Port,
		queuesValue, node.Workers,
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
	query := fmt.Sprintf(`UPDATE %s SET last_heartbeat = %s WHERE node_id = %s`, d.table("nodes"), p(1), p(2))
	result, err := d.db.ExecContext(ctx, query, time.Now(), string(nodeID))
	if err != nil {
		return err
	}
	if n, _ := result.RowsAffected(); n == 0 {
		var exists int
		query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE node_id = %s`, d.table("nodes"), p(1))
		if err := d.db.QueryRowContext(ctx, query, string(nodeID)).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			return tasker.ErrNodeNotFound
		}
	}
	return nil
}

func (d *Driver) ListNodes(ctx context.Context) ([]tasker.NodeInfo, error) {
	query := fmt.Sprintf(`
		SELECT node_id, host, port, queues, workers, status, started_at, last_heartbeat, version
		FROM %s
		WHERE last_heartbeat > %s
		ORDER BY started_at`, d.table("nodes"), d.dialect.Placeholder(1))

	rows, err := d.db.QueryContext(ctx, query, time.Now().Add(-30*time.Second))
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
	now := time.Now()
	result, err := d.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET node_id = %s, started_at = %s,
		last_heartbeat = %s WHERE id = 1 AND (node_id = %s OR last_heartbeat <= %s)`,
		d.table("leader"), p(1), p(2), p(3), p(4), p(5)),
		string(nodeID), now, now, string(nodeID), now.Add(-ttl))
	if err != nil {
		return false, err
	}
	rows, _ := result.RowsAffected()
	if rows > 0 {
		return true, nil
	}

	query := fmt.Sprintf(`INSERT INTO %s (id, node_id, host, port, status, started_at, last_heartbeat, version)
		VALUES (1, %s, 'leader', 0, 'leader', %s, %s, '1.0.0')`, d.table("leader"), p(1), p(2), p(3))
	if _, mysql := d.dialect.(*mysqlDialect); mysql {
		query += " ON DUPLICATE KEY UPDATE id = id"
	} else {
		query += " ON CONFLICT (id) DO NOTHING"
	}
	result, err = d.db.ExecContext(ctx, query, string(nodeID), now, now)
	if err != nil {
		return false, err
	}
	rows, _ = result.RowsAffected()
	if rows == 1 {
		return true, nil
	}
	var current string
	err = d.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT node_id FROM %s WHERE id = 1`, d.table("leader"))).Scan(&current)
	return current == string(nodeID), err
}

func (d *Driver) IsLeader(ctx context.Context, nodeID tasker.NodeID) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS(SELECT 1 FROM %s WHERE id = 1 AND node_id = %s
		              AND last_heartbeat > %s)`,
		d.table("leader"), d.dialect.Placeholder(1), d.dialect.Placeholder(2))
	var exists bool
	err := d.db.QueryRowContext(ctx, query, string(nodeID), time.Now().Add(-15*time.Second)).Scan(&exists)
	return exists, err
}

func (d *Driver) ResignLeadership(ctx context.Context, nodeID tasker.NodeID) error {
	_, err := d.db.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE id = 1 AND node_id = %s`,
			d.table("leader"), d.dialect.Placeholder(1)), string(nodeID))
	return err
}

func (d *Driver) AcquireLock(ctx context.Context, key string, nodeID tasker.NodeID, ttl time.Duration) (bool, error) {
	now := time.Now()
	result, err := d.db.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET expires_at = %s
		WHERE lock_key = %s AND node_id = %s AND expires_at > %s`, d.table("locks"),
		d.dialect.Placeholder(1), d.dialect.Placeholder(2), d.dialect.Placeholder(3), d.dialect.Placeholder(4)),
		now.Add(ttl), key, string(nodeID), now)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n > 0 {
		return true, nil
	}
	_, err = d.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE lock_key = %s AND expires_at <= %s`,
		d.table("locks"), d.dialect.Placeholder(1), d.dialect.Placeholder(2)), key, now)
	if err != nil {
		return false, err
	}
	result, err = d.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (lock_key, node_id, expires_at) VALUES (%s, %s, %s)`,
		d.table("locks"), d.dialect.Placeholder(1), d.dialect.Placeholder(2), d.dialect.Placeholder(3)),
		key, string(nodeID), now.Add(ttl))
	if err != nil {
		var existing string
		if scanErr := d.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT node_id FROM %s WHERE lock_key = %s`,
			d.table("locks"), d.dialect.Placeholder(1)), key).Scan(&existing); scanErr == nil {
			return existing == string(nodeID), nil
		}
		return false, err
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
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'available', node_id = NULL, scheduled_at = %s,
		    attempted_at = NULL, started_at = NULL
		WHERE state = 'running' AND COALESCE(attempted_at, started_at) < %s`,
		d.table("jobs"), p(1), p(2))

	now := time.Now()
	result, err := d.db.ExecContext(ctx, query, now, now.Add(-timeout))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (d *Driver) Prune(ctx context.Context, before time.Time, states []tasker.State) (int64, error) {
	if len(states) == 0 {
		return 0, nil
	}
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
	return d.insertFailedJobTx(ctx, d.db, id)
}

type sqlExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func (d *Driver) insertFailedJobTx(ctx context.Context, execer sqlExecer, id tasker.JobID) error {
	p := d.dialect.Placeholder
	query := fmt.Sprintf(`
		INSERT INTO %s (uuid, queue, kind, payload, attempt, max_attempts, errors, tags)
		SELECT uuid, queue, kind, payload, attempt, max_attempts, errors, tags
		FROM %s WHERE id = %s`,
		d.table("failed_jobs"), d.table("jobs"), p(1))

	_, err := execer.ExecContext(ctx, query, id)
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
	var uniqueKey, nodeID, batchID sql.NullString
	var output []byte

	err := row.Scan(
		&job.ID, &job.UUID, &queue, &kind, &job.Payload,
		&state, &job.Priority, &job.Attempt, &job.MaxAttempts,
		&attemptedByStr, &job.AttemptedAt, &errorsJSON,
		&output, &tagsStr, &job.ScheduledAt, &job.CreatedAt,
		&job.StartedAt, &job.CompletedAt, &job.FinalizedAt,
		&nodeID, &batchID, &timeout, &metadataJSON,
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
	attemptedBy := decodeTextArray(attemptedByStr)
	job.AttemptedBy = make([]tasker.NodeID, len(attemptedBy))
	for i, id := range attemptedBy {
		job.AttemptedBy[i] = tasker.NodeID(id)
	}
	job.Output = output
	if nodeID.Valid {
		job.NodeID = tasker.NodeID(nodeID.String)
	}
	if uniqueKey.Valid {
		job.UniqueKey = uniqueKey.String
	}
	if batchID.Valid {
		job.BatchID = batchID.String
	}

	if len(errorsJSON) > 0 {
		if err := json.Unmarshal(errorsJSON, &job.Errors); err != nil {
			return nil, fmt.Errorf("decode job errors: %w", err)
		}
	}

	if len(metadataJSON) > 0 {
		if err := json.Unmarshal(metadataJSON, &job.Metadata); err != nil {
			return nil, fmt.Errorf("decode job metadata: %w", err)
		}
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
		item = strings.ReplaceAll(item, `\`, `\\`)
		escaped[i] = `"` + strings.ReplaceAll(item, `"`, `\"`) + `"`
	}
	return "{" + strings.Join(escaped, ",") + "}"
}

func decodeTextArray(s string) []string {
	if strings.HasPrefix(strings.TrimSpace(s), "[") {
		var result []string
		if json.Unmarshal([]byte(s), &result) == nil {
			return result
		}
	}
	s = strings.Trim(s, "{}")
	if s == "" {
		return nil
	}
	var result []string
	var item strings.Builder
	quoted := false
	wasQuoted := false
	escaped := false
	appendItem := func() {
		value := item.String()
		if !wasQuoted {
			value = strings.TrimSpace(value)
		}
		result = append(result, value)
		item.Reset()
		wasQuoted = false
	}
	for _, r := range s {
		if escaped {
			item.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && quoted {
			escaped = true
			continue
		}
		if r == '"' {
			quoted = !quoted
			wasQuoted = true
			continue
		}
		if r == ',' && !quoted {
			appendItem()
			continue
		}
		item.WriteRune(r)
	}
	appendItem()
	return result
}

func (d *Driver) encodeStrings(items []string) (string, error) {
	if _, postgres := d.dialect.(*postgresDialect); postgres {
		return encodeTextArray(items), nil
	}
	encoded, err := json.Marshal(items)
	if err != nil {
		return "", fmt.Errorf("encode string list: %w", err)
	}
	return string(encoded), nil
}

func (d *Driver) emptyArray() string {
	if _, postgres := d.dialect.(*postgresDialect); postgres {
		return "{}"
	}
	return "[]"
}

func (d *Driver) encodeJobFields(job *tasker.JobRow) (attemptedBy, tags string, errorsJSON, metadataJSON []byte, err error) {
	attempted := make([]string, len(job.AttemptedBy))
	for i, nodeID := range job.AttemptedBy {
		attempted[i] = string(nodeID)
	}
	if attemptedBy, err = d.encodeStrings(attempted); err != nil {
		return "", "", nil, nil, err
	}
	if tags, err = d.encodeStrings(job.Tags); err != nil {
		return "", "", nil, nil, err
	}
	errors := job.Errors
	if errors == nil {
		errors = []tasker.AttemptError{}
	}
	errorsJSON, err = json.Marshal(errors)
	if err != nil {
		return "", "", nil, nil, fmt.Errorf("encode job errors: %w", err)
	}
	metadata := job.Metadata
	if metadata == nil {
		metadata = map[string]string{}
	}
	metadataJSON, err = json.Marshal(metadata)
	if err != nil {
		return "", "", nil, nil, fmt.Errorf("encode job metadata: %w", err)
	}
	return attemptedBy, tags, errorsJSON, metadataJSON, nil
}

func nullableString(value string) interface{} {
	if value == "" {
		return nil
	}
	return value
}

func escapeLike(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `%`, `\%`)
	return strings.ReplaceAll(value, `_`, `\_`)
}

func placeholdersJobIDs(ids []tasker.JobID, ph func(int) string) string {
	parts := make([]string, len(ids))
	for i := range ids {
		parts[i] = ph(i + 1)
	}
	return strings.Join(parts, ",")
}

func (d *Driver) durationMillisSQL(start, end string) string {
	switch d.dialect.(type) {
	case *postgresDialect:
		return fmt.Sprintf("EXTRACT(EPOCH FROM (%s - %s)) * 1000", end, start)
	case *mysqlDialect:
		return fmt.Sprintf("TIMESTAMPDIFF(MICROSECOND, %s, %s) / 1000", start, end)
	default:
		return fmt.Sprintf("(julianday(%s) - julianday(%s)) * 86400000", end, start)
	}
}

func validIdentifierPrefix(prefix string) bool {
	for _, r := range prefix {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '_' {
			return false
		}
	}
	return true
}

func placeholders(ids []uint64, start int, ph func(int) string) string {
	parts := make([]string, len(ids))
	for i := range ids {
		parts[i] = ph(start + i)
	}
	return strings.Join(parts, ",")
}

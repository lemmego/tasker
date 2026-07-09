package sqldriver

import (
	"fmt"
	"time"
)

type NowValue struct {
	SQL   string      // SQL fragment, empty means use placeholder
	Value interface{} // Go value to pass when SQL is empty
}

type Dialect interface {
	Now() NowValue
	ILike() string
	Placeholder(n int) string
	SupportsSkipLocked() bool
	ClaimQuery(table string) string
	ArrayType() string
}

type postgresDialect struct{}

func (d *postgresDialect) Now() NowValue {
	return NowValue{SQL: "NOW()"}
}

func (d *postgresDialect) ILike() string { return "ILIKE" }

func (d *postgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

func (d *postgresDialect) SupportsSkipLocked() bool { return true }

func (d *postgresDialect) ClaimQuery(table string) string {
	return fmt.Sprintf(`
		WITH locked AS (
			SELECT id FROM %s
			WHERE queue = %%s
			  AND state = 'available'
			  AND scheduled_at <= NOW()
			ORDER BY priority DESC, scheduled_at ASC, id ASC
			LIMIT %%s
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s j
		SET state = 'running',
		    attempt = j.attempt + 1,
		    attempted_at = NOW(),
		    started_at = COALESCE(j.started_at, NOW()),
		    node_id = %%s,
		    attempted_by = array_append(
		        CASE WHEN array_length(j.attempted_by, 1) >= 50
		             THEN j.attempted_by[array_length(j.attempted_by, 1) - 48:]
		             ELSE j.attempted_by
		        END,
		        %%s::TEXT
		    )
		FROM locked l
		WHERE j.id = l.id
		RETURNING j.id, j.uuid, j.queue, j.kind, j.payload, j.state, j.priority,
		          j.attempt, j.max_attempts, j.attempted_by, j.attempted_at,
		          j.errors, j.output, j.tags, j.scheduled_at, j.created_at,
		          j.started_at, j.completed_at, j.finalized_at, j.node_id,
		          j.batch_id, j.timeout, j.metadata`, table, table)
}

func (d *postgresDialect) ArrayType() string { return "TEXT[]" }

type mysqlDialect struct{}

func (d *mysqlDialect) Now() NowValue { return NowValue{SQL: "NOW()"} }

func (d *mysqlDialect) ILike() string { return "LIKE" }

func (d *mysqlDialect) Placeholder(n int) string { return "?" }

func (d *mysqlDialect) SupportsSkipLocked() bool { return true }

func (d *mysqlDialect) ClaimQuery(table string) string {
	return (&postgresDialect{}).ClaimQuery(table)
}

func (d *mysqlDialect) ArrayType() string { return "JSON" }

type sqliteDialect struct{}

func (d *sqliteDialect) Now() NowValue {
	return NowValue{Value: time.Now()}
}

func (d *sqliteDialect) ILike() string { return "LIKE" }

func (d *sqliteDialect) Placeholder(n int) string { return "?" }

func (d *sqliteDialect) SupportsSkipLocked() bool { return false }

func (d *sqliteDialect) ClaimQuery(table string) string {
	return fmt.Sprintf(`
		UPDATE %s
		SET state = 'running',
		    attempt = attempt + 1,
		    attempted_at = ?,
		    started_at = COALESCE(started_at, ?),
		    node_id = ?,
		    attempted_by = ?
		WHERE id IN (
			SELECT id FROM %s
			WHERE queue = ?
			  AND state = 'available'
			  AND scheduled_at <= ?
			ORDER BY priority DESC, scheduled_at ASC, id ASC
			LIMIT ?
		)
		RETURNING id, uuid, queue, kind, payload, state, priority,
		          attempt, max_attempts, attempted_by, attempted_at,
		          errors, output, tags, scheduled_at, created_at,
		          started_at, completed_at, finalized_at, node_id,
		          batch_id, timeout, metadata`, table, table)
}

func (d *sqliteDialect) ArrayType() string { return "TEXT" }

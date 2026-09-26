package sqldriver

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func openSQLite(t *testing.T) *sql.DB {
	t.Helper()
	pool, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "borrowed.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// The whole point of borrowing: the queue runs against the pool it was given,
// so it cannot end up pointed at a different database than the rest of the
// application.
func TestBorrowedPoolIsUsedAsGiven(t *testing.T) {
	pool := openSQLite(t)

	driver, err := NewDriver(Config{DB: pool, Dialect: "sqlite", TablePrefix: "tasker_"})
	if err != nil {
		t.Fatal(err)
	}
	if driver.db != pool {
		t.Fatal("the driver did not use the pool it was given")
	}
	if err := driver.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// The tables must exist in the caller's database, not somewhere else.
	var name string
	err = pool.QueryRow(
		`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'tasker_jobs'`).Scan(&name)
	if err != nil {
		t.Fatalf("tasker_jobs was not created in the borrowed database: %v", err)
	}
}

// Closing the driver must not close a pool the application still needs. This
// is the sharp edge of borrowing and the reason Close is conditional.
func TestCloseLeavesABorrowedPoolOpen(t *testing.T) {
	pool := openSQLite(t)

	driver, err := NewDriver(Config{DB: pool, Dialect: "sqlite", TablePrefix: "tasker_"})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := pool.Ping(); err != nil {
		t.Fatalf("closing the driver closed the application's pool: %v", err)
	}
}

// An owned pool must still be closed, or the queue leaks a connection pool
// every time it shuts down.
func TestCloseClosesAnOwnedPool(t *testing.T) {
	driver, err := NewDriver(Config{
		DriverName:  "sqlite",
		DSN:         filepath.Join(t.TempDir(), "owned.sqlite"),
		TablePrefix: "tasker_",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := driver.db.Ping(); err == nil {
		t.Fatal("an owned pool was left open after Close")
	}
}

// A borrowed pool carries no driver name, and for a pool opened through a
// connector there may be no registered driver name at all — so guessing a
// dialect would silently produce the wrong SQL. Refuse instead.
func TestBorrowedPoolRequiresADialect(t *testing.T) {
	if _, err := NewDriver(Config{DB: openSQLite(t), TablePrefix: "tasker_"}); err == nil {
		t.Fatal("NewDriver accepted a borrowed pool with no dialect")
	}
}

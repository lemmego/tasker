package tasker

import "context"

// jobRowContextKey is the key the executing job's row is stored under.
type jobRowContextKey struct{}

// ContextWithRow puts the row being executed on the context.
//
// The worker calls this before it runs any middleware, so a JobMiddleware and
// a job's own Handle can reach the row. Exported because a caller that
// executes a job itself — a test, a one-off runner — should be able to put
// the same thing there.
func ContextWithRow(ctx context.Context, row *JobRow) context.Context {
	if row == nil {
		return ctx
	}
	return context.WithValue(ctx, jobRowContextKey{}, row)
}

// RowFromContext returns the row of the job being executed.
//
// This is what closes the gap between an enqueue and an execution. A
// DispatchMiddleware can write to a row's Metadata — the id of the request
// that queued the job, say — and until now nothing on the execution side
// could read it back: JobMiddleware is handed the decoded job and the raw
// payload, neither of which carries Metadata. The two halves happen in
// different processes, so the row is the only thing that travels between
// them.
//
// The row is the worker's own, not a copy. Read it; writing to it races the
// worker's own state transitions.
func RowFromContext(ctx context.Context) (*JobRow, bool) {
	row, ok := ctx.Value(jobRowContextKey{}).(*JobRow)
	return row, ok
}

package web

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/lemmego/tasker"
	"github.com/lemmego/tasker/supervisor"
)

type Server struct {
	manager     *tasker.Manager
	supervisor  *supervisor.Supervisor
	mux         *http.ServeMux
	middlewares []func(http.Handler) http.Handler
	prefix      string
	authEnabled bool
}

func New(mgr *tasker.Manager, sup *supervisor.Supervisor) *Server {
	return NewWithPrefix(mgr, sup, "")
}

func NewWithPrefix(mgr *tasker.Manager, sup *supervisor.Supervisor, prefix string) *Server {
	s := &Server{
		manager:    mgr,
		supervisor: sup,
		mux:        http.NewServeMux(),
		prefix:     prefix,
	}
	s.registerRoutes()
	return s
}

func (s *Server) Use(mw func(http.Handler) http.Handler) {
	s.middlewares = append(s.middlewares, mw)
}

// UseAuth installs the middleware responsible for authenticating management
// requests. Management routes remain disabled until this method is called.
// The middleware may reject requests, redirect them, or attach its own user
// identity to the request context.
func (s *Server) UseAuth(mw func(http.Handler) http.Handler) {
	s.authEnabled = true
	s.Use(mw)
}

// UseCSRF protects state-changing management requests with a double-submit
// token. It is intended to be used alongside UseAuth for browser sessions.
func (s *Server) UseCSRF() {
	s.Use(csrfMiddleware(s.cookiePath()))
}

func (s *Server) Handler() http.Handler {
	var h http.Handler = s.mux
	if !s.authEnabled {
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeError(w, http.StatusUnauthorized, "tasker management authentication is not configured")
		})
	}
	for i := len(s.middlewares) - 1; i >= 0; i-- {
		h = s.middlewares[i](h)
	}
	return h
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.Handler().ServeHTTP(w, r)
}

func (s *Server) p(path string) string {
	return s.prefix + path
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("GET "+s.p("/"), s.handleDashboard)
	s.mux.HandleFunc("GET "+s.p("/api/stats"), s.handleStats)
	s.mux.HandleFunc("GET "+s.p("/api/jobs"), s.handleListJobs)
	s.mux.HandleFunc("GET "+s.p("/api/jobs/{id}"), s.handleGetJob)
	s.mux.HandleFunc("POST "+s.p("/api/jobs/{id}/retry"), s.handleRetryJob)
	s.mux.HandleFunc("POST "+s.p("/api/jobs/{id}/cancel"), s.handleCancelJob)
	s.mux.HandleFunc("POST "+s.p("/api/jobs/batch/retry"), s.handleBatchRetry)
	s.mux.HandleFunc("POST "+s.p("/api/jobs/batch/cancel"), s.handleBatchCancel)
	s.mux.HandleFunc("GET "+s.p("/api/queues"), s.handleListQueues)
	s.mux.HandleFunc("POST "+s.p("/api/queues/{queue}/pause"), s.handlePauseQueue)
	s.mux.HandleFunc("POST "+s.p("/api/queues/{queue}/resume"), s.handleResumeQueue)
	s.mux.HandleFunc("GET "+s.p("/api/workers"), s.handleListWorkers)
	s.mux.HandleFunc("GET "+s.p("/api/metrics/jobs"), s.handleJobMetrics)
	s.mux.HandleFunc("GET "+s.p("/api/metrics/queues"), s.handleQueueMetrics)
	s.mux.HandleFunc("POST "+s.p("/api/prune"), s.handlePrune)
	s.mux.HandleFunc("GET "+s.p("/api/events"), s.handleEvents)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, dashboardHTML)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.manager.GlobalStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	filter := tasker.JobFilter{
		Limit:  50,
		Offset: 0,
	}

	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			filter.Limit = clampLimit(n)
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if n, err := strconv.Atoi(o); err == nil {
			filter.Offset = n
		}
	}
	if states := r.URL.Query().Get("states"); states != "" {
		for _, s := range strings.Split(states, ",") {
			filter.States = append(filter.States, tasker.State(s))
		}
	}
	if queues := r.URL.Query().Get("queues"); queues != "" {
		for _, q := range strings.Split(queues, ",") {
			filter.Queues = append(filter.Queues, tasker.QueueName(q))
		}
	}
	if kinds := r.URL.Query().Get("kinds"); kinds != "" {
		filter.Kinds = strings.Split(kinds, ",")
	}
	if search := r.URL.Query().Get("search"); search != "" {
		filter.Search = search
	}
	filter.OrderBy = r.URL.Query().Get("order_by")
	filter.Order = r.URL.Query().Get("order")

	jobs, total, err := s.manager.QueryJobs(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"data":   jobs,
		"total":  total,
		"limit":  filter.Limit,
		"offset": filter.Offset,
	})
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := s.manager.GetJob(r.Context(), id)
	if err != nil {
		if err == tasker.ErrJobNotFound {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := s.manager.Retry(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	id, err := parseID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	job, err := s.manager.Cancel(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, job)
}

func (s *Server) handleBatchRetry(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []tasker.JobID `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := s.manager.RetryBatch(r.Context(), req.IDs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleBatchCancel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []tasker.JobID `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if err := s.manager.CancelBatch(r.Context(), req.IDs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	// A nil supervisor is a legitimate construction: New(manager, nil) is
	// public, and a process that reads the queue without working it has no
	// supervisor to give. It used to be a nil pointer dereference here,
	// which the api recoverer turns into a 500 on the one endpoint the
	// dashboard's overview cannot render without.
	if s.supervisor == nil {
		// An empty list rather than an error. "This process runs no
		// workers" is not a failure, and the dashboard says so in those
		// words rather than claiming there are no queues.
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	queues := s.supervisor.Queues()
	type queueInfo struct {
		Name   tasker.QueueName   `json:"name"`
		Stats  *tasker.QueueStats `json:"stats"`
		Paused bool               `json:"paused"`
	}

	var result []queueInfo
	for _, q := range queues {
		stats, err := s.manager.QueueStats(r.Context(), q)
		if err != nil {
			continue
		}
		paused := false
		if pool, ok := s.supervisor.Pools()[q]; ok {
			paused = pool.WorkerCount() == 0
		}
		result = append(result, queueInfo{
			Name:   q,
			Stats:  stats,
			Paused: paused,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handlePauseQueue(w http.ResponseWriter, r *http.Request) {
	queue := tasker.QueueName(r.PathValue("queue"))
	if s.supervisor == nil {
		writeError(w, http.StatusConflict,
			"this process runs no workers, so it cannot pause a queue")
		return
	}
	if err := s.supervisor.PauseQueue(queue); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumeQueue(w http.ResponseWriter, r *http.Request) {
	queue := tasker.QueueName(r.PathValue("queue"))
	if s.supervisor == nil {
		writeError(w, http.StatusConflict,
			"this process runs no workers, so it cannot resume a queue")
		return
	}
	if err := s.supervisor.ResumeQueue(queue); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	nodes, err := s.manager.Driver().ListNodes(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, nodes)
}

func (s *Server) handleJobMetrics(w http.ResponseWriter, r *http.Request) {
	type metric struct {
		Kind       string  `json:"kind"`
		Throughput float64 `json:"throughput"`
		AvgRuntime float64 `json:"avg_runtime_ms"`
		Failed     int64   `json:"failed"`
		Total      int64   `json:"total"`
	}

	var result []metric
	for _, kind := range tasker.ListRegisteredJobs() {
		stats, err := s.manager.JobStats(r.Context(), kind)
		if err != nil {
			continue
		}
		result = append(result, metric{
			Kind:       stats.Kind,
			Throughput: stats.Throughput,
			AvgRuntime: stats.AvgRuntimeMs,
			Failed:     stats.FailedCount,
			Total:      stats.TotalCount,
		})
	}

	if result == nil {
		result = []metric{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleQueueMetrics(w http.ResponseWriter, r *http.Request) {
	type metric struct {
		Queue      tasker.QueueName `json:"queue"`
		Throughput float64          `json:"throughput_per_min"`
		AvgRuntime float64          `json:"avg_runtime_ms"`
		Available  int64            `json:"available"`
		Running    int64            `json:"running"`
		Completed  int64            `json:"completed"`
		Failed     int64            `json:"failed"`
	}

	var result []metric
	if s.supervisor == nil {
		writeJSON(w, http.StatusOK, []any{})
		return
	}
	for _, q := range s.supervisor.Queues() {
		stats, err := s.manager.QueueStats(r.Context(), q)
		if err != nil {
			continue
		}
		result = append(result, metric{
			Queue:      stats.Queue,
			Throughput: stats.ThroughputPerMin,
			AvgRuntime: stats.AvgRuntimeMs,
			Available:  stats.Available,
			Running:    stats.Running,
			Completed:  stats.Completed,
			Failed:     stats.Failed,
		})
	}

	if result == nil {
		result = []metric{}
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	// http.NewResponseController rather than w.(http.Flusher), and this is
	// the bug that mattered most: a type assertion traverses nothing, so any
	// middleware wrapping the ResponseWriter — lemmego's own RequestLogger
	// among them — made this answer 500 "streaming not supported" for every
	// request. The controller follows Unwrap, so a wrapped writer works.
	rc := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// nginx buffers text/event-stream by default, which makes a working
	// stream look dead through a proxy and nowhere else.
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	// Flushed before anything is sent, so the client can tell "connected"
	// from "hung". Without it the first observable byte was the first tick,
	// which is five seconds of a dashboard that looks broken.
	if err := rc.Flush(); err != nil {
		return
	}

	ctx := r.Context()

	// The server's write deadline is not the stream's. api/app sets one on
	// http.Server, so without clearing it the connection dies partway through
	// and the browser reconnects in a loop; a stream owns its own lifetime.
	// Best-effort: a writer that cannot do this still works, it just ends at
	// the server's deadline and reconnects.
	_ = rc.SetWriteDeadline(time.Time{})

	send := func() bool {
		stats, err := s.manager.GlobalStats(ctx)
		if err != nil {
			// A failed read is not a reason to drop the connection: the next
			// tick may well succeed, and a reconnect loop is worse than a
			// gap. A comment frame keeps the socket warm meanwhile.
			fmt.Fprint(w, ": stats unavailable\n\n")
			return rc.Flush() == nil
		}
		data, err := json.Marshal(stats)
		if err != nil {
			return true
		}
		// A named event, so a client can listen for this one specifically
		// rather than for whatever arrives.
		fmt.Fprintf(w, "event: stats\ndata: %s\n\n", data)
		return rc.Flush() == nil
	}

	// Immediately, not on the first tick. The dashboard paints from this
	// frame, so waiting five seconds for it is five seconds of empty page.
	if !send() {
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Comfortably under the idle timeout of every proxy worth naming. An
	// idle intermediary closes a connection it believes is dead, and a
	// five-second data tick is not a heartbeat: it stops being sent the
	// moment GlobalStats starts failing, which is exactly when the
	// connection matters.
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !send() {
				return
			}
		case <-heartbeat.C:
			fmt.Fprint(w, ": keepalive\n\n")
			if rc.Flush() != nil {
				return
			}
		}
	}
}

func (s *Server) handlePrune(w http.ResponseWriter, r *http.Request) {
	before := time.Now().Add(-7 * 24 * time.Hour)
	states := []tasker.State{tasker.StateCompleted, tasker.StateFailed, tasker.StateCancelled}
	count, err := s.manager.Driver().Prune(r.Context(), before, states)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"pruned": count,
	})
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parseID(s string) (tasker.JobID, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return tasker.JobID(n), nil
}

const (
	csrfCookieName = "tasker_csrf"

	// csrfHeaderName is named rather than written twice. The dashboard has to
	// send exactly this, in a JavaScript string literal no compiler reads, so
	// a test pins the two together.
	csrfHeaderName = "X-CSRF-Token"
)

// cookiePath scopes the CSRF cookie to where the dashboard is mounted.
//
// Not "/", which is what it used to be. A cookie at the root is sent on every
// request to the host application — every page, every API call, every asset —
// to be read by nothing, because only this dashboard's script looks at it.
// Scoped to the mount prefix it is sent to the dashboard and nowhere else,
// and the script can still read it because the script only ever runs under
// that prefix.
func (s *Server) cookiePath() string {
	if s.prefix == "" {
		return "/"
	}
	return s.prefix
}

// csrfMiddleware is a double-submit guard, deliberately its own rather than
// the host framework's.
//
// tasker does not depend on lemmego/api and must not: it is a standalone
// queue that runs in programs with no framework at all, so it has no session
// to bind a token to and no middleware stack to borrow one from.
//
// # Why the names differ from the application's
//
// This is the load-bearing part, and it is the cookie name rather than the
// header. lemmego/api's CSRF is *session-backed*: its XSRF-TOKEN cookie only
// delivers the session's _token to JavaScript, and the check compares the
// request header against the session, never against the cookie. Reading the
// token back out of the cookie is commented out in that file on purpose,
// because doing so would turn a session-bound token into a plain
// double-submit.
//
// So a shared cookie name would be worse than untidy. This middleware writes
// a fresh random value into the cookie whenever a GET arrives without one —
// and that value is in no session. Under the name XSRF-TOKEN it would
// overwrite the application's cookie, the next form post anywhere on the site
// would send a token the session does not know, and CSRF would start failing
// on pages that have nothing to do with the queue. A separate name is what
// keeps an embedded dashboard from breaking its host.
func csrfMiddleware(cookiePath string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
				if _, err := r.Cookie(csrfCookieName); err != nil {
					token, err := newCSRFToken()
					if err != nil {
						writeError(w, http.StatusInternalServerError, "could not create csrf token")
						return
					}
					http.SetCookie(w, &http.Cookie{
						Name:     csrfCookieName,
						Value:    token,
						Path:     cookiePath,
						HttpOnly: false,
						SameSite: http.SameSiteStrictMode,
					})
				}
				next.ServeHTTP(w, r)
				return
			}

			cookie, err := r.Cookie(csrfCookieName)
			provided := r.Header.Get(csrfHeaderName)
			if err != nil || provided == "" || !secureStringEqual(cookie.Value, provided) {
				writeError(w, http.StatusForbidden, "invalid csrf token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func newCSRFToken() (string, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", token), nil
}

func secureStringEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// DefaultPageSize and MaxPageSize bound a job listing.
//
// The limit arrives from a query string, which is the normal case rather
// than the hostile one — a dashboard builds the URL — but it was previously
// passed through untouched, so limit=1000000 was a request for a million
// rows that the database would honour. One request, and the process is out
// of memory.
const (
	DefaultPageSize = 50
	MaxPageSize     = 500
)

// clampLimit keeps a listing bounded.
//
// Zero and negative mean "unspecified" rather than "none": a caller asking
// for no rows is almost always a caller who built the URL wrong, and
// answering an empty list reads as an empty queue.
func clampLimit(n int) int {
	if n <= 0 {
		return DefaultPageSize
	}
	if n > MaxPageSize {
		return MaxPageSize
	}
	return n
}

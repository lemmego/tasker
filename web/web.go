package web

import (
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

func (s *Server) Handler() http.Handler {
	var h http.Handler = s.mux
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
			filter.Limit = n
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
		"data":  jobs,
		"total": total,
		"limit": filter.Limit,
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
	queues := s.supervisor.Queues()
	type queueInfo struct {
		Name   tasker.QueueName  `json:"name"`
		Stats  *tasker.QueueStats `json:"stats"`
		Paused bool              `json:"paused"`
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
	if err := s.supervisor.PauseQueue(queue); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResumeQueue(w http.ResponseWriter, r *http.Request) {
	queue := tasker.QueueName(r.PathValue("queue"))
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
		Queue          tasker.QueueName `json:"queue"`
		Throughput     float64          `json:"throughput_per_min"`
		AvgRuntime     float64          `json:"avg_runtime_ms"`
		Available      int64            `json:"available"`
		Running        int64            `json:"running"`
		Completed      int64            `json:"completed"`
		Failed         int64            `json:"failed"`
	}

	var result []metric
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
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := s.manager.GlobalStats(ctx)
			if err != nil {
				continue
			}
			data, _ := json.Marshal(stats)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
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

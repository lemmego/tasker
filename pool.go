package tasker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

type Pool struct {
	mu         sync.Mutex
	manager    *Manager
	queue      QueueName
	maxWorkers int
	workers    []*worker
	running    bool
	stop       chan struct{}
	stopped    chan struct{}
}

func NewPool(mgr *Manager, queue QueueName, maxWorkers int) *Pool {
	return &Pool{
		manager:    mgr,
		queue:      queue,
		maxWorkers: maxWorkers,
		stop:       make(chan struct{}),
		stopped:    make(chan struct{}),
	}
}

func (p *Pool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("pool already running for queue %s", p.queue)
	}

	p.running = true
	p.workers = make([]*worker, 0, p.maxWorkers)

	for i := 0; i < p.maxWorkers; i++ {
		w := newWorker(
			fmt.Sprintf("%s-%s-%d", p.manager.config.NodeID, p.queue, i),
			p.manager, p.queue,
		)
		w.start(ctx)
		p.workers = append(p.workers, w)
	}

	slog.Info("worker pool started",
		"queue", p.queue,
		"workers", p.maxWorkers,
		"node", p.manager.config.NodeID,
	)

	go p.watch(ctx)

	return nil
}

func (p *Pool) Stop(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	p.running = false
	close(p.stop)

	var wg sync.WaitGroup
	for _, w := range p.workers {
		wg.Add(1)
		go func(wrk *worker) {
			defer wg.Done()
			wrk.stop()
			wrk.wait()
		}(w)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	close(p.stopped)
	return nil
}

func (p *Pool) Scale(target int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if target < 1 {
		target = 1
	}

	current := len(p.workers)

	if target > current {
		for i := current; i < target; i++ {
			w := newWorker(
				fmt.Sprintf("%s-%s-%d", p.manager.config.NodeID, p.queue, i),
				p.manager, p.queue,
			)
			w.start(context.Background())
			p.workers = append(p.workers, w)
		}
	} else if target < current {
		toRemove := p.workers[target:]
		p.workers = p.workers[:target]

		go func() {
			var wg sync.WaitGroup
			for _, wrk := range toRemove {
				wg.Add(1)
				go func(w *worker) {
					defer wg.Done()
					w.stop()
					w.wait()
				}(wrk)
			}
			wg.Wait()
		}()
	}

	return nil
}

func (p *Pool) WorkerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.workers)
}

func (p *Pool) Queue() QueueName {
	return p.queue
}

func (p *Pool) watch(ctx context.Context) {
	<-p.stop
	for _, w := range p.workers {
		w.stop()
	}
}

type worker struct {
	id      string
	manager *Manager
	queue   QueueName
	active  chan struct{}
	done    chan struct{}
	once    sync.Once
}

func newWorker(id string, mgr *Manager, queue QueueName) *worker {
	return &worker{
		id:      id,
		manager: mgr,
		queue:   queue,
		active:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

func (w *worker) start(ctx context.Context) {
	go w.run(ctx)
}

func (w *worker) stop() {
	w.once.Do(func() {
		close(w.done)
	})
}

func (w *worker) wait() {
	<-w.done
}

func (w *worker) run(ctx context.Context) {
	slog.Debug("worker started", "id", w.id, "queue", w.queue)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-w.done:
			break loop
		default:
		}

		jobs, err := w.manager.driver.Claim(ctx, w.queue, w.manager.config.NodeID, 1)
		if err != nil {
			slog.Error("worker claim failed", "id", w.id, "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-w.done:
				break loop
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		w.active <- struct{}{}
		w.execute(ctx, jobs[0])
		<-w.active
	}

	slog.Debug("worker stopped", "id", w.id, "queue", w.queue)
	w.once.Do(func() {
		close(w.done)
	})
}

type execResult struct {
	state   State
	output  []byte
	err     error
	backoff time.Duration
}

func (w *worker) execute(ctx context.Context, job *JobRow) {
	logger := slog.With("job_id", job.ID, "kind", job.Kind, "queue", job.Queue)

	execCtx := ctx
	if job.Timeout > 0 {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithTimeout(ctx, job.Timeout)
		defer cancel()
	}

	result := w.processJob(execCtx, job, logger)

	switch result.state {
	case StateCompleted:
		err := w.manager.driver.Complete(ctx, job.ID, result.output)
		if err != nil {
			logger.Error("failed to mark job completed", "error", err)
		}

	case StateRetryable:
		err := w.manager.driver.Enqueue(ctx, &JobRow{
			UUID:        job.UUID,
			Queue:       job.Queue,
			Kind:        job.Kind,
			Payload:     job.Payload,
			State:       StateRetryable,
			Priority:    job.Priority,
			Attempt:     job.Attempt,
			MaxAttempts: job.MaxAttempts,
			Tags:        job.Tags,
			ScheduledAt: time.Now().Add(result.backoff),
			CreatedAt:   job.CreatedAt,
			BatchID:     job.BatchID,
			Timeout:     job.Timeout,
		})
		if err != nil {
			logger.Error("failed to schedule retry", "error", err)
		} else {
			logger.Debug("job scheduled for retry", "backoff", result.backoff, "attempt", job.Attempt)
		}

	case StateFailed:
		err := w.manager.driver.Fail(ctx, job.ID, result.err)
		if err != nil {
			logger.Error("failed to mark job failed", "error", err)
		} else {
			logger.Info("job failed permanently", "error", result.err)
		}

		w.callFailedHook(ctx, job, result.err)
	}
}

func (w *worker) processJob(ctx context.Context, job *JobRow, logger *slog.Logger) execResult {
	jobObj, err := w.decodeJob(job.Kind, job.Payload)
	if err != nil {
		return execResult{state: StateFailed, err: fmt.Errorf("decode error: %w", err)}
	}

	middlewares := w.collectMiddlewares(jobObj)

	var execErr error

	runFunc := func(ctx context.Context) error {
		if h, ok := jobObj.(BeforeHandleHook); ok {
			if err := h.BeforeHandle(ctx); err != nil {
				return fmt.Errorf("before handle hook: %w", err)
			}
		}

		if err := jobObj.Handle(ctx); err != nil {
			return err
		}

		if h, ok := jobObj.(AfterHandleHook); ok {
			if err := h.AfterHandle(ctx); err != nil {
				return fmt.Errorf("after handle hook: %w", err)
			}
		}

		return nil
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		mw := middlewares[i]
		next := runFunc
		runFunc = func(ctx context.Context) error {
			return mw(ctx, jobObj, job.Payload, next)
		}
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				execErr = fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
			}
		}()
		execErr = runFunc(ctx)
	}()

	if execErr != nil {
		nextState, backoff := ComputeNextState(job, execErr)
		return execResult{
			state:   nextState,
			err:     execErr,
			backoff: backoff,
		}
	}

	return execResult{
		state: StateCompleted,
	}
}

func (w *worker) decodeJob(kind string, payload []byte) (Job, error) {
	factory, ok := GetRegisteredJob(kind)
	if !ok {
		return nil, fmt.Errorf("no job registered for kind: %s", kind)
	}

	job := factory()
	if err := json.Unmarshal(payload, job); err != nil {
		return nil, err
	}

	return job, nil
}

func (w *worker) collectMiddlewares(job Job) []JobMiddleware {
	var middlewares []JobMiddleware
	middlewares = append(middlewares, GetGlobalMiddlewares()...)
	if mw, ok := job.(ShouldMiddleware); ok {
		middlewares = append(middlewares, mw.Middleware()...)
	}
	return middlewares
}

func (w *worker) callFailedHook(ctx context.Context, job *JobRow, err error) {
	jobObj, decodeErr := w.decodeJob(job.Kind, job.Payload)
	if decodeErr != nil {
		slog.Error("failed to decode job for failed hook", "error", decodeErr)
		return
	}

	if f, ok := jobObj.(ShouldFail); ok {
		if hookErr := f.Failed(ctx, job.Payload, err); hookErr != nil {
			slog.Error("failed hook returned error", "error", hookErr)
		}
	}
}

type PoolManager struct {
	mu      sync.RWMutex
	pools   map[QueueName]*Pool
	manager *Manager
}

func NewPoolManager(mgr *Manager) *PoolManager {
	return &PoolManager{
		pools:   make(map[QueueName]*Pool),
		manager: mgr,
	}
}

func (pm *PoolManager) StartPool(ctx context.Context, queue QueueName, concurrency int) (*Pool, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.pools[queue]; exists {
		return nil, fmt.Errorf("pool already exists for queue %s", queue)
	}

	pool := NewPool(pm.manager, queue, concurrency)
	if err := pool.Start(ctx); err != nil {
		return nil, err
	}
	pm.pools[queue] = pool
	return pool, nil
}

func (pm *PoolManager) StopPool(ctx context.Context, queue QueueName) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pool, exists := pm.pools[queue]
	if !exists {
		return nil
	}

	if err := pool.Stop(ctx); err != nil {
		return err
	}
	delete(pm.pools, queue)
	return nil
}

func (pm *PoolManager) ScalePool(queue QueueName, target int) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	pool, exists := pm.pools[queue]
	if !exists {
		return fmt.Errorf("no pool for queue %s", queue)
	}

	return pool.Scale(target)
}

func (pm *PoolManager) StopAll(ctx context.Context) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for queue, pool := range pm.pools {
		if err := pool.Stop(ctx); err != nil {
			return err
		}
		delete(pm.pools, queue)
	}
	return nil
}

func (pm *PoolManager) WorkerCount() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	total := 0
	for _, pool := range pm.pools {
		total += pool.WorkerCount()
	}
	return total
}

func (pm *PoolManager) Pools() map[QueueName]*Pool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	pools := make(map[QueueName]*Pool)
	for k, v := range pm.pools {
		pools[k] = v
	}
	return pools
}

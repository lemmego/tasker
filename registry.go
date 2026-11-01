package tasker

import (
	"sync"
)

type JobFactory func() Job

var (
	jobRegistry       = make(map[string]JobFactory)
	jobRegistryMu     sync.RWMutex
	globalMiddlewares []JobMiddleware
	middlewareMu      sync.RWMutex

	dispatchMiddlewares []DispatchMiddleware
	dispatchMu          sync.RWMutex
)

func RegisterJob(name string, factory JobFactory) {
	jobRegistryMu.Lock()
	defer jobRegistryMu.Unlock()
	jobRegistry[name] = factory
}

func GetRegisteredJob(name string) (JobFactory, bool) {
	jobRegistryMu.RLock()
	defer jobRegistryMu.RUnlock()
	f, ok := jobRegistry[name]
	return f, ok
}

func ListRegisteredJobs() []string {
	jobRegistryMu.RLock()
	defer jobRegistryMu.RUnlock()
	names := make([]string, 0, len(jobRegistry))
	for n := range jobRegistry {
		names = append(names, n)
	}
	return names
}

func AddGlobalMiddleware(mw JobMiddleware) {
	middlewareMu.Lock()
	defer middlewareMu.Unlock()
	globalMiddlewares = append(globalMiddlewares, mw)
}

func SetGlobalMiddlewares(mws []JobMiddleware) {
	middlewareMu.Lock()
	defer middlewareMu.Unlock()
	globalMiddlewares = mws
}

func GetGlobalMiddlewares() []JobMiddleware {
	middlewareMu.RLock()
	defer middlewareMu.RUnlock()
	mws := make([]JobMiddleware, len(globalMiddlewares))
	copy(mws, globalMiddlewares)
	return mws
}

// AddDispatchMiddleware installs a middleware around every enqueue, for the
// whole process. Additive, like AddGlobalMiddleware.
//
// Process-wide rather than per-Manager, matching the execution side, because
// an observer wants every enqueue including the ones made through
// tasker.Global(). The cost is that it is global state a test can leave
// behind, which is what SetDispatchMiddlewares(nil) is for.
func AddDispatchMiddleware(mw DispatchMiddleware) {
	if mw == nil {
		return
	}
	dispatchMu.Lock()
	defer dispatchMu.Unlock()
	dispatchMiddlewares = append(dispatchMiddlewares, mw)
}

// SetDispatchMiddlewares replaces the installed set. Passing nil clears it,
// which is how a test stops leaking into the next one.
func SetDispatchMiddlewares(mws []DispatchMiddleware) {
	dispatchMu.Lock()
	defer dispatchMu.Unlock()
	dispatchMiddlewares = mws
}

// GetDispatchMiddlewares returns a copy of the installed set.
func GetDispatchMiddlewares() []DispatchMiddleware {
	dispatchMu.RLock()
	defer dispatchMu.RUnlock()
	mws := make([]DispatchMiddleware, len(dispatchMiddlewares))
	copy(mws, dispatchMiddlewares)
	return mws
}

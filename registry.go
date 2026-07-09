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

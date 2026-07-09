package tasker

type Config struct {
	Driver         Driver
	DefaultQueue   QueueName
	DefaultMaxAttempts int
	NodeID         NodeID
}

func DefaultAppConfig() Config {
	cfg := DefaultConfig()
	return Config{
		DefaultQueue:       cfg.DefaultQueue,
		DefaultMaxAttempts: cfg.DefaultMaxAttempts,
		NodeID:             cfg.NodeID,
	}
}

func NewConfiguredManager(cfg Config) *Manager {
	return NewManager(
		WithDriver(cfg.Driver),
		WithDefaultQueue(cfg.DefaultQueue),
		WithMaxAttempts(cfg.DefaultMaxAttempts),
		WithNodeID(cfg.NodeID),
	)
}

package celery

// NoOpBackend is a Backend that doesn't anything
type NoOpBackend struct{}

// Publish implements the Backend interface.
func (NoOpBackend) Publish(Task, *ResultMeta) {}

package celery

// DiscardBackend is a Backend that doesn't anything
type DiscardBackend struct{}

// Publish implements the Backend interface.
func (DiscardBackend) Publish(Task, *ResultMeta) {}

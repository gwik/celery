package celery

import (
	"github.com/gwik/celery/types"
)

// NoOpBackend is a Backend that doesn't anything
type NoOpBackend struct{}

// Publish implements the Backend interface.
func (NoOpBackend) Publish(types.Task, *types.ResultMeta) {}

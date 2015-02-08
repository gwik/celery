/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"golang.org/x/net/context"
)

// Task is the interface for tasks.
type Task interface {
	context.Context
	Msg() Message
	Ack() error
	Reject(requeue bool) error
}

// Subscriber is the interface components that produces tasks.
type Subscriber interface {
	Subscribe() <-chan Task
	Close() error
}

// HandleFunc is the type for function that run tasks and return their results.
type HandleFunc func(context.Context, []interface{}, map[string]interface{}) (interface{}, error)

/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"time"

	"golang.org/x/net/context"
)

type Task interface {
	Msg() *Message
	Ack() error
	Reject(requeue bool) error
}

// Task bundled with a context. used to pass through channels.
type TaskContext struct {
	C context.Context
	T Task
}

type Subscriber interface {
	Subscribe() <-chan TaskContext
	Close() error
}

func IsReady(t Task) bool {
	return time.Now().After(t.Msg().ETA)
}

type HandleFunc func(Task) interface{}

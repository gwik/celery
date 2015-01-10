/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"time"
)

type Task interface {
	Msg() *Message
	Ack()
}

type Subscriber interface {
	Subscribe() <-chan Task
	Close() error
}

func IsReady(t Task) bool {
	return time.Now().After(t.Msg().ETA)
}

type HandleFunc func(Task) interface{}

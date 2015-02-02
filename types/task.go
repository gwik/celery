/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"golang.org/x/net/context"
)

type Task interface {
	context.Context
	Msg() Message
	Ack() error
	Reject(requeue bool) error
}

type Subscriber interface {
	Subscribe() <-chan Task
	Close() error
}

type HandleFunc func(context.Context, []interface{}, map[string]interface{}) (interface{}, error)

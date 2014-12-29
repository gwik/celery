/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"code.google.com/p/go.net/context"
)

type Task struct {
	Msg *Message
	Ctx context.Context
}

type Result struct {
	Task *Task
}

func NewResult(task *Task) *Result {
	return &Result{Task: task}
}

func NewTask(msg *Message) *Task {
	return &Task{
		Msg: msg,
		Ctx: context.Background(),
	}
}

type HandleFunc func(*Task) interface{}

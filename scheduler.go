/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package gocelery

import (
	"container/heap"
	"log"
	"time"

	"github.com/gwik/gocelery/types"
)

type table []*types.TaskContext

func (t table) Len() int { return len(t) }

func (t table) Less(i, j int) bool {
	return t[i].T.Msg().ETA.Before(t[j].T.Msg().ETA)
}

func (t table) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *table) Push(task interface{}) {
	*t = append(*t, task.(*types.TaskContext))
}

func (t *table) Pop() interface{} {
	old := *t
	n := len(old)
	task := old[n-1]
	*t = old[0 : n-1]
	return task
}

func (t table) Top() *types.TaskContext {
	if len(t) == 0 {
		return nil
	}
	return t[0]
}

type scheduler struct {
	t    *table
	sub  <-chan types.TaskContext
	pub  chan types.TaskContext
	quit chan struct{}
}

func Schedule(sub types.Subscriber) types.Subscriber {
	t := make(table, 0, 32)
	heap.Init(&t)

	sched := &scheduler{
		t:    &t,
		sub:  sub.Subscribe(),
		pub:  make(chan types.TaskContext),
		quit: make(chan struct{}),
	}

	go sched.loop()
	return sched
}

func (s *scheduler) Subscribe() <-chan types.TaskContext {
	return s.pub
}

func (s *scheduler) loop() {
	var timer <-chan time.Time
	for {
		top := s.t.Top()
		if top != nil {
			delay := top.T.Msg().ETA.Sub(time.Now())
			timer = time.After(delay)
			log.Printf("next pop in %s", delay)
		} else {
			timer = nil
			log.Println("wait for tasks...")
		}

		select {
		case task, ok := <-s.sub:
			if ok {
				heap.Push(s.t, &task)
			} else {
				close(s.quit)
			}
		case <-timer:
			s.pub <- *heap.Pop(s.t).(*types.TaskContext)
		case <-s.quit:
			close(s.pub)
			return
		}
	}
}

func (s *scheduler) Close() error {
	close(s.quit)
	return nil
}

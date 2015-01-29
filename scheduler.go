/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"container/heap"
	"log"
	"time"

	"github.com/gwik/celery/types"
)

type item struct {
	eta time.Time
	tc  types.TaskContext
}

var empty item = item{}

type table []item

func (t table) Len() int { return len(t) }

func (t table) Less(i, j int) bool {
	return t[i].eta.Before(t[j].eta)
}

func (t table) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *table) Push(task interface{}) {
	*t = append(*t, task.(item))
}

func (t *table) Pop() interface{} {
	old := *t
	n := len(old)
	task := old[n-1]
	*t = old[0 : n-1]
	return task
}

func (t table) Top() item {
	if len(t) == 0 {
		return empty
	}
	return t[0]
}

type Scheduler struct {
	t        *table
	sub      <-chan types.TaskContext
	pub      chan types.TaskContext
	backdoor chan item
	quit     chan struct{}
}

func NewScheduler(sub types.Subscriber) *Scheduler {
	t := make(table, 0, 32)
	heap.Init(&t)

	sched := &Scheduler{
		t:        &t,
		sub:      sub.Subscribe(),
		pub:      make(chan types.TaskContext),
		backdoor: make(chan item),
		quit:     make(chan struct{}),
	}

	go sched.loop()
	return sched
}

func (s *Scheduler) Publish(eta time.Time, t types.TaskContext) {
	s.backdoor <- item{eta, t}
}

func (s *Scheduler) Subscribe() <-chan types.TaskContext {
	return s.pub
}

func (s *Scheduler) loop() {
	var timer <-chan time.Time
	var next types.TaskContext
	var out chan<- types.TaskContext

	defer func() {
		log.Println("close sched.")
		close(s.pub)
	}()

	for {
		if out == nil {
			top := s.t.Top()
			if top != empty {
				now := time.Now()
				delay := top.eta.Sub(now)
				if delay < 0 {
					next = heap.Pop(s.t).(item).tc
					out = s.pub
				} else {
					timer = time.After(delay)
					// log.Printf("next pop in %s", delay)
				}
			} else {
				timer = nil
				log.Println("wait for tasks...")
			}
		}

		select { // carefull the order matters
		case <-s.quit:
			log.Println("close scheduler.")
			return
		case out <- next:
			out = nil
		case <-timer:
			next = heap.Pop(s.t).(item).tc
			out = s.pub
		case it := <-s.backdoor:
			heap.Push(s.t, it)
		case tc, ok := <-s.sub:
			if ok {
				heap.Push(s.t, item{tc.T.Msg().ETA, tc})
			} else {
				return
			}
		}
	}
}

func (s *Scheduler) Close() error {
	log.Println("close sched Close.")
	close(s.quit)
	return nil
}

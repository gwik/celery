/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"container/heap"
	"log"
	"time"
)

type item struct {
	eta time.Time
	t   Task
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

// Scheduler pull tasks from a subscriber and publish them when
// their ETA is reached.
type Scheduler struct {
	t        *table
	sub      <-chan Task
	pub      chan Task
	backdoor chan item
	quit     chan struct{}
}

// NewScheduler returns a new scheduler pulling tasks from sub.
func NewScheduler(sub Subscriber) *Scheduler {
	t := make(table, 0, 32)
	heap.Init(&t)

	sched := &Scheduler{
		t:        &t,
		sub:      sub.Subscribe(),
		pub:      make(chan Task),
		backdoor: make(chan item),
		quit:     make(chan struct{}),
	}

	go sched.loop()
	return sched
}

// Publish publishes a task at the given ETA.
func (s *Scheduler) Publish(eta time.Time, t Task) {
	s.backdoor <- item{eta, t}
}

// Subscribe implements the Subscriber interface.
func (s *Scheduler) Subscribe() <-chan Task {
	return s.pub
}

func (s *Scheduler) schedule(eta time.Time, t Task) time.Duration {
	delta := eta.Sub(time.Now())
	if delta > 0 {
		heap.Push(s.t, item{eta, t})
	}
	return delta
}

func (s *Scheduler) scheduleTask(t Task) time.Duration {
	return s.schedule(t.Msg().ETA, t)
}

func (s *Scheduler) loop() {
	var timer <-chan time.Time
	var t Task
	var out chan<- Task

	defer func() {
		log.Println("Close scheduler.")
		close(s.pub)
	}()

	for {
		timer = nil
		if out == nil {
			top := s.t.Top()
			if top != empty {
				delay := top.eta.Sub(time.Now())
				if delay < 0 {
					t = heap.Pop(s.t).(item).t
					out = s.pub
				} else {
					timer = time.After(delay)
					log.Printf("next pop in %s", delay)
				}
			}
		}

		select { // carefull the order matters
		case <-s.quit:
			return
		case out <- t:
			out = nil
		case <-timer:
			t = heap.Pop(s.t).(item).t
			out = s.pub
		case it, ok := <-s.backdoor:
			if !ok {
				return
			}
			if when := s.schedule(it.eta, it.t); when <= 0 {
				out = s.pub
				t = it.t
			}
		case it, ok := <-s.sub:
			if !ok {
				return
			}
			if when := s.scheduleTask(it); when <= 0 {
				out = s.pub
				t = it
			}
		}
	}
}

// Close implements the subscriber interface. It stops publishing new tasks.
func (s *Scheduler) Close() error {
	log.Println("close sched Close.")
	close(s.quit)
	return nil
}

package gocelery

import (
	"container/heap"
	"log"
	"time"

	"github.com/gwik/gocelery/types"
)

type table []*types.Task

func (t table) Len() int { return len(t) }

func (t table) Less(i, j int) bool {
	return t[i].Msg.ETA.Before(t[j].Msg.ETA)
}

func (t table) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *table) Push(task interface{}) {
	*t = append(*t, task.(*types.Task))
}

func (t *table) Pop() interface{} {
	old := *t
	n := len(old)
	task := old[n-1]
	*t = old[0 : n-1]
	return task
}

func (t table) Top() *types.Task {
	if len(t) == 0 {
		return nil
	}
	return t[0]
}

type Scheduler struct {
	t       *table
	inChan  chan *types.Task
	outChan chan *types.Task
}

func NewScheduler() *Scheduler {
	t := make(table, 0, 32)
	heap.Init(&t)

	return &Scheduler{
		t:       &t,
		inChan:  make(chan *types.Task),
		outChan: make(chan *types.Task),
	}
}

func (s *Scheduler) Add(task *types.Task) {
	s.inChan <- task
}

func (s *Scheduler) Start() {
	go func() {
		never := make(chan time.Time)
		for {
			var timer <-chan time.Time
			top := s.t.Top()
			if top != nil {
				delay := top.Msg.ETA.Sub(time.Now())
				timer = time.After(delay)
				log.Printf("next pop in %s", delay)
			} else {
				timer = never
				log.Println("wait for tasks...")
			}

			select {
			case task, ok := <-s.inChan:
				if !ok {
					close(s.outChan)
					return
				}
				heap.Push(s.t, task)
			case <-timer:
				s.outChan <- heap.Pop(s.t).(*types.Task)
			}
		}
	}()
}

func (s *Scheduler) Stop() {
	close(s.inChan)
}

func (s *Scheduler) Consume() <-chan *types.Task {
	return s.outChan
}

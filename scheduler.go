package celery

import (
	"container/heap"
	"log"
	"time"

	"github.com/gwik/gocelery/types"
)

type table []*types.Message

func (t table) Len() int { return len(t) }

func (t table) Less(i, j int) bool {
	return t[i].ETA.Before(t[j].ETA)
}

func (t table) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *table) Push(msg interface{}) {
	*t = append(*t, msg.(*types.Message))
}

func (t *table) Pop() interface{} {
	old := *t
	n := len(old)
	msg := old[n-1]
	*t = old[0 : n-1]
	return msg
}

func (t table) Top() *types.Message {
	if len(t) == 0 {
		return nil
	}
	return t[0]
}

type Scheduler struct {
	t       *table
	inChan  chan *types.Message
	outChan chan *types.Message
}

func NewScheduler() *Scheduler {
	t := make(table, 0, 32)
	heap.Init(&t)

	return &Scheduler{
		t:       &t,
		inChan:  make(chan *types.Message),
		outChan: make(chan *types.Message),
	}
}

func (s *Scheduler) Add(msg *types.Message) {
	s.inChan <- msg
}

func (s *Scheduler) Start() {
	go func() {
		never := make(chan time.Time)
		for {
			var timer <-chan time.Time
			top := s.t.Top()
			if top != nil {
				delay := top.ETA.Sub(time.Now())
				timer = time.After(delay)
				log.Printf("next pop in %v", delay)
			} else {
				timer = never
				log.Println("wait for tasks...")
			}

			select {
			case msg, ok := <-s.inChan:
				if !ok {
					close(s.outChan)
					return
				}
				heap.Push(s.t, msg)
			case <-timer:
				s.outChan <- heap.Pop(s.t).(*types.Message)
			}
		}
	}()
}

func (s *Scheduler) Stop() {
	close(s.inChan)
}

func (s *Scheduler) Consume() <-chan *types.Message {
	return s.outChan
}

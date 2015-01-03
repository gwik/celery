/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package gocelery

import (
	"log"
	"sync/atomic"
	"time"

	"github.com/gwik/gocelery/syncutil"
	"github.com/gwik/gocelery/types"
)

type Worker struct {
	handlerReg map[string]types.HandleFunc
	sub        <-chan types.Task
	gate       *syncutil.Gate
	results    chan *types.Result
	quit       chan struct{}

	completed uint64
	running   uint32
}

type key int

const (
	dispatchedAt key = 0
)

func NewWorker(concurrency int, sub types.Subscriber) *Worker {
	return &Worker{
		handlerReg: make(map[string]types.HandleFunc),
		sub:        sub.Subscribe(),
		gate:       syncutil.NewGate(concurrency),
		results:    make(chan *types.Result),
		quit:       make(chan struct{}),
	}
}

func (w *Worker) Results() <-chan *types.Result {
	return w.results
}

func (w *Worker) Register(name string, h types.HandleFunc) {
	if _, exists := w.handlerReg[name]; exists {
		log.Fatalf("Already registered: %s.", name)
	}
	w.handlerReg[name] = h
}

func (w *Worker) Start() {
	go w.loop()
}

func (w *Worker) loop() {
	go func() {
		for {
			<-time.After(time.Second * 1)
			log.Printf("%d jobs completed, %d running", atomic.LoadInt64(&w.completed), atomic.LoadInt32(&w.running))
		}
	}()

	for {
		select {
		case t := <-w.sub: // TODO deal with channel closing
			log.Printf("Dispatch %s", t.Msg().Task)
			h, exists := w.handlerReg[t.Msg().Task]
			if !exists {
				log.Printf("No handler for task: %s", t.Msg().Task)
				t.Ack() // FIXME
			} else {
				w.gate.Start()
				atomic.AddUInt32(&w.running, 1)
				go func(t types.Task, h types.HandleFunc) {
					defer atomic.AddUInt32(&w.running, ^uint32(0)) // -1
					defer atomic.AddUInt64(&w.completed, 1)
					defer w.gate.Done()
					h(t) // send function return through result
					t.Ack()
					//w.results <- types.NewResult(j.task)
				}(t, h)
			}
		case <-w.quit:
			return
		}
		// TODO quit
	}
}

/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package gocelery

import (
	"log"

	"github.com/gwik/gocelery/syncutil"
	"github.com/gwik/gocelery/types"
)

type job struct {
	task    types.Task
	handler types.HandleFunc
}

type Worker struct {
	handlerReg map[string]types.HandleFunc
	sub        <-chan types.Task
	gate       *syncutil.Gate
	results    chan *types.Result
	quit       chan struct{}
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
	for {
		select {
		case t := <-w.sub: // TODO deal with channel closing
			log.Printf("Dispatch %s", t.Msg().Task)
			h, exists := w.handlerReg[t.Msg().Task]
			if !exists {
				log.Printf("No handler for task: %s", t.Msg().Task)
				t.Ack() // FIXME
			} else {
				j := &job{t, h}
				w.gate.Start()
				go func(j *job) {
					defer w.gate.Done()
					j.handler(j.task) // send function return through result
					j.task.Ack()
					//w.results <- types.NewResult(j.task)
				}(j)
			}
		case <-w.quit:
			return
		}
		// TODO quit
	}
}

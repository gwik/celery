/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package gocelery

import (
	"fmt"
	"log"

	"github.com/gwik/gocelery/syncutil"
	"github.com/gwik/gocelery/types"
)

type job struct {
	task    *types.Task
	handler types.HandleFunc
}

type Worker struct {
	handlerReg map[string]types.HandleFunc
	ch         chan *job
	gate       *syncutil.Gate
	resultChan chan<- *types.Result
}

type key int

const (
	dispatchedAt key = 0
)

func NewWorker(concurrency int, resultChan chan<- *types.Result) *Worker {
	return &Worker{
		handlerReg: make(map[string]types.HandleFunc),
		ch:         make(chan *job),
		gate:       syncutil.NewGate(concurrency),
		resultChan: resultChan,
	}
}

func (w *Worker) Register(name string, h types.HandleFunc) {
	if _, exists := w.handlerReg[name]; exists {
		log.Fatalf("Already registered: %s.", name)
	}
	w.handlerReg[name] = h
}

// Dispatch run the job.
func (w *Worker) Dispatch(t *types.Task) error {
	log.Printf("Dispatch %s", t.Msg.Task)
	h, exists := w.handlerReg[t.Msg.Task]
	if !exists {
		return fmt.Errorf("No handler for task: %s", t.Msg.Task)
	}
	w.ch <- &job{t, h}
	return nil
}

func (w *Worker) Start() {
	go w.work()
}

func (w *Worker) Stop() {
}

func (w *Worker) work() {
	for {
		select {
		case job := <-w.ch:
			w.gate.Start()
			go func() {
				defer w.gate.Done()
				job.handler(job.task)
				w.resultChan <- types.NewResult(job.task)
			}()
			// case <-w.ctx.Done():
			// 	return
		}
	}
}

/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"fmt"
	"log"
	"time"

	"github.com/gwik/gocelery/syncutil"
	"github.com/gwik/gocelery/types"

	"code.google.com/p/go.net/context"
)

type job struct {
	msg *types.Message
	ctx context.Context
	h   types.HandleFunc
	r   chan<- types.Result
}

type Worker struct {
	handlerReg map[string]types.HandleFunc
	ch         chan *job
	resultChan chan types.Result
	ctx        context.Context
	cancelF    context.CancelFunc
	gate       *syncutil.Gate
}

type key int

const (
	dispatchedAt key = 0
)

func NewWorker(concurrency int) *Worker {
	ctx, cancelF := context.WithCancel(context.Background())
	return &Worker{
		handlerReg: make(map[string]types.HandleFunc),
		ch:         make(chan *job),
		resultChan: make(chan types.Result),
		ctx:        ctx,
		cancelF:    cancelF,
		gate:       syncutil.NewGate(concurrency),
	}
}

func (w *Worker) Register(name string, h types.HandleFunc) {
	if _, exists := w.handlerReg[name]; exists {
		log.Fatalf("Already registered: %s.", name)
	}
	w.handlerReg[name] = h
}

// Dispatch run the job.
func (w *Worker) Dispatch(msg *types.Message, r chan<- types.Result) error {
	log.Printf("Dispatch %s", msg.Task)
	h, exists := w.handlerReg[msg.Task]
	if !exists {
		return fmt.Errorf("No handler for task: %s", msg.Task)
	}
	ctx := context.WithValue(w.ctx, dispatchedAt, time.Now())
	w.ch <- &job{msg, ctx, h, r}
	return nil
}

func (w *Worker) Start() {
	go w.work()
}

func (w *Worker) Stop() {
	w.cancelF()
}

func (w *Worker) work() {
	for {
		select {
		case job := <-w.ch:
			w.gate.Start()
			go func() {
				defer w.gate.Done()
				job.r <- job.h(job.ctx, job.msg)
			}()
		case <-w.ctx.Done():
			return
		}
	}
}

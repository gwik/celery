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
	sub        <-chan types.TaskContext
	gate       *syncutil.Gate
	results    chan *types.Result
	quit       chan struct{}
	backend    types.Backend

	completed uint64
	running   uint32
}

type key int

const (
	keyTask key = 0
)

func NewWorker(concurrency int, sub types.Subscriber, backend types.Backend) *Worker {
	return &Worker{
		handlerReg: make(map[string]types.HandleFunc),
		sub:        sub.Subscribe(),
		gate:       syncutil.NewGate(concurrency),
		results:    make(chan *types.Result),
		quit:       make(chan struct{}),
		backend:    backend,
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

const (
	taskKey = key(0)
)

func MsgFromContext(ctx context.Context) {
	return ctx.Value(keyTask).(*types.Task).Msg()
}

func setJobContext(ctx context.Context, t *types.Task) context.Context {
	return ctx.WithValue(taskKey, t)
}

func (w *Worker) loop() {
	go func() {
		for {
			<-time.After(time.Second * 1)
			log.Printf("%d jobs completed, %d running", atomic.LoadUint64(&w.completed), atomic.LoadUint32(&w.running))
		}
	}()

	for {
		select {
		case t, ok := <-w.sub:
			if !ok {
				continue
			}

			task, ctx := t.T, t.C

			log.Printf("Dispatch %s", msg.Task)
			h, exists := w.handlerReg[msg.Task]

			if !exists {
				log.Printf("No handler for task: %s", msg.Task)
				t.T.Reject(true)
			} else {

				w.gate.Start()
				atomic.AddUint32(&w.running, 1)
				jobCtx := setContext(ctx, task)
				// jobCtx, cancel := context.WithCancel(jobCtx)

				go func() {

					defer atomic.AddUint32(&w.running, ^uint32(0)) // -1
					defer atomic.AddUint64(&w.completed, 1)
					defer w.gate.Done()

					v := h(jobCtx, msg.Args, msg.KwArgs) // send function return through result
					t.Ack()
					log.Printf("%s result: %v", msg.ID, v)

					w.backend.Publish(t, &types.ResultMeta{
						Status: types.SUCCESS,
						Result: v,
						TaskId: msg.ID,
					})
					//w.results <- types.NewResult(j.task)
				}()
			}
		case <-w.quit:
			return
		}
		// TODO quit
	}
}

/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/gwik/celery/syncutil"
	"github.com/gwik/celery/types"
	"golang.org/x/net/context"
)

type Worker struct {
	handlerReg map[string]types.HandleFunc
	sub        <-chan types.TaskContext
	gate       *syncutil.Gate
	results    chan *types.Result
	quit       chan struct{}
	backend    types.Backend
	retry      *Scheduler

	// stats
	completed uint64
	running   uint32
}

type key int

const (
	keyTask key = 0
)

func NewWorker(concurrency int, sub types.Subscriber, backend types.Backend, retry *Scheduler) *Worker {
	// XXX: FIXME scheduler should be exported or interface.
	return &Worker{
		handlerReg: make(map[string]types.HandleFunc),
		sub:        sub.Subscribe(),
		gate:       syncutil.NewGate(concurrency),
		results:    make(chan *types.Result),
		quit:       make(chan struct{}),
		backend:    backend,
		retry:      retry,
	}
}

func (w *Worker) Results() <-chan *types.Result {
	return w.results
}

func (w *Worker) RegisterFunc(name string, fn interface{}) {

	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		panic(fmt.Sprintf("%v is not a func", fn))
	}

	fmt.Printf("%v %s", fn, name)

	var wrapper types.HandleFunc = func(ctx context.Context, args []interface{}, _ map[string]interface{}) (interface{}, error) {
		in := make([]reflect.Value, len(args)+1)
		in[0] = reflect.ValueOf(ctx)
		for i, arg := range args {
			in[i+1] = reflect.ValueOf(arg)
		}

		var result interface{}
		var err error

		res := vfn.Call(in)

		if !res[0].IsNil() {
			result = res[0].Interface()
		}

		if !res[1].IsNil() {
			err = res[1].Interface().(error)
		}

		return result, err
	}

	w.Register(name, wrapper)
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
	taskKey  = key(0)
	retryKey = key(1)
)

func retries(ctx context.Context) int {
	val := ctx.Value(retryKey)
	if val != nil {
		return val.(int)
	}
	return 0
}

func retry(ctx context.Context) context.Context {
	return context.WithValue(ctx, retryKey, retries(ctx)+1)
}

func MsgFromContext(ctx context.Context) types.Message {
	msg := ctx.Value(keyTask).(types.Task).Msg()
	msg.Retries = retries(ctx)
	return msg
}

func setJobContext(parent context.Context, t types.Task) context.Context {
	ctx := context.WithValue(parent, taskKey, t)
	ctx = context.WithValue(ctx, retryKey, retries(ctx))
	return ctx
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
			msg := task.Msg()

			log.Printf("Dispatch %s", msg.Task)
			h, exists := w.handlerReg[msg.Task]

			if !exists {
				log.Printf("No handler for task: %s", msg.Task)
				task.Reject(true)
			} else {

				w.gate.Start()
				atomic.AddUint32(&w.running, 1)
				jobCtx := setJobContext(ctx, task)
				// jobCtx, cancel := context.WithCancel(jobCtx)

				go func() {

					defer atomic.AddUint32(&w.running, ^uint32(0)) // -1
					defer w.gate.Done()
					defer func() {
						if r := recover(); r != nil {
							log.Printf("%s: %s", r, debug.Stack())
						}
						task.Reject(false)
					}()
					v, err := h(jobCtx, msg.Args, msg.KwArgs) // send function return through result

					if err != nil {

						if retryErr, ok := err.(Retryable); ok {
							log.Printf("retry %s %v", msg.ID, retryErr.At())
							c := retry(ctx)
							w.retry.Publish(retryErr.At(), types.TaskContext{T: task, C: c})
							return
						}

						log.Printf("job %s/%s failed: %v", msg.Task, msg.ID, err)
						task.Reject(false)
						return
					}

					task.Ack()
					log.Printf("job %s/%s succeeded result: %v", msg.Task, msg.ID, v)

					w.backend.Publish(task, &types.ResultMeta{
						Status: types.SUCCESS,
						Result: v,
						TaskId: msg.ID,
					})
					atomic.AddUint64(&w.completed, 1)
					//w.results <- types.NewResult(j.task)
				}()
			}
		case <-w.quit:
			return
		}
		// TODO quit
	}
}

type Retryable interface {
	At() time.Time
	error
}

type RetryError struct {
	err error
	at  time.Time
}

var _ Retryable = (*RetryError)(nil)

func (re *RetryError) Error() string {
	return re.err.Error()
}

func (re *RetryError) At() time.Time {
	return re.at
}

func Retry(err error, delay time.Duration) *RetryError {
	return &RetryError{
		err: err,
		at:  time.Now().Add(delay),
	}
}

func Again(reason string, delay time.Duration) *RetryError {
	return &RetryError{
		err: errors.New(reason),
		at:  time.Now().Add(delay),
	}
}

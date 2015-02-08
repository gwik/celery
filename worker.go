/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/gwik/celery/syncutil"
	"golang.org/x/net/context"
)

type key int

const (
	taskKey  = key(0)
	retryKey = key(1)
)

type retryTask struct {
	Task
	retries int
}

func retries(t Task) int {
	if rt, ok := t.(retryTask); ok {
		return rt.retries
	}
	return 0
}

func retry(t Task) Task {
	if rt, ok := t.(retryTask); ok {
		rt.retries += 1
		return rt
	}

	return retryTask{t, 1}
}

// MsgFromContext can be called within task function to get the
// celery message from the context.
func MsgFromContext(ctx context.Context) Message {
	task := ctx.Value(taskKey).(Task)
	msg := task.Msg()
	msg.Retries = retries(task)
	return msg
}

func jobContext(t Task) context.Context {
	ctx := context.WithValue(t, taskKey, t)
	ctx = context.WithValue(ctx, retryKey, retries(t))
	return ctx
}

type cancelProxy struct {
	context.Context
	quit   chan struct{}
	cancel chan struct{}
}

// A context which is cancelled when cancel chan is closed or parent is done.
func newCancelProxy(ctx context.Context, cancel <-chan struct{}) *cancelProxy {
	cd := &cancelProxy{ctx, make(chan struct{}), make(chan struct{})}

	go func() {
		select {
		case <-cd.Context.Done():
			close(cd.cancel)
			return
		case <-cancel:
			close(cd.cancel)
			return
		case <-cd.quit:
			return
		}
	}()
	return cd
}

func (cd *cancelProxy) Done() <-chan struct{} {
	return cd.cancel
}

func (cd *cancelProxy) Close() {
	close(cd.quit)
}

// Worker runs tasks and publish their results.
type Worker struct {
	handlerReg map[string]HandleFunc
	sub        <-chan Task
	gate       *syncutil.Gate
	results    chan *Result
	quit       chan struct{}
	backend    Backend
	retry      *Scheduler

	// stats
	completed uint64
	running   uint32
}

// NewWorker returns a new worker.
//
// concurrency is the number of concurrent goroutines that run tasks.
//
// sub is the subscriber from which the tasks are coming (usually a Scheduler)
//
// Results are published to backend.
//
// retry is a Scheduler used for tasks that are retried after some time (usually same as sub).
// It can be nil, in which case the tasks are not retried.
func NewWorker(concurrency int, sub Subscriber, backend Backend, retry *Scheduler) *Worker {
	return &Worker{
		handlerReg: make(map[string]HandleFunc),
		sub:        sub.Subscribe(),
		gate:       syncutil.NewGate(concurrency),
		results:    make(chan *Result),
		quit:       make(chan struct{}),
		backend:    backend,
		retry:      retry,
	}
}

// RegisterFunc registers a function that must have a golang.org/x/context.Context as first argument.
// Other arguments will be passed from the message arguments.
// The return must be some kind of (interface{}, error). First returned argument must be serializable
// by the backend to publish the result.
//
// 		w.RegisterFunc("tasks.add", func(ctx context.Context, a float64, b float64) (float64, error) {
//			return a + b, nil
// 		})
//
// It should be used before start.
func (w *Worker) RegisterFunc(name string, fn interface{}) {
	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		panic(fmt.Sprintf("%v is not a func", fn))
	}

	var wrapper = func(ctx context.Context, args []interface{}, _ map[string]interface{}) (interface{}, error) {
		in := make([]reflect.Value, len(args)+1)
		in[0] = reflect.ValueOf(ctx)
		for i, arg := range args {
			in[i+1] = reflect.ValueOf(arg)
		}

		res := vfn.Call(in)

		var result interface{}
		var err error
		if res[0].IsValid() {
			result = res[0].Interface()
		}
		if res[1].IsValid() {
			errInf := res[1].Interface()
			if errInf != nil {
				err = errInf.(error)
			}
		}
		return result, err
	}

	w.Register(name, wrapper)
}

// Register registers a HandleFunc function with the given name. It should be
// used before Start.
func (w *Worker) Register(name string, h HandleFunc) {
	if _, exists := w.handlerReg[name]; exists {
		log.Fatalf("Already registered: %s.", name)
	}
	w.handlerReg[name] = h
}

// Start starts the worker. It should be called after task registration is complete.
func (w *Worker) Start() {
	go w.loop()
}

// Close closes the worker, it stops processing new tasks.
func (w *Worker) Close() error {
	close(w.quit)
	return nil
}

func (w *Worker) loop() {
	go func() {
		for {
			<-time.After(time.Second * 1)
			log.Printf("%d jobs completed, %d running",
				atomic.LoadUint64(&w.completed), atomic.LoadUint32(&w.running))
		}
	}()

	for {
		select {
		case task, ok := <-w.sub:
			if !ok {
				return
			}
			msg := task.Msg()
			log.Printf("Dispatch %s", msg.Task)

			select {
			case <-task.Done():
				log.Printf("Canceled: %s", msg.ID)
				continue
			default:
			}

			h, exists := w.handlerReg[msg.Task]
			if !exists {
				log.Printf("No handler for task: %s", msg.Task)
				task.Reject(false)
				continue
			}

			w.gate.Start()
			atomic.AddUint32(&w.running, 1)

			go w.run(task, h)

		case <-w.quit:

			return
		}
	}
}

func (w *Worker) run(task Task, h HandleFunc) {
	defer atomic.AddUint32(&w.running, ^uint32(0)) // -1
	defer w.gate.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("%s: %s", r, debug.Stack())
			task.Reject(false)
		}
	}()

	ctx := newCancelProxy(jobContext(task), w.quit)
	defer ctx.Close()

	msg := task.Msg()
	v, err := h(ctx, msg.Args, msg.KwArgs) // send function return through result

	select {
	case <-ctx.Done():
		log.Printf("after, done: %s do nothing.", msg.ID)
		return
	default:
	}

	if err != nil {
		if retryErr, ok := err.(Retryable); ok {
			if w.retry == nil {
				log.Printf("no scheduler, won't retry %s/%s", msg.Task, msg.ID)
				return
			}
			log.Printf("retry %s %v", msg.ID, retryErr.At())
			w.retry.Publish(retryErr.At(), retry(task))
			return
		}
		log.Printf("job %s/%s failed: %v", msg.Task, msg.ID, err)
		task.Reject(false)
		return
	}

	task.Ack()
	log.Printf("job %s/%s succeeded result: %v", msg.Task, msg.ID, v)
	w.backend.Publish(task, &ResultMeta{
		Status: SUCCESS,
		Result: v,
		TaskId: msg.ID,
	})
	atomic.AddUint64(&w.completed, 1)
}

// Retryable is the interface for retryable errors.
type Retryable interface {
	error
	At() time.Time // the time at which the task should be retried.
}

// RetryError is a Retryable error implementation that wraps other errors.
type RetryError struct {
	error
	at time.Time
}

var _ Retryable = (*RetryError)(nil)

// At implements the Retryable interface. It is the time
func (re *RetryError) At() time.Time {
	return re.at
}

// RetryNTimes is helper function that return a Retryable error if tried less than n times.
// It will retry in delay power the number of previous tries.
func RetryNTimes(n int, ctx context.Context, err error, delay time.Duration) error {
	msg := MsgFromContext(ctx)
	if msg.Retries > n {
		return err
	}
	return Retry(err, time.Duration(math.Pow(delay.Seconds(), float64(msg.Retries+1)))*time.Second)
}

// Retry is a helper function to retry a task on error after delay.
func Retry(err error, delay time.Duration) Retryable {
	return &RetryError{
		err,
		time.Now().Add(delay),
	}
}

// Again is a helper function to retry a task with a reason after delay.
func Again(reason string, delay time.Duration) Retryable {
	return &RetryError{
		errors.New(reason),
		time.Now().Add(delay),
	}
}

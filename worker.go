/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
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

type key int

const (
	taskKey  = key(0)
	retryKey = key(1)
)

type retryTask struct {
	types.Task
	retries int
}

func retries(t types.Task) int {
	if rt, ok := t.(retryTask); ok {
		return rt.retries
	}
	return 0
}

func retry(t types.Task) types.Task {
	if rt, ok := t.(retryTask); ok {
		rt.retries += 1
		return rt
	}

	return retryTask{t, 1}
}

func MsgFromContext(ctx context.Context) types.Message {
	task := ctx.Value(taskKey).(types.Task)
	msg := task.Msg()
	msg.Retries = retries(task)
	return msg
}

func jobContext(t types.Task) context.Context {
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

type Worker struct {
	handlerReg map[string]types.HandleFunc
	sub        <-chan types.Task
	gate       *syncutil.Gate
	results    chan *types.Result
	quit       chan struct{}
	backend    types.Backend
	retry      *Scheduler

	// stats
	completed uint64
	running   uint32
}

func NewWorker(concurrency int, sub types.Subscriber, backend types.Backend, retry *Scheduler) *Worker {
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

func (w *Worker) RegisterFunc(name string, fn interface{}) {
	vfn := reflect.ValueOf(fn)
	if vfn.Kind() != reflect.Func {
		panic(fmt.Sprintf("%v is not a func", fn))
	}

	fmt.Printf("%v %s", fn, name)

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

func (w *Worker) Register(name string, h types.HandleFunc) {
	if _, exists := w.handlerReg[name]; exists {
		log.Fatalf("Already registered: %s.", name)
	}
	w.handlerReg[name] = h
}

func (w *Worker) Start() {
	go w.loop()
}

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

func (w *Worker) run(task types.Task, h types.HandleFunc) {
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
	w.backend.Publish(task, &types.ResultMeta{
		Status: types.SUCCESS,
		Result: v,
		TaskId: msg.ID,
	})
	atomic.AddUint64(&w.completed, 1)
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

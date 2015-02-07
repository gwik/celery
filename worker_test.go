package celery

import (
	"testing"
	"time"

	"github.com/gwik/celery/types"
	"golang.org/x/net/context"
	"sync/atomic"
)

type testSubscriber struct {
	Ch chan types.Task
}

func newTestSubscriber() *testSubscriber {
	return &testSubscriber{
		Ch: make(chan types.Task),
	}
}

func (s *testSubscriber) Subscribe() <-chan types.Task {
	return s.Ch
}

func (s *testSubscriber) Close() error {
	close(s.Ch)
	return nil
}

type testTask struct {
	context.Context
	msg    types.Message
	ack    func() error
	reject func(bool) error
}

func (t testTask) Ack() error {
	return t.ack()
}

func (t testTask) Reject(requeue bool) error {
	return t.reject(requeue)
}

func (t testTask) Msg() types.Message {
	return t.msg
}

type testBackend struct {
	done      chan struct{}
	published map[string]types.Result
}

func (t *testBackend) Publish(task types.Task, r *types.ResultMeta) {
	t.published[task.Msg().ID] = r.Result
	close(t.done)
}

func TaskStub(msg types.Message) types.Task {
	return &testTask{
		context.Background(),
		msg,
		func() error { return nil },
		func(bool) error { return nil },
	}
}

func TestRegisterAndRunTask(t *testing.T) {
	sub := newTestSubscriber()
	results := make(map[string]types.Result)
	done := make(chan struct{})
	backend := &testBackend{done, results}
	worker := NewWorker(1, sub, backend, nil)

	worker.Register("job", func(ctx context.Context, args []interface{}, kwargs map[string]interface{}) (interface{}, error) {

		if len(args) != 2 {
			t.Fatalf("Expected len `args` to be 2 was: %d", len(args))
		}

		if len(kwargs) != 2 {
			t.Fatalf("Expected len `kwargs` to be 2 was: %d", len(kwargs))
		}
		if val, ok := kwargs["one"]; ok {
			if i, ok := val.(int); ok {
				if i != 1 {
					t.Errorf("Expected `one` value to be 1 was: %d", i)
				}
			} else {
				t.Error("Expected `one` value to be an int")
			}
		} else {
			t.Error("Expected `kwargs` to have key one")
		}

		return "result", nil
	})

	worker.Start()
	defer worker.Close()

	sub.Ch <- TaskStub(types.Message{
		Task:   "job",
		ID:     "job1",
		Args:   []interface{}{"foo", 1},
		KwArgs: map[string]interface{}{"one": 1, "two": nil},
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout.")
	}

	if res, exists := results["job1"]; exists {
		if res.(string) != "result" {
			t.Fatalf("expected result to be `result`")
		}
	} else {
		t.Fatal("expected a result")
	}

}

func TestRegisterFuncAndRunTask(t *testing.T) {
	sub := newTestSubscriber()
	done := make(chan struct{})
	results := make(map[string]types.Result)
	worker := NewWorker(10, sub, &testBackend{done, results}, nil)

	worker.RegisterFunc("job", func(ctx context.Context, val float64, list []string) (types.Result, error) {
		return []interface{}{val, list}, nil
	})

	worker.Start()
	defer worker.Close()

	sub.Ch <- TaskStub(types.Message{
		Task: "job",
		ID:   "job1",
		Args: []interface{}{float64(2.86), []string{"a", "b", "c"}},
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout.")
	}

	if res, exists := results["job1"]; exists {
		res := res.([]interface{})
		if v, ok := res[0].(float64); !ok || v != 2.86 {
			t.Errorf("expected result[0] to be 2.86 float64 was: %v", res)
		}
		if _, ok := res[1].([]string); !ok {
			t.Fatalf("expected result[1] to be a list of strings")
		}
	} else {
		t.Fatal("expected a result")
	}

}

func TestRetryTask(t *testing.T) {
	sub := newTestSubscriber()
	done := make(chan struct{})
	results := make(map[string]types.Result)
	sched := NewScheduler(sub)
	worker := NewWorker(1, sched, &testBackend{done, results}, sched)

	var count uint32

	worker.RegisterFunc("job", func(ctx context.Context) (types.Result, error) {
		atomic.AddUint32(&count, 1)
		msg := MsgFromContext(ctx)
		if msg.Retries < 3 {
			return nil, Again("cause I want to.", time.Duration(10)*time.Millisecond)
		}
		defer close(done)
		return atomic.LoadUint32(&count), nil
	})

	worker.Start()
	defer worker.Close()

	sub.Ch <- TaskStub(types.Message{
		Task: "job",
		ID:   "job1",
		Args: []interface{}{},
	})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout.")
	}

	if res, exists := results["job1"]; exists {
		if res.(uint32) != count && count != 2 {
			t.Errorf("expected retries to be 2 was: %d", res.(uint32))
		}
	} else {
		t.Error("result should be set.")
	}

}

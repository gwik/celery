package celery

import (
	"testing"
	"time"

	"golang.org/x/net/context"
	"sync"
	"sync/atomic"
)

type testSubscriber struct {
	Ch chan Task
}

func newTestSubscriber() *testSubscriber {
	return &testSubscriber{
		Ch: make(chan Task),
	}
}

func (s *testSubscriber) Subscribe() <-chan Task {
	return s.Ch
}

func (s *testSubscriber) Close() error {
	close(s.Ch)
	return nil
}

type testTask struct {
	context.Context
	msg    Message
	ack    func() error
	reject func(bool) error
}

func (t testTask) Ack() error {
	return t.ack()
}

func (t testTask) Reject(requeue bool) error {
	return t.reject(requeue)
}

func (t testTask) Msg() Message {
	return t.msg
}

type testBackend struct {
	mu        sync.Mutex
	published map[string]chan Result
}

func newTestBackend() *testBackend {
	return &testBackend{
		mu:        sync.Mutex{},
		published: make(map[string]chan Result),
	}
}

func (t *testBackend) resultChan(id string) chan Result {
	t.mu.Lock()
	defer t.mu.Unlock()

	if ch, ok := t.published[id]; ok {
		return ch
	}
	ch := make(chan Result, 1)
	t.published[id] = ch
	return ch
}

func (t *testBackend) Notify(id string) <-chan Result {
	return t.resultChan(id)
}

func (t *testBackend) Publish(task Task, r *ResultMeta) {
	ch := t.resultChan(task.Msg().ID)
	ch <- r.Result
}

func TaskStub(msg Message) Task {
	return &testTask{
		context.Background(),
		msg,
		func() error { return nil },
		func(bool) error { return nil },
	}
}

func TestRegisterAndRunTask(t *testing.T) {
	sub := newTestSubscriber()
	results := newTestBackend()
	worker := NewWorker(1, sub, results, nil)

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

	sub.Ch <- TaskStub(Message{
		Task:   "job",
		ID:     "job1",
		Args:   []interface{}{"foo", 1},
		KwArgs: map[string]interface{}{"one": 1, "two": nil},
	})

	select {
	case res := <-results.Notify("job1"):
		if res.(string) != "result" {
			t.Fatalf("expected result to be `result`")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout.")
	}

}

func TestRegisterFuncAndRunTask(t *testing.T) {
	sub := newTestSubscriber()
	results := newTestBackend()
	worker := NewWorker(10, sub, results, nil)

	worker.RegisterFunc("job", func(ctx context.Context, val float64, list []string) ([]interface{}, error) {
		return []interface{}{val, list}, nil
	})

	worker.Start()
	defer worker.Close()

	sub.Ch <- TaskStub(Message{
		Task: "job",
		ID:   "job1",
		Args: []interface{}{float64(2.86), []string{"a", "b", "c"}},
	})

	select {
	case r := <-results.Notify("job1"):
		res := r.([]interface{})
		if v, ok := res[0].(float64); !ok || v != 2.86 {
			t.Errorf("expected result[0] to be 2.86 float64 was: %v", res)
		}
		if _, ok := res[1].([]string); !ok {
			t.Fatalf("expected result[1] to be a list of strings")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout.")
	}

}

func TestRetryTask(t *testing.T) {
	sub := newTestSubscriber()
	results := newTestBackend()
	sched := NewScheduler(sub)
	worker := NewWorker(1, sched, results, sched)

	var count uint32

	worker.RegisterFunc("job", func(ctx context.Context) (uint32, error) {
		atomic.AddUint32(&count, 1)
		msg := MsgFromContext(ctx)
		if msg.Retries < 3 {
			return 0, Again("cause I want to.", time.Duration(10)*time.Millisecond)
		}
		return atomic.LoadUint32(&count), nil
	})

	worker.Start()
	defer worker.Close()

	sub.Ch <- TaskStub(Message{
		Task: "job",
		ID:   "job1",
		Args: []interface{}{},
	})

	select {
	case res := <-results.Notify("job1"):
		if res.(uint32) != count && count != 2 {
			t.Errorf("expected retries to be 2 was: %d", res.(uint32))
		}
	case <-time.After(time.Second):
		t.Fatal("timeout.")
	}

}

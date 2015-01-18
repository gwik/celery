package main

import (
	"log"
	"time"

	"github.com/gwik/celery"
	"github.com/gwik/celery/types"
	"golang.org/x/net/context"
)

func two(context context.Context, args []interface{}, kwargs map[string]interface{}) (interface{}, error) {
	<-time.After(time.Second * 10)
	return nil, nil
}

func add(context context.Context, args []interface{}, kwargs map[string]interface{}) (interface{}, error) {
	return args[0].(float64) + args[1].(float64), nil
}

func tryagain(ctx context.Context, args []interface{}, _ map[string]interface{}) (interface{}, error) {

	delay, count := args[0].(float64), int(args[0].(float64))
	msg := celery.MsgFromContext(ctx)
	if msg.Retries < count {
		log.Printf("retry %s", msg.ID)
		return nil, celery.Again("cause I want to.", time.Duration(delay)*time.Second)
	}

	return nil, nil
}

func byname(ctx context.Context, foo string, bar float64) (string, error) {
	log.Printf("call by name %v %v %v", ctx, foo, bar)
	return "results string", nil
}

func paniking(ctx context.Context, err string) (string, error) {
	panic(err)
	return "", nil
}

type noopBackend struct{}

func (noopBackend) Publish(types.Task, *types.ResultMeta) {}

func Consume(queueName string) error {

	sched := celery.NewScheduler(celery.AMQPSubscriber("celery"))

	// backend := NewAMQPBackend()
	worker := celery.NewWorker(10, sched, noopBackend{}, sched)

	worker.Register("tasks.add", add)
	worker.Register("tasks.two", two)
	worker.Register("tasks.tryagain", tryagain)
	worker.RegisterFunc("tasks.byname", byname)
	worker.RegisterFunc("tasks.panic", paniking)

	worker.Start()

	forever := make(chan struct{})
	<-forever

	return nil
}

func main() {
	Consume("celery")
}

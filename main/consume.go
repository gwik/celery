package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	"golang.org/x/net/context"

	_ "github.com/gwik/celery/jsonmessage"

	"github.com/gwik/celery"
	"github.com/gwik/celery/amqpconsumer"
	"github.com/gwik/celery/amqputil"
	"github.com/gwik/celery/types"
)

func two(context context.Context, args []interface{}, kwargs map[string]interface{}) (interface{}, error) {
	<-time.After(time.Millisecond * time.Duration(10))
	return nil, nil
}

func add(context context.Context, args []interface{}, kwargs map[string]interface{}) (interface{}, error) {
	return args[0].(float64) + args[1].(float64), nil
}

func tryagain(ctx context.Context, args []interface{}, _ map[string]interface{}) (interface{}, error) {

	delay, count := args[0].(float64), int(args[1].(float64))
	msg := celery.MsgFromContext(ctx)
	log.Printf("msg retries: %d/%d", msg.Retries, count)
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

	if os.Getenv("LOG_OFF") != "" {
		log.SetOutput(ioutil.Discard)
	}

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	retry := amqputil.NewRetry(os.Getenv("AMQP_URL"), nil, 2*time.Second)

	config := amqpconsumer.DefaultConfig()
	config.QDurable = true

	sched := celery.NewScheduler(amqpconsumer.NewAMQPSubscriber(queueName, &config, retry))

	// backend := NewAMQPBackend()
	worker := celery.NewWorker(100, sched, noopBackend{}, sched)

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

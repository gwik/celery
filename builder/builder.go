package builder

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gwik/celery"
	"github.com/gwik/celery/amqpconsumer"
	"github.com/gwik/celery/amqputil"

	_ "github.com/gwik/celery/jsonmessage"
)

func Serve(queue string, declare func(worker *celery.Worker)) {
	conf := celery.ConfigFromEnv()

	if os.Getenv("LOG_OFF") != "" {
		log.SetOutput(ioutil.Discard)
	}

	// go func() {
	// 	log.Println(http.ListenAndServe("localhost:6060", nil))
	// }()

	retry := amqputil.NewRetry(conf.BrokerURL, nil, 2*time.Second)
	sched := celery.NewScheduler(amqpconsumer.NewAMQPSubscriber(queue, nil, retry))

	// backend := NewAMQPBackend()
	worker := celery.NewWorker(conf.CelerydConcurrency, sched, celery.NoOpBackend{}, sched)

	quit := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)

	go func() {
		s := <-sigs
		log.Printf("Signal %v received. Closing...", s)
		retry.Close()
		// TODO wait for worker to complete running tasks.
		close(quit)
	}()

	declare(worker)
	worker.Start()

	<-quit

}

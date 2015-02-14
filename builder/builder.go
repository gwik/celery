package builder

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/gwik/celery"
	// "github.com/gwik/celery/amqpbackend"
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
	// backend := amqpbackend.NewAMQPBackend(retry)
	backend := &celery.DiscardBackend{}

	worker := celery.NewWorker(conf.CelerydConcurrency, sched, backend, sched)

	quit := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, os.Kill)

	go func() {
		s := <-sigs
		log.Printf("Signal %v received. Closing...", s)
		go func() {
			<-time.After(5 * time.Minute)
			os.Exit(1)
		}()
		go func() {
			<-sigs
			os.Exit(1)
		}()
		worker.Close()
		worker.Wait()
		retry.Close()
		log.Println("Closed.")
		close(quit)
	}()

	declare(worker)
	worker.Start()

	<-quit

}

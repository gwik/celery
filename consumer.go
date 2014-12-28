/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"log"
	"os"

	_ "github.com/gwik/gocelery/message/json"
	"github.com/gwik/gocelery/types"

	"code.google.com/p/go.net/context"
	"github.com/streadway/amqp"
)

var connection *amqp.Connection

func init() {
	var err error
	connection, err = amqp.Dial(os.Getenv("AMQP_URL"))
	if err != nil {
		panic(err)
	}
}

func two(ctx context.Context, msg *types.Message) types.Result {
	log.Printf("two: %v", msg)
	return struct{}{}
}

func add(ctx context.Context, msg *types.Message) types.Result {
	log.Printf("add: %v", msg)
	return struct{}{}
}

func Consume(queueName string) error {
	ch, err := connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when usused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	worker := NewWorker(10)
	worker.Register("tasks.add", add)
	worker.Register("tasks.two", two)
	worker.Start()

	scheduler := NewScheduler()

	go func() {
		scheduler.Start()
		in := scheduler.Consume()
		for msg := range in {
			reply := make(chan types.Result, 1)
			err := worker.Dispatch(msg, reply)
			if err != nil {
				log.Printf("Dispatch error: %v", err)
				continue
			}

			go func(reply <-chan types.Result) {
				<-reply
				// XXX: Ack
			}(reply)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	for d := range msgs {
		log.Printf("%s", d.Body)
		msg, err := types.DecodeMessage(d.ContentType, d.Body)
		if err != nil {
			log.Println(err)
			continue
		}
		scheduler.Add(msg)
		d.Ack(false)
	}

	return nil
}

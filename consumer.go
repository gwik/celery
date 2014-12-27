/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"log"
	"os"

	"github.com/gwik/celery/types"

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

func two(ctx context.Context, msg types.Message) types.Result {
	log.Printf("two: %v", msg)
	return struct{}{}
}

func add(ctx context.Context, msg types.Message) types.Result {
	log.Printf("add: %v", msg)
	return struct{}{}
}

// func process(d amqp.Delivery) {
// 	log.Printf("Content Type: %v", d.ContentType)
// 	log.Printf("Routing Key: %v", d.RoutingKey)
// 	log.Printf("Headers: %v", d.Headers)
// 	log.Printf("Body: %s", d.Body)

// 	msg, err := types.DecodeJSONMessage(d.Body)
// 	if err != nil {
// 		panic(err)
// 	}

// 	/*
// 		Task() string
// 		ID() string
// 		Args() []interface{}
// 		KwArgs() map[string]interface{}
// 		Retries() int
// 		ETA() time.Time
// 		Expires() time.Time
// 	*/

// 	log.Printf("Message Task: %v", msg.Task())
// 	log.Printf("Message ID: %v", msg.ID())
// 	log.Printf("Message Args: %v", msg.Args())
// 	log.Printf("Message KwArgs: %v", msg.KwArgs())
// 	log.Printf("Message Retries: %v", msg.Retries())
// 	log.Printf("Message ETA: %v", msg.ETA())
// 	log.Printf("Message Expires: %v", msg.Expires())

// 	var args [2]float64
// 	for i, v := range msg.Args() {
// 		args[i] = v.(float64)
// 	}

// 	if len(args) == 2 {
// 		log.Printf("Add: %v => %0.2f", args, add(args[0], args[1]))
// 	} else {
// 		panic("invalid arguments")
// 	}
// }

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

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			if d.ContentType != "application/json" {
				d.Ack(false)
			}
			log.Printf("%s", d.Body)
			msg, err := types.DecodeJSONMessage(d.Body)
			if err != nil {
				log.Println(err)
				continue
			}
			reply := make(chan types.Result, 1)
			err = worker.Dispatch(msg, reply)
			if err != nil {
				log.Printf("Dispatch error: %v", err)
				continue
			}
			go func(d amqp.Delivery, reply <-chan types.Result) {
				<-reply
				d.Ack(false)
			}(d, reply)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

	return nil
}

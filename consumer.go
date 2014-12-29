/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package gocelery

import (
	"log"
	"os"

	_ "github.com/gwik/gocelery/message/json"
	amqptransport "github.com/gwik/gocelery/transport/amqp"
	"github.com/gwik/gocelery/types"

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

func two(task *types.Task) interface{} {
	log.Printf("two: %v", task.Msg.ID)
	return struct{}{}
}

func add(task *types.Task) interface{} {
	log.Printf("add: %v", task.Msg.ID)
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

	results := make(chan *types.Result)
	worker := NewWorker(10, results)
	worker.Register("tasks.add", add)
	worker.Register("tasks.two", two)
	worker.Start()

	scheduler := NewScheduler()

	go func() {
		scheduler.Start()
		for task := range scheduler.Consume() {
			err := worker.Dispatch(task)
			if err != nil {
				log.Printf("Dispatch error: %v", err)
				continue
			}
		}
	}()

	go func() {
		for r := range results {
			d := amqptransport.DeliveryFromContext(r.Task.Ctx)
			d.Ack(false)
			log.Printf("Acked: %v", r.Task.Msg.ID)
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
		task := types.NewTask(msg)
		task.Ctx = amqptransport.NewContext(task.Ctx, d)
		scheduler.Add(task)
	}

	forever := make(chan struct{})
	<-forever

	return nil
}

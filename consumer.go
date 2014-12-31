/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package gocelery

import (
	"log"
	"os"
	"time"

	_ "github.com/gwik/gocelery/message/json"
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

func two(task types.Task) interface{} {
	<-time.After(time.Second * 10)
	log.Printf("two: %v", task.Msg().ID)
	return struct{}{}
}

func add(task types.Task) interface{} {
	<-time.After(time.Millisecond * 200)
	log.Printf("add: %v", task.Msg().ID)
	return struct{}{}
}

type amqpTask struct {
	msg *types.Message
	d   amqp.Delivery
}

func (t *amqpTask) Msg() *types.Message {
	return t.msg
}

func (t *amqpTask) Ack() {
	t.d.Ack(false)
}

type amqpConsumer struct {
	q    string
	out  chan types.Task
	quit chan struct{}
}

func AMQPSubscriber(queueName string) types.Subscriber {
	c := &amqpConsumer{
		q:    queueName,
		out:  make(chan types.Task),
		quit: make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *amqpConsumer) Subscribe() <-chan types.Task {
	return c.out
}

func (c *amqpConsumer) Close() error {
	close(c.quit)
	return nil
}

func (c *amqpConsumer) loop() {
	ch, err := connection.Channel()
	if err != nil {
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		c.q,   // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
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

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")

	var task types.Task
	var out chan types.Task
	in := msgs

	for {
		select {
		case d := <-in:
			log.Printf("%s", d.Body)
			msg, err := types.DecodeMessage(d.ContentType, d.Body)
			if err != nil {
				log.Println(err)
			}
			task = &amqpTask{msg, d}
			out = c.out
			in = nil
		case out <- task:
			out = nil
			in = msgs
		case <-c.quit:
			close(c.out)
		}
	}

}

func Consume(queueName string) error {

	in := Schedule(AMQPSubscriber("celery"))
	worker := NewWorker(10, in)

	worker.Register("tasks.add", add)
	worker.Register("tasks.two", two)
	worker.Start()

	forever := make(chan struct{})
	<-forever

	return nil
}

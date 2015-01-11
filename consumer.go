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
	"golang.org/x/net/context"
)

func two(task types.Task) interface{} {
	<-time.After(time.Second * 10)
	log.Printf("two: %v", task.Msg().ID)
	return nil
}

func add(task types.Task) interface{} {
	log.Printf("add: %v", task.Msg().ID)
	args := task.Msg().Args
	return args[0].(float64) + args[1].(float64)
}

type amqpTask struct {
	msg *types.Message
	ch  *amqp.Channel
	tag uint64 // delivery tag
}

func (t *amqpTask) Msg() *types.Message {
	return t.msg
}

func (t *amqpTask) Ack() error {
	return t.ch.Ack(t.tag, false)
}

func (t *amqpTask) Reject(requeue bool) error {
	return t.ch.Reject(t.tag, requeue)
}

type amqpConsumer struct {
	q    string
	out  chan types.TaskContext
	quit chan struct{}
}

type quitContext chan struct{}

func (qc quitContext) Done() <-chan struct{} {
	return qc
}

func (qc quitContext) Deadline() (deadline time.Time, ok bool) {
	return
}

func (qc quitContext) Value(key interface{}) interface{} {
	return nil
}

func (qc quitContext) Err() error {
	select {
	case <-qc:
		return context.Canceled
	default:
		return nil
	}
}

func AMQPSubscriber(queueName string) *amqpConsumer {
	c := &amqpConsumer{
		q:    queueName,
		out:  make(chan types.TaskContext),
		quit: make(chan struct{}),
	}
	go c.loop()
	return c
}

func (c *amqpConsumer) Subscribe() <-chan types.TaskContext {
	return c.out
}

func (c *amqpConsumer) Close() error {
	close(c.quit)
	return nil
}

func (c *amqpConsumer) connect() (*amqp.Channel, <-chan amqp.Delivery) {
	url := os.Getenv("AMQP_URL")
	for {
		connection, err := amqp.Dial(url)
		if err != nil {
			log.Printf("could not connect to %s: %v, will retry...", url, err)
			<-time.After(time.Second * 2)
			continue
		}

		ch, err := connection.Channel()
		if err != nil {
			if err == amqp.ErrClosed {
				continue
			}
			log.Println("unexpected AMQP Channel error: %v", err)
			panic(err)
		}

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

		return ch, msgs
	}
}

func (c *amqpConsumer) loop() {

	var task types.TaskContext
	var out chan types.TaskContext
	ch, msgs := c.connect()
	in := msgs

	for {
		select {
		case d, ok := <-in:
			if !ok {
				close(c.quit)
				return
			}
			log.Printf("%s %s", d.Body, d.ReplyTo)
			msg, err := types.DecodeMessage(d.ContentType, d.Body)
			if err != nil {
				log.Println(err)
				d.Reject(true)
				continue
			}
			ctx := types.ContextFromMessage(quitContext(c.quit), msg)
			task = types.TaskContext{T: &amqpTask{msg, ch, d.DeliveryTag}, C: ctx}
			out = c.out
			in = nil
		case out <- task:
			out = nil
			in = msgs
		case <-c.quit:
			return
		}
	}

}

type noopBackend struct{}

func (noopBackend) Publish(types.Task, *types.ResultMeta) {}

func Consume(queueName string) error {

	in := Schedule(AMQPSubscriber("celery"))

	// backend := NewAMQPBackend()
	worker := NewWorker(10, in, noopBackend{})

	worker.Register("tasks.add", add)
	worker.Register("tasks.two", two)
	worker.Start()

	forever := make(chan struct{})
	<-forever

	return nil
}

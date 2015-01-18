/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package celery

import (
	"log"
	"os"
	"time"

	_ "github.com/gwik/celery/message/json"
	"github.com/gwik/celery/types"
	"github.com/gwik/celery/util/amqputil"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

type amqpTask struct {
	msg *types.Message
	ch  *amqp.Channel
	tag uint64 // delivery tag
}

func (t *amqpTask) Msg() types.Message {
	return *t.msg
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

func (c *amqpConsumer) declare(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare(
		c.q,   // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		return nil, err
	}

	return msgs, nil
}

func (c *amqpConsumer) loop() {

	var ch *amqp.Channel
	var tc types.TaskContext
	var out chan types.TaskContext
	var in, msgs <-chan amqp.Delivery

	retrier := amqputil.NewAMQPRetry(os.Getenv("AMQP_URL"), nil, time.Second*5)
	chch := retrier.Channel()

	ctx, abort := context.WithCancel(context.Background())

	for {
		select {
		case ch = <-chch:
			var err error
			msgs, err = c.declare(ch)
			if err != nil {
				if err != amqp.ErrClosed {
					panic(err)
				}
				chch = retrier.Channel()
				continue
			}
			chch = nil
			in = msgs
		case d, ok := <-in:
			if !ok {
				chch = retrier.Channel()
				in = nil
				abort()
				continue
			}
			log.Printf("%s %s", d.Body, d.ReplyTo)
			msg, err := types.DecodeMessage(d.ContentType, d.Body)
			if err != nil {
				log.Println(err)
				d.Reject(true)
				continue
			}
			mctx := types.ContextFromMessage(ctx, msg)
			tc = types.TaskContext{T: &amqpTask{&msg, ch, d.DeliveryTag}, C: mctx}
			out = c.out
			in = nil
		case out <- tc:
			out = nil
			in = msgs
		case <-c.quit:
			abort()
			close(out)
			return
		}
	}

}

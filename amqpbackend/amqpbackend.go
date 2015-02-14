/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package amqpbackend

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/gwik/celery"
	"github.com/gwik/celery/amqputil"
	"github.com/streadway/amqp"
)

type result struct {
	t celery.Task
	r *celery.ResultMeta
}

type amqpBackend struct {
	results chan *result
	quit    chan struct{}
	retry   *amqputil.Retry
	encoder func(v interface{}) ([]byte, error) // FIXME: use marshal interface
}

func NewAMQPBackend(retry *amqputil.Retry) *amqpBackend {
	b := &amqpBackend{
		results: make(chan *result, 100),
		quit:    make(chan struct{}),
		encoder: json.Marshal,
		retry:   retry,
	}

	go b.loop()
	return b
}

// routingKey returns a routing key from a task id tid
func routingKey(tid string) string {
	return strings.Replace(tid, "-", "", -1)
}

func (b *amqpBackend) declare(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		"celeryresults", // name
		"direct",        // type
		true,            // durable
		false,           // autoDelete
		false,           // internal
		false,           // noWait
		nil,             // args
	)
}

func (b *amqpBackend) declareResultQueue(name string, ch *amqp.Channel) error {
	if _, err := ch.QueueDeclare(name, true, true, false, false, amqp.Table{"x-expires": int32(86400000)}); err != nil {
		return err
	}

	if err := ch.QueueBind(name, name, "celeryresults", false, nil); err != nil {
		return err
	}

	return nil
}

func (b *amqpBackend) loop() {

	var ch *amqp.Channel
	var in chan *result
	var ok bool
	chch := b.retry.Channel()

	for {
		select {
		case ch, ok = <-chch: // wait for an AMQP channel
			if !ok {
				log.Println("Terminated amqp consumer.")
				return
			}
			err := b.declare(ch)
			if err != nil {
				if err != amqp.ErrClosed {
					panic(err)
				}
				chch = b.retry.Channel()
				continue
			}
			chch = nil
			in = b.results
		case rm := <-in:
			key := routingKey(rm.t.Msg().ID)
			log.Printf("publishing result %s %v", key, rm.r.Result)
			body, err := b.encoder(rm.r)
			if err != nil {
				log.Printf("Failed to encoding result: %v", err)
				break
			}
			if err := b.declareResultQueue(key, ch); err != nil {
				log.Printf("Failed to declare result queue %s: %v", key, err)
				chch = b.retry.Channel()
				in = nil
				break
			}
			err = ch.Publish(
				"celeryresults",
				key,
				true,  // mandatory
				false, // immediate
				amqp.Publishing{
					CorrelationId: rm.t.Msg().ID,
					DeliveryMode:  amqp.Transient,
					Timestamp:     time.Now(),
					ContentType:   "application/json",
					Body:          body,
				},
			)
			if err != nil {
				log.Printf("Failed to publish result: %v", err)
				chch = b.retry.Channel()
				in = nil
			}
		case <-b.quit:
			return
		}
	}
}

func (b *amqpBackend) Publish(t celery.Task, r *celery.ResultMeta) {
	b.results <- &result{t, r}
}

func (b *amqpBackend) Close() {
	close(b.quit)
}

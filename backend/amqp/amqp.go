/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package amqp

import (
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/gwik/celery/types"
	"github.com/streadway/amqp"
)

const (
	resultExchange     = "celeryresults"
	resultExchangeType = "direct"
)

type result struct {
	t types.Task
	r *types.ResultMeta
}

type amqpBackend struct {
	results chan *result
	quit    chan struct{}
	encoder func(v interface{}) ([]byte, error)
}

func NewAMQPBackend() types.Backend {
	b := &amqpBackend{
		results: make(chan *result, 100),
		quit:    make(chan struct{}),
		encoder: json.Marshal,
	}

	go b.loop()
	return b
}

// routingKey returns a routing key from a task id tid
func routingKey(tid string) string {
	return strings.Replace(tid, "-", "", -1)
}

func (b *amqpBackend) loop() {

	connection, _ := amqp.Dial(os.Getenv("AMQP_URL"))

	ch, err := connection.Channel()
	if err != nil {
		log.Printf("could not connect to AMQP result backend: %v", err)
		return
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		resultExchange,     // name
		resultExchangeType, // type
		true,               // durable
		false,              // autoDelete
		false,              // internal
		false,              // noWait
		nil,                // args
	)

	if err != nil {
		log.Printf("Result AMQP exchange declaration failed: %v", err)
		return
	}

	returns := make(chan amqp.Return)
	ch.NotifyReturn(returns)

	log.Println("AMQP backend ready.")

	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				log.Println("returned closed.")
				returns = nil
			} else {
				if ret.ReplyCode == 312 {
					log.Printf("retry send result %v", ret.CorrelationId)
					// <-time.After(time.Millisecond * 100)
					err = ch.Publish(
						resultExchange,
						ret.RoutingKey,
						true,  // mandatory
						false, // immediate
						amqp.Publishing{
							CorrelationId: ret.CorrelationId,
							DeliveryMode:  amqp.Transient,
							Timestamp:     ret.Timestamp,
							ContentType:   ret.ContentType,
							Body:          ret.Body,
						},
					)
				} else {
					log.Printf("returned: %v", ret)
				}
			}
		case rm := <-b.results:
			// XXX: make it async
			key := routingKey(rm.t.Msg().ID)
			log.Printf("publishing result %s %v", key, rm.r.Result)
			body, err := b.encoder(rm.r)
			if err != nil {
				log.Printf("Failed to encoding result: %v", err)
				break
			}
			if err != nil {
				log.Printf("Failed to declare result queue %s: %v", key, err)
				break
			}
			err = ch.Publish(
				resultExchange,
				key,
				returns != nil, // mandatory
				false,          // immediate
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
				break
			}
		case <-b.quit:
			return
		}
	}
}

func (b *amqpBackend) Publish(t types.Task, r *types.ResultMeta) {
	b.results <- &result{t, r}
}

func (b *amqpBackend) Close() {
	close(b.quit)
}

/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package amqputil

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

// AMQPRetry retry connecting on failures.
type AMQPRetry struct {
	url     string
	config  *amqp.Config
	delay   time.Duration
	getters chan chan *amqp.Channel
}

func NewAMQPRetry(url string, config *amqp.Config, delay time.Duration) *AMQPRetry {
	ar := &AMQPRetry{
		url:     url,
		config:  config,
		delay:   delay,
		getters: make(chan chan *amqp.Channel),
	}

	ar.loop()

	return ar
}

func (ar *AMQPRetry) connect() *amqp.Connection {
	for {
		var conn *amqp.Connection
		var err error

		if ar.config == nil {
			conn, err = amqp.Dial(ar.url)
		} else {
			conn, err = amqp.DialConfig(ar.url, *ar.config)
		}

		if err != nil {
			if err == amqp.ErrClosed {
				log.Printf("could not connect to %s: %v", ar.url, err)
				<-time.After(ar.delay)
				continue
			}
			panic(err)
		}

		return conn
	}
}

func (ar *AMQPRetry) loop() {
	conn := ar.connect()
	for getter := range ar.getters {

		for {
			ch, err := conn.Channel()
			if err == nil {
				getter <- ch
				close(getter)
				break
			}
			if err == amqp.ErrClosed {
				conn = ar.connect()
				continue
			}
			panic(err)
		}
	}
}

func (ar *AMQPRetry) Channel() <-chan *amqp.Channel {
	ch := make(chan *amqp.Channel, 1)
	go func() {
		ar.getters <- ch
	}()

	return ch
}

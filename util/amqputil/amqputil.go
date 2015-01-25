/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package amqputil

import (
	"log"
	"net"
	"time"

	"github.com/streadway/amqp"
)

// AMQPRetry retry connecting on failures.
type AMQPRetry struct {
	url     string
	config  *amqp.Config
	delay   time.Duration
	getters chan chan *amqp.Channel
	err     error
}

func NewAMQPRetry(url string, config *amqp.Config, delay time.Duration) *AMQPRetry {
	ar := &AMQPRetry{
		url:     url,
		config:  config,
		delay:   delay,
		getters: make(chan chan *amqp.Channel, 1024),
	}

	go ar.loop()

	return ar
}

func (ar *AMQPRetry) connect() (*amqp.Connection, error) {
	for {
		var conn *amqp.Connection
		var err error
		log.Printf("connecting to %s", ar.url)

		if ar.config == nil {
			conn, err = amqp.Dial(ar.url)
		} else {
			conn, err = amqp.DialConfig(ar.url, *ar.config)
		}

		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				log.Printf("could not connect to %s will retry after %v: %v", ar.url, ar.delay, err)
				<-time.After(ar.delay)
				continue
			}
			return nil, err
		}

		return conn, nil
	}
}

func (ar *AMQPRetry) Err() error {
	return ar.err
}

func (ar *AMQPRetry) Close() error {
	close(ar.getters)
	return ar.err
}

func (ar *AMQPRetry) loop() {

	for {
		conn, err := ar.connect()
		if err != nil {
			log.Printf("amqp connection error, will terminate: %v", err)
			ar.err = err
			for {
				getter, ok := <-ar.getters
				if !ok {
					return
				}
				close(getter)
			}
			return
		}

		for {
			getter, ok := <-ar.getters
			if !ok {
				return
			}
			ch, err := conn.Channel()
			if err == nil {
				getter <- ch
				close(getter)
				break
			}
		}
	}

}

func (ar *AMQPRetry) Channel() <-chan *amqp.Channel {
	// getter is buffered to avoid blocking
	// if reveiver is not listening it won't leak
	// and it won't wait forever, preveting others
	// from getting their channel.
	getter := make(chan *amqp.Channel, 1)
	ar.getters <- getter
	return getter
}

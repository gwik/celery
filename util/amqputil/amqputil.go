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

// Retry connects to amqp and retry on temporary network failures.
type Retry struct {
	url     string
	config  *amqp.Config
	delay   time.Duration
	getters chan chan *amqp.Channel
	err     error
}

// NewRetry builds a new retry.
func NewRetry(url string, config *amqp.Config, delay time.Duration) *Retry {
	ar := &Retry{
		url:     url,
		config:  config,
		delay:   delay,
		getters: make(chan chan *amqp.Channel, 1024),
	}

	go ar.loop()

	return ar
}

func (ar *Retry) connect() (*amqp.Connection, error) {
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

// Err return the non-temporary failure.
func (ar *Retry) Err() error {
	return ar.err
}

// Close closes the AMQP connections and stops the retry.
func (ar *Retry) Close() error {
	close(ar.getters)
	return ar.err
}

func (ar *Retry) loop() {

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
				conn.Close()
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

// Channel returns a chan of AMQP Channels. When the AMQP connection is
// ready it will be sent an AMQP Channel and will be closed.
// if the chan is closed before sending a channel it means an error
// occured and the receiver must not call the method again.
func (ar *Retry) Channel() <-chan *amqp.Channel {
	// getter is buffered to avoid blocking
	// if reveiver is not listening it won't leak
	// and it won't wait forever, preveting others
	// from getting their channel.
	getter := make(chan *amqp.Channel, 1)
	ar.getters <- getter
	return getter
}

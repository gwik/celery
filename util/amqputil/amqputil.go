/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package amqputil

import (
	"errors"
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
	closing chan chan error
	getters chan chan *amqp.Channel
}

// NewRetry builds a new retry.
func NewRetry(url string, config *amqp.Config, delay time.Duration) *Retry {
	ar := &Retry{
		url:     url,
		config:  config,
		delay:   delay,
		closing: make(chan chan error),
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

		done := make(chan struct{}, 1)
		go func() {
			if ar.config == nil {
				conn, err = amqp.Dial(ar.url)
			} else {
				conn, err = amqp.DialConfig(ar.url, *ar.config)
			}
			done <- struct{}{}
		}()

		select {
		case errC := <-ar.closing:
			errC <- nil
			go func() {
				<-done
				if err != nil {
					conn.Close()
				}
			}()
			return nil, errors.New("closed while connecting.")
		case <-done:
		}

		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				log.Printf("could not connect to %s will retry after %v: %v\n", ar.url, ar.delay, err)
				select {
				case errC := <-ar.closing:
					errC <- err
					return nil, err
				case <-time.After(ar.delay):
					continue
				}
			}
			return nil, err
		}

		return conn, nil
	}
}

// Close closes the AMQP connections and stops the retry.
func (ar *Retry) Close() error {
	errC := make(chan error)
	ar.closing <- errC
	return <-errC
}

func (ar *Retry) loop() {

	for {
		conn, err := ar.connect()
		if err != nil {
			log.Printf("amqp connection error, will terminate: %v", err)
			for {
				select {
				case errC := <-ar.closing:
					errC <- err
					return
				case getter := <-ar.getters:
					close(getter)
				}
			}
		}

		var out chan *amqp.Channel
		var ach *amqp.Channel
		more := true

		for more {
			select {
			case errC := <-ar.closing:
				errC <- conn.Close()
				return
			case out <- ach:
				close(out)
				out = nil
				ach = nil
			case getter := <-ar.getters:
				ch, err := conn.Channel()
				if err == nil {
					ach = ch
					out = getter
					break
				}
				more = false
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

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
	url      string
	config   *amqp.Config
	delay    time.Duration
	closing  chan chan error
	requests chan chan<- *amqp.Channel
	err      error
}

// NewRetry builds a new retry.
func NewRetry(url string, config *amqp.Config, delay time.Duration) *Retry {
	ar := &Retry{
		url:      url,
		config:   config,
		delay:    delay,
		closing:  make(chan chan error),
		requests: make(chan chan<- *amqp.Channel, 1024),
	}

	go ar.loop()

	return ar
}

func (ar *Retry) connect() <-chan *amqp.Connection {

	ch := make(chan *amqp.Connection)

	go func() {
		defer close(ch)

		var conn *amqp.Connection
		var retry <-chan time.Time

		for {
			log.Printf("connecting to %s", ar.url)

			if ar.config == nil {
				conn, ar.err = amqp.Dial(ar.url)
			} else {
				conn, ar.err = amqp.DialConfig(ar.url, *ar.config)
			}

			if ar.err != nil {
				if _, ok := ar.err.(net.Error); ok {
					log.Printf("could not connect to %s will retry after %v: %v\n", ar.url, ar.delay, ar.err)
					retry = time.After(ar.delay)
				} else {
					return
				}
			}

			select {
			case <-retry:
				retry = nil
				continue
			case errC := <-ar.closing:
				if ar.err == nil {
					errC <- conn.Close()
				} else {
					errC <- ar.err
				}
			case ch <- conn:
			}
			return
		}
	}()

	return ch
}

// Close closes the AMQP connections and stops the retry.
func (ar *Retry) Close() error {
	errC := make(chan error)
	ar.closing <- errC
	return <-errC
}

func (ar *Retry) terminate() {
	for {
		select {
		case req := <-ar.requests:
			close(req)
		case errC := <-ar.closing:
			errC <- ar.err
			return
		}
	}
}

func (ar *Retry) loop() {

	for {
		var out chan<- *amqp.Channel
		var in chan chan<- *amqp.Channel
		var ach *amqp.Channel
		var conn *amqp.Connection
		var ok bool

		connC := ar.connect()

		for {
			select {
			case conn, ok = <-connC:
				if !ok {
					ar.terminate()
					return
				}
				log.Printf("AMQP connection ready.")
				in = ar.requests
			case errC, ok := <-ar.closing:
				if ok {
					errC <- conn.Close()
				}
				return
			case out <- ach:
				close(out)
				out = nil
				ach = nil
				in = ar.requests
			case c, ok := <-in:
				if !ok {
					return
				}
				in = nil
				ch, err := conn.Channel()
				if err == nil {
					ach = ch
					out = c
					break
				}
				// re-queue
				ar.enqueue(c)
				connC = ar.connect()
			}
		}
	}
}

func (ar *Retry) enqueue(c chan<- *amqp.Channel) {
	select {
	case ar.requests <- c:
	default:
		go func() { ar.requests <- c }()
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
	c := make(chan *amqp.Channel, 1)
	ar.enqueue(c)

	return c
}

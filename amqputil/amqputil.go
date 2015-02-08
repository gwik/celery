/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

/*

Package amqputil provides utilities to work with http://github.com/streadway/amqp/ package.

*/
package amqputil

import (
	"log"
	"net"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// Retry connects to AMQP and retry on network failures.
type Retry struct {
	url     string
	config  *amqp.Config
	delay   time.Duration
	closing chan chan error
	err     error

	mu       sync.RWMutex
	requests chan chan<- *amqp.Channel
	stopped  bool
}

// NewRetry builds a new retry.
func NewRetry(url string, config *amqp.Config, delay time.Duration) *Retry {
	ar := &Retry{
		url:      url,
		config:   config,
		delay:    delay,
		closing:  make(chan chan error),
		mu:       sync.RWMutex{},
		stopped:  false,
		requests: make(chan chan<- *amqp.Channel, 1024),
	}

	go ar.loop()

	return ar
}

// Close closes the AMQP connections and stops the retry.
func (ar *Retry) Close() error {
	errC := make(chan error)
	ar.closing <- errC
	return <-errC
}

func (ar *Retry) loop() {

	defer ar.terminate()

	for {
		var out chan<- *amqp.Channel
		var ach *amqp.Channel
		var conn *amqp.Connection

		for {

			for conn == nil { // connection retry loop

				log.Printf("connecting to %s", ar.url)

				if ar.config == nil {
					conn, ar.err = amqp.Dial(ar.url)
				} else {
					conn, ar.err = amqp.DialConfig(ar.url, *ar.config)
				}
				if ar.err != nil {
					if _, ok := ar.err.(net.Error); ok {
						log.Printf("could not connect to %s will retry after %v: %v", ar.url, ar.delay, ar.err)
						select {
						case <-time.After(ar.delay):
							continue
						case errC := <-ar.closing:
							close(ar.closing)
							if conn != nil {
								errC <- conn.Close()
							} else {
								errC <- ar.err
							}
							return
						}
					} else {
						log.Printf("AMQP error: %v", ar.err)
						return
					}
				}
			}

			log.Printf("Connected to AMQP at %s", ar.url)
			select {
			case errC := <-ar.closing:
				close(ar.closing)
				errC <- conn.Close()
				return
			case out <- ach:
				close(out)
				out = nil
				ach = nil
			case c := <-ar.requests:
				ch, err := conn.Channel()
				if err == nil {
					ach = ch
					out = c
					break
				}
				// re-queue
				ar.enqueue(c)
				conn = nil
			}
		}
	}
}

func (ar *Retry) terminate() {
	// close is protected by a Write lock to prevent a data race.
	// The write lock ensure that write (close) happens before read (send in enqueue).
	ar.mu.Lock()
	ar.stopped = true
	close(ar.requests)
	ar.mu.Unlock()

	for req := range ar.requests {
		close(req)
	}
	errC, ok := <-ar.closing
	if ok {
		errC <- ar.err
		close(ar.closing)
	}
}

func (ar *Retry) enqueue(c chan<- *amqp.Channel) {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	if ar.stopped {
		close(c)
		return
	}

	ar.requests <- c
}

// Channel returns a chan of AMQP Channels. When the AMQP connection is
// ready it will be sent an AMQP Channel and will be closed.
// if the chan is closed before sending a channel it means an error
// occured and the receiver must not call the method again.
func (ar *Retry) Channel() <-chan *amqp.Channel {
	// c is buffered to avoid blocking.
	// If reveiver is not listening it won't leak
	// and it won't wait forever, preveting other callers
	// from getting their channel.
	c := make(chan *amqp.Channel, 1)
	ar.enqueue(c)

	return c
}

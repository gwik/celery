/*
Copyright (c) 2014-2015 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package amqpconsumer

import (
	"log"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"

	"github.com/gwik/celery/types"
	"github.com/gwik/celery/util/amqputil"
)

type amqpTask struct {
	msg *types.Message
	ch  *amqp.Channel
	tag uint64 // delivery tag
}

type Config struct {
	// queue
	QDurable    bool
	QAutoDelete bool
	QExclusive  bool
	QNoWait     bool
	QArgs       amqp.Table // queue extra arguments

	// consumer
	Consumer   string // consumer name
	CAutoACK   bool
	CExclusive bool
	CNoLocal   bool
	CNoWait    bool
	CArgs      amqp.Table // consumer extra arguments
}

var defaultConfig = &Config{
	QDurable:    false,
	QAutoDelete: false,
	QExclusive:  false,
	QNoWait:     false,
	QArgs:       nil,

	Consumer:   "",
	CAutoACK:   false,
	CExclusive: false,
	CNoLocal:   false,
	CNoWait:    false,
	CArgs:      nil,
}

// DefaultConfig returns a config with the following defaults:
//
// QDurable:    false,
// QAutoDelete: false,
// QExclusive:  false,
// QNoWait:     false,
// QArgs:      nil,
//
// Consumer:  "",
// CAutoACK:   false,
// CExclusive: false,
// CNoLocal:   false,
// CNoWait:    false,
// CArgs:     nil,
func DefaultConfig() Config {
	return *defaultConfig
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
	q      string
	config *Config
	retry  *amqputil.Retry
	out    chan types.TaskContext
	quit   chan struct{}
}

var _ types.Subscriber = (*amqpConsumer)(nil)

// NewAMQPSubscriber Create a new AMQP Subscriber. config can be nil, in
// that case it will be set with DefaultConfig.
func NewAMQPSubscriber(queue string, config *Config, retry *amqputil.Retry) *amqpConsumer {
	if config == nil {
		dcfg := DefaultConfig()
		config = &dcfg
	}
	c := &amqpConsumer{
		q:      queue,
		config: config,
		retry:  retry,
		out:    make(chan types.TaskContext),
		quit:   make(chan struct{}),
	}
	go c.loop()
	return c
}

// Subscribe implements the Subscriber interface.
func (c *amqpConsumer) Subscribe() <-chan types.TaskContext {
	return c.out
}

// Close implements the Subscriber interface.
func (c *amqpConsumer) Close() error {
	close(c.quit)
	return nil
}

func (c *amqpConsumer) declare(ch *amqp.Channel) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare(
		c.q,                  // name
		c.config.QDurable,    // durable
		c.config.QAutoDelete, // delete when usused
		c.config.QExclusive,  // exclusive
		c.config.QNoWait,     // no-wait
		c.config.QArgs,       // arguments
	)
	if err != nil {
		return nil, err
	}

	msgs, err := ch.Consume(
		q.Name,              // queue
		c.config.Consumer,   // consumer
		c.config.CAutoACK,   // auto-ack
		c.config.CExclusive, // exclusive
		c.config.CNoLocal,   // no-local
		c.config.CNoWait,    // no-wait
		c.config.CArgs,      // args
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
	var ok bool

	chch := c.retry.Channel()

	ctx, abort := context.WithCancel(context.Background())

	defer func() {
		if ch != nil {
			ch.Close()
		}
	}()
	defer close(c.out)

	for {
		select {
		case ch, ok = <-chch: // wait for an AMQP channel
			if !ok {
				return
			}
			var err error
			msgs, err = c.declare(ch)
			if err != nil {
				if err != amqp.ErrClosed {
					panic(err)
				}
				chch = c.retry.Channel()
				continue
			}
			chch = nil
			in = msgs
		case d, ok := <-in: // wait for AMQP deliveries
			if !ok {
				chch = c.retry.Channel()
				in = nil
				abort()
				continue
			}
			// log.Printf("%s %s", d.Body, d.ReplyTo)
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
		case out <- tc: // send new task downstream.
			out = nil
			in = msgs
		case <-c.quit: // quit
			abort()
			return
		}
	}

}

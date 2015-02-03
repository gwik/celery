/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

/*
types package provides common types for celery.
*/
package types

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
)

// Message v1 as described at http://celery.readthedocs.org/en/latest/internals/protocol.html
type Message struct {
	Task    string
	ID      string
	Args    []interface{}
	KwArgs  map[string]interface{}
	Retries int
	ETA     time.Time
	Expires time.Time

	// TODO: extensions

}

// DecoderFunc is a func that parses bytes and return a Message
type DecoderFunc func([]byte) (Message, error)

var messageDecoderRegister map[string]DecoderFunc

// RegisterMessageDecoder registers a DecoderFunc function for a given content type.
func RegisterMessageDecoder(contentType string, decoder DecoderFunc) {
	messageDecoderRegister[contentType] = decoder
}

// DecodeMessage decodes a message using registered decoders.
func DecodeMessage(contentType string, p []byte) (Message, error) {
	dec, exists := messageDecoderRegister[contentType]
	if !exists {
		return Message{}, fmt.Errorf("No decoder registered for %s", contentType)
	}
	return dec(p)
}

// ContextFromMessage prepares a context from a parent context and a message.
func ContextFromMessage(parent context.Context, msg Message) context.Context {
	if !msg.Expires.IsZero() {
		ctx, _ := context.WithDeadline(parent, msg.Expires)
		return ctx
	}
	return parent
}

func init() {
	messageDecoderRegister = make(map[string]DecoderFunc)
}

/*

Scheme	MIME Type
json	application/json
yaml	application/x-yaml
pickle	application/x-python-serialize
msgpack	application/x-msgpack

*/

/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
)

// Message as describe at http://celery.readthedocs.org/en/latest/internals/protocol.html
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

type DecoderFunc func([]byte) (*Message, error)

var messageDecoderRegister map[string]DecoderFunc

func RegisterMessageDecoder(contentType string, decoder DecoderFunc) {
	messageDecoderRegister[contentType] = decoder
}

func DecodeMessage(contentType string, p []byte) (*Message, error) {
	dec, exists := messageDecoderRegister[contentType]
	if !exists {
		return nil, fmt.Errorf("No decoder registered for %s", contentType)
	}
	return dec(p)
}

func ContextFromMessage(parent context.Context, msg *Message) context.Context {
	if msg.Expires.IsZero() {
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

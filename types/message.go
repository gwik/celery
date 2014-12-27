/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"encoding/json"
	"time"
)

// Message interface as describe at http://celery.readthedocs.org/en/latest/internals/protocol.html
type Message interface {
	Task() string
	ID() string
	Args() []interface{}
	KwArgs() map[string]interface{}
	Retries() int
	ETA() time.Time
	Expires() time.Time

	// TODO: extensions
}

/*

Scheme	MIME Type
json	application/json
yaml	application/x-yaml
pickle	application/x-python-serialize
msgpack	application/x-msgpack

*/

type jsonMessage struct {
	FTask    string                 `json:"task"`
	FId      string                 `json:"id"`
	FArgs    []interface{}          `json:"args"`
	FKwargs  map[string]interface{} `json:"kwargs"`
	FRetries int                    `json:"retries"`
	FEta     string                 `json:"eta,omitempty"`
	FExpires string                 `json:"expires,omitempty"`
}

func (jm *jsonMessage) Task() string {
	return jm.FTask
}

func (jm *jsonMessage) ID() string {
	return jm.FId
}

func (jm *jsonMessage) Args() []interface{} {
	return jm.FArgs
}

func (jm *jsonMessage) KwArgs() map[string]interface{} {
	return jm.FKwargs
}

func (jm *jsonMessage) Retries() int {
	return jm.FRetries
}

func (jm *jsonMessage) ETA() time.Time {
	if jm.FEta == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, jm.FEta)
	if err != nil {
		panic(err)
	}
	return t
}

func (jm *jsonMessage) Expires() time.Time {
	if jm.FExpires == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, jm.FExpires)
	if err != nil {
		panic(err)
	}
	return t
}

func DecodeJSONMessage(p []byte) (Message, error) {
	m := &jsonMessage{}
	err := json.Unmarshal(p, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

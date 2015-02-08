/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

/*

Package json implements decoding celery json message.
Import blank it to make application/json messages decoding available.

	import _ "github.com/gwik/celery/jsonmessage"

*/
package jsonmessage

import (
	"encoding/json"
	"time"

	"github.com/gwik/celery"
)

type jsonMessage struct {
	Task    string                 `json:"task"`
	ID      string                 `json:"id"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	Eta     string                 `json:"eta,omitempty"`
	Expires string                 `json:"expires,omitempty"`
}

func (jm *jsonMessage) ETA() time.Time {
	if jm.Eta == "" {
		return time.Now()
	}
	t, err := time.Parse(time.RFC3339Nano, jm.Eta)
	if err != nil {
		panic(err)
	}
	return t
}

func (jm *jsonMessage) ExpiresAt() time.Time {
	if jm.Expires == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, jm.Expires)
	if err != nil {
		panic(err)
	}
	return t
}

func decodeJSONMessage(p []byte) (celery.Message, error) {
	m := &jsonMessage{}
	err := json.Unmarshal(p, m)
	if err != nil {
		return celery.Message{}, err
	}
	return celery.Message{
		Task:    m.Task,
		ID:      m.ID,
		Args:    m.Args,
		KwArgs:  m.Kwargs,
		Retries: m.Retries,
		ETA:     m.ETA(),
		Expires: m.ExpiresAt(),
	}, nil
}

func init() {
	celery.RegisterMessageDecoder("application/json", decodeJSONMessage)
}

/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// copied from camlistore project
package celery

// A gate limits concurrency.
type gate struct {
	c chan struct{}
}

// newGate returns a new gate that will only permit max operations at once.
func newGate(max int) *gate {
	return &gate{make(chan struct{}, max)}
}

// Start starts an operation, blocking until the gate has room.
func (g *gate) Start() {
	g.c <- struct{}{}
}

// Done finishes an operation.
func (g *gate) Done() {
	select {
	case <-g.c:
	default:
		panic("Done called more than Start")
	}
}

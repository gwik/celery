/*
Copyright (c) 2014 Antonin Amand <antonin.amand@gmail.com>, All rights reserved.
See LICENSE file or http://www.opensource.org/licenses/BSD-3-Clause.
*/

package types

import (
	"code.google.com/p/go.net/context"
)

type Result interface {
}

// type Retry struct {
// 	After int
// 	err   error
// }

type HandleFunc func(context.Context, *Message) Result

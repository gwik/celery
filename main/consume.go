package main

import (
	"github.com/gwik/gocelery"
)

func main() {
	gocelery.Consume("celery")
}

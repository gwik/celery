package main

import (
	"github.com/gwik/gocelery"
)

func main() {
	celery.Consume("celery")
}

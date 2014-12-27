package main

import (
	"github.com/gwik/celery"
)

func main() {
	celery.Consume("celery")
}

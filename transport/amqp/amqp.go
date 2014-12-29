package amqp

import (
	"code.google.com/p/go.net/context"
	"github.com/streadway/amqp"
)

type key int

const deliveryKey = 1

func NewContext(ctx context.Context, d amqp.Delivery) context.Context {
	return context.WithValue(ctx, deliveryKey, d)
}

func DeliveryFromContext(ctx context.Context) amqp.Delivery {
	return ctx.Value(deliveryKey).(amqp.Delivery)
}

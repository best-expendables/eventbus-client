package base_consumer

import (
	"context"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/consumer_middleware"
)

type consumer struct {
	makeConsumerFunc func(context.Context) Consumer
	middleware       []consumer_middleware.Middleware
}

func (c consumer) Consume(ctx context.Context, msg *eventbusclient.Message) {
	originConsumer := c.makeConsumerFunc(ctx)
	originConsumer.Use(c.middleware...)
	originConsumer.Consume(ctx, msg)
}

func (c consumer) Use(middleware ...consumer_middleware.Middleware) {
	c.middleware = append(c.middleware, middleware...)
}

func (c consumer) Middlewares() []consumer_middleware.Middleware {
	return c.middleware
}

func MakeConsumer(
	makeConsumerFunc func(context.Context) Consumer,
	middleware []consumer_middleware.Middleware,
) Consumer {
	return &consumer{
		makeConsumerFunc: makeConsumerFunc,
		middleware:       middleware,
	}
}

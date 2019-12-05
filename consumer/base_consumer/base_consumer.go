package base_consumer

import (
	"context"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/eventbus-client/consumer/consumer_middleware"
)

type BaseConsumer struct {
	middlewares []consumer_middleware.Middleware
}

func (c *BaseConsumer) Consume(ctx context.Context, message *eventbusclient.Message) {
}

func (c *BaseConsumer) Use(middlewares ...consumer_middleware.Middleware) {
	c.middlewares = append(c.middlewares, middlewares...)
}

func (c *BaseConsumer) Middlewares() []consumer_middleware.Middleware {
	return c.middlewares
}

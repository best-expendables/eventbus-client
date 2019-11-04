package eventbusclient

import (
	"context"

	"github.com/jinzhu/gorm"
)

type consumer struct {
	makeConsumerFunc func(context.Context, *gorm.DB) Consumer
	middleware       []ConsumerMiddleware
	errHandlers      []ErrorHandler
}

func (c consumer) Consume(ctx context.Context, msg *Message) error {
	originConsumer := c.makeConsumerFunc(ctx, getDbManagerFromCtx(ctx))
	originConsumer.Use(c.middleware...)
	originConsumer.UseErrorHandler(c.errHandlers...)

	return originConsumer.Consume(ctx, msg)
}

func (c consumer) Use(middleware ...ConsumerMiddleware) {
	c.middleware = append(c.middleware, middleware...)
}

func (c consumer) UseErrorHandler(errorHandlers ...ErrorHandler) {
	c.errHandlers = append(c.errHandlers, errorHandlers...)
}

func (c consumer) Middlewares() []ConsumerMiddleware {
	return c.middleware
}

func (c consumer) ErrorHandlers() []ErrorHandler {
	return c.errHandlers
}

func MakeConsumer(
	makeConsumerFunc func(context.Context, *gorm.DB) Consumer,
	middleware []ConsumerMiddleware,
	errorHandlers []ErrorHandler,
) Consumer {
	return &consumer{
		makeConsumerFunc: makeConsumerFunc,
		middleware:       middleware,
		errHandlers:      errorHandlers,
	}
}

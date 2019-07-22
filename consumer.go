package eventbusclient

import "context"

// Handler handle message received
type Consumer interface {
	// Consumer Message, return error in case of failure
	Consume(ctx context.Context, message *Message) error

	//Specify some midldewares to be use before consuming message
	Use(middleware ...ConsumerMiddleware)

	//Specify the error handlers that will be used when the consumer return error
	UseErrorHandler(errorHandlers ...ErrorHandler)

	//Return list of middlewares being used
	Middlewares() []ConsumerMiddleware

	//Return list of error handlers being used
	ErrorHandlers() []ErrorHandler
}

type BaseConsumer struct {
	errHandlers []ErrorHandler
	middlewares []ConsumerMiddleware
}

func (c *BaseConsumer) Consume(ctx context.Context, message *Message) error {
	return nil
}

func (c *BaseConsumer) Use(middlewares ...ConsumerMiddleware) {
	c.middlewares = append(c.middlewares, middlewares...)
}

func (c *BaseConsumer) UseErrorHandler(errorHandlers ...ErrorHandler) {
	c.errHandlers = append(c.errHandlers, errorHandlers...)
}

func (c *BaseConsumer) Middlewares() []ConsumerMiddleware {
	return c.middlewares
}

func (c *BaseConsumer) ErrorHandlers() []ErrorHandler {
	return c.errHandlers
}

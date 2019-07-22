package eventbusclient

import "context"

// ConsumerMock Mock struct for consumer
type ConsumerMock struct {
	ConsumeFn         func(ctx context.Context, message *Message) error
	UseFn             func(middleware ...ConsumerMiddleware)
	UseErrorHandlerFn func(errorHandlers ...ErrorHandler)
	MiddlewaresFn     func() []ConsumerMiddleware
	ErrorHandlersFn   func() []ErrorHandler
}

func (mock ConsumerMock) Consume(ctx context.Context, message *Message) error {
	return mock.ConsumeFn(ctx, message)
}
func (mock ConsumerMock) Use(middleware ...ConsumerMiddleware) {
	mock.UseFn(middleware...)
}
func (mock ConsumerMock) UseErrorHandler(errorHandlers ...ErrorHandler) {
	mock.UseErrorHandlerFn(errorHandlers...)
}
func (mock ConsumerMock) Middlewares() []ConsumerMiddleware {
	return mock.MiddlewaresFn()
}
func (mock ConsumerMock) ErrorHandlers() []ErrorHandler {
	return mock.ErrorHandlersFn()
}

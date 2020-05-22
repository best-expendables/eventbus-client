package base_consumer

import (
	"context"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"bitbucket.org/gank-global/eventbus-client/consumer/consumer_middleware"
)

// MockConsumer mocks struct for consumer_manager
type MockConsumer struct {
	ConsumeFn     func(ctx context.Context, message *eventbusclient.Message)
	UseFn         func(middleware ...consumer_middleware.Middleware)
	MiddlewaresFn func() []consumer_middleware.Middleware
	ProccessFunc  func(ctx context.Context, message *eventbusclient.Message)
}

func (mock MockConsumer) Consume(ctx context.Context, message *eventbusclient.Message) {
	mock.ConsumeFn(ctx, message)
}
func (mock MockConsumer) Use(middleware ...consumer_middleware.Middleware) {
	mock.UseFn(middleware...)
}
func (mock MockConsumer) Middlewares() []consumer_middleware.Middleware {
	return mock.MiddlewaresFn()
}

func (mock MockConsumer) Process(ctx context.Context, message *eventbusclient.Message) {
	mock.ProccessFunc(ctx, message)
}

package producer_manager

import (
	"context"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
)

// ProducerMock producer_manager mock
type ProducerMock struct {
	UseFn        func(middleware ...PublishFuncMiddleware)
	PublishFn    func(ctx context.Context, message *eventbusclient.Message) error
	PublishRawFn func(ctx context.Context, message *eventbusclient.Message) error
	CloseFn      func() error
}

// Use method mock
func (m ProducerMock) Use(middleware ...PublishFuncMiddleware) {
	m.UseFn(middleware...)
}

// Publish method mock
func (m ProducerMock) Publish(ctx context.Context, message *eventbusclient.Message) error {
	return m.PublishFn(ctx, message)
}

func (m ProducerMock) PublishRaw(ctx context.Context, message *eventbusclient.Message) error {
	return m.PublishRawFn(ctx, message)
}

func (m ProducerMock) Close() error {
	return m.CloseFn()
}

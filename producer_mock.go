package eventbusclient

import "context"

// ProducerMock producer mock
type ProducerMock struct {
	UseFn        func(middleware ...PublishFuncMiddleware)
	PublishFn    func(ctx context.Context, message *Message) error
	PublishRawFn func(ctx context.Context, message *Message) error
	CloseFn      func() error
}

// Use method mock
func (m ProducerMock) Use(middleware ...PublishFuncMiddleware) {
	m.UseFn(middleware...)
}

// Publish method mock
func (m ProducerMock) Publish(ctx context.Context, message *Message) error {
	return m.PublishFn(ctx, message)
}

func (m ProducerMock) PublishRaw(ctx context.Context, message *Message) error {
	return m.PublishRawFn(ctx, message)
}

func (m ProducerMock) Close() error {
	return m.CloseFn()
}

package consumer_middleware

import (
	"context"

	"bitbucket.org/gank-global/trace"
	eventbusclient "github.com/best-expendables/eventbus-client"
)

func StoreTraceIdIntoContext(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, msg *eventbusclient.Message) {
		ctx = trace.ContextWithRequestID(ctx, msg.Header.TraceId)
		next(ctx, msg)
	}
}

func StoreUserIdIntoContext(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, msg *eventbusclient.Message) {
		ctx = trace.ContextWithUserID(ctx, msg.Header.UserId)
		next(ctx, msg)
	}
}

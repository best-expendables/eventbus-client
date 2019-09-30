package eventbusclient

import (
	"context"
	"fmt"
	"runtime/debug"

	"bitbucket.org/snapmartinc/logger"
	"bitbucket.org/snapmartinc/trace"
	newrelic "github.com/newrelic/go-agent"
)

type (
	//HandlerFunc publish message
	ConsumeFunc func(ctx context.Context, message *Message) error
	//HandlerFuncMiddleware middleware
	ConsumerMiddleware func(next ConsumeFunc) ConsumeFunc
)

func StoreTraceIdIntoContext(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, msg *Message) error {
		ctx = trace.ContextWithRequestID(ctx, msg.Header.TraceId)
		return next(ctx, msg)
	}
}

func StoreUserIdIntoContext(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, msg *Message) error {
		ctx = trace.ContextWithUserID(ctx, msg.Header.UserId)
		return next(ctx, msg)
	}
}

func NewrelicAcceptTraceId(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, msg *Message) error {
		if txn := newrelic.FromContext(ctx); txn != nil {
			if traceId := msg.Header.TraceId; traceId != "" {
				err := txn.AcceptDistributedTracePayload(newrelic.TransportAMQP, traceId)
				if err != nil {
					return err
				}
			}
		}
		return next(ctx, msg)
	}
}

//Log every processing message
func MessageLog(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *Message) error {
		logEntry := logger.EntryFromContext(ctx)
		if logEntry == nil {
			logEntry = loggerFactory.Logger(ctx)
			logger.ContextWithEntry(logEntry, ctx)
		}

		fields := getLogFieldFromMessage(message)
		logEntry.WithFields(fields).Info("MessageConsuming")

		return next(ctx, message)
	}
}

//Recover the consumer from panic, prevent consumer from dying unexpectedly
func Recover(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *Message) error {
		defer func() {
			if r := recover(); r != nil {
				logEntry := logger.EntryFromContext(ctx)
				if logEntry == nil {
					logEntry = loggerFactory.Logger(ctx)
				}

				fields := getLogFieldFromMessage(message)
				fields["trace"] = string(debug.Stack())
				logEntry.WithFields(fields).Error(fmt.Sprintf("MessagePanic: %v", r))
			}
		}()

		return next(ctx, message)
	}
}

func RecoverWithRetry(publisher Producer) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *Message) error {
			defer func() {
				if r := recover(); r != nil {
					//Log first
					logEntry := logger.EntryFromContext(ctx)
					if logEntry == nil {
						logEntry = loggerFactory.Logger(ctx)
					}

					fields := getLogFieldFromMessage(message)
					fields["trace"] = string(debug.Stack())
					logEntry.WithFields(fields).Error(fmt.Sprintf("MessagePanic: %v", r))

					//Publish to retry queue
					message.Header.XRetryCount++
					message.RoutingKey = fmt.Sprintf("%s.delayed", message.RoutingKey)
					publisher.Publish(ctx, message)
				}
			}()
			return next(ctx, message)
		}
	}
}

func makeConsumerMiddlewareChain(middleWares []ConsumerMiddleware, head ConsumeFunc) ConsumeFunc {
	total := len(middleWares)
	if total == 0 {
		return head
	}

	h := middleWares[0](head)
	for i := 1; i <= total-1; i++ {
		h = middleWares[i](h)
	}

	return h
}

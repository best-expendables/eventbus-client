package eventbusclient

import (
	"bitbucket.org/snapmartinc/lgo/logger"
	"bitbucket.org/snapmartinc/lgo/newrelic-context"
	"context"
	"github.com/newrelic/go-agent"
)

type (
	//PublishFunc publish message
	PublishFunc func(ctx context.Context, message *Message) error
	//PublishFuncMiddleware middleware
	PublishFuncMiddleware func(next PublishFunc) PublishFunc
)

func NewRelicPublishMiddleware(next PublishFunc) PublishFunc {
	return func(ctx context.Context, message *Message) error {
		if txn := nrcontext.GetTnxFromContext(ctx); txn != nil {
			segment := newrelic.DatastoreSegment{
				StartTime:    newrelic.StartSegmentNow(txn),
				Product:      "RabbitMQ",
				Collection:   message.RoutingKey,
				Operation:    "Publish",
				DatabaseName: message.Exchange,
			}
			defer segment.End()
		}

		return next(ctx, message)
	}
}

func PublishMessageLogMiddleware(next PublishFunc) PublishFunc {
	return func(ctx context.Context, message *Message) error {
		logEntry := logger.EntryFromContext(ctx)
		if logEntry == nil {
			logEntry = loggerFactory.Logger(ctx)
		}

		fields := getLogFieldFromMessage(message)
		logEntry.WithFields(fields).Info("MessagePublishing")

		return next(ctx, message)
	}
}

func makePublisherMiddlewareChain(middleWares []PublishFuncMiddleware, head PublishFunc) PublishFunc {
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

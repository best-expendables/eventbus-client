package producer_manager

import (
	"context"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/eventbus-client/helper"
	"bitbucket.org/snapmartinc/logger"
)

type (
	//PublishFunc publish message
	PublishFunc func(ctx context.Context, message *eventbusclient.Message) error
	//PublishFuncMiddleware middleware
	PublishFuncMiddleware func(next PublishFunc) PublishFunc
)

func PublishMessageLogMiddleware(next PublishFunc) PublishFunc {
	return func(ctx context.Context, message *eventbusclient.Message) error {
		logEntry := logger.EntryFromContext(ctx)
		if logEntry == nil {
			logEntry = helper.LoggerFactory.Logger(ctx)
		}

		fields := helper.GetLogFieldFromMessage(message)
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

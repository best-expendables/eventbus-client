package consumer_middleware

import (
	"context"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/eventbus-client/helper"
)

type ConsumeFunc func(ctx context.Context, message *eventbusclient.Message)

type Middleware func(next ConsumeFunc) ConsumeFunc

//Log every processing message
func MessageLog(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *eventbusclient.Message) {
		logEntry := helper.LoggerFromCtx(ctx)

		fields := helper.GetLogFieldFromMessage(message)
		logEntry.WithFields(fields).Info("MessageConsuming")

		next(ctx, message)
	}
}

func LogFailedMessage(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *eventbusclient.Message) {
		defer func() {
			if message.Error == nil {
				return
			}
			logEntry := helper.LoggerFromCtx(ctx)
			fields := helper.GetLogFieldFromMessage(message)
			logEntry.WithFields(fields).Errorf("MessageFailed: %s", message.Error)

		}()
		next(ctx, message)
	}
}

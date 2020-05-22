package consumer_middleware

import (
	"context"
	"fmt"
	"runtime/debug"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"bitbucket.org/gank-global/eventbus-client/helper"
	"github.com/pkg/errors"
)

//Recover the consumer_manager from panic, prevent consumer_manager from dying unexpectedly
func Recover(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *eventbusclient.Message) {
		defer func() {
			if r := recover(); r != nil {
				logEntry := helper.LoggerFromCtx(ctx)

				fields := helper.GetLogFieldFromMessage(message)
				fields["trace"] = string(debug.Stack())
				logEntry.WithFields(fields).Error(fmt.Sprintf("MessagePanic: %v", r))
			}
		}()
		next(ctx, message)
	}
}

func RecoverWithRetry(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *eventbusclient.Message) {
		defer func() {
			if r := recover(); r != nil {
				message.Error = eventbusclient.NewRetryError(errors.Wrap(message.Error, "retry with panic error"))
			}
		}()
		next(ctx, message)
	}
}

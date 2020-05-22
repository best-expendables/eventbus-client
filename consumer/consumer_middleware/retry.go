package consumer_middleware

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"bitbucket.org/gank-global/eventbus-client/helper"
	"bitbucket.org/gank-global/eventbus-client/producer_manager"
	"github.com/pkg/errors"
)

func RetryWithError(publisher producer_manager.Producer, retryCount int, delayRoutingKeys ...string) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *eventbusclient.Message) {
			defer func() {
				if message.Error == nil {
					return
				}
				if _, ok := message.Error.(eventbusclient.RetryErrorType); !ok {
					return
				}
				logEntry := helper.LoggerFromCtx(ctx)

				fields := helper.GetLogFieldFromMessage(message)
				fields["trace"] = string(debug.Stack())

				message.Header.XRetryCount = message.Header.XRetryCount + 1

				if message.Header.XRetryCount > int16(retryCount) {
					logEntry.WithFields(fields).Error(fmt.Sprintf("re: %v", message.Error))
					message.Status = eventbusclient.MessageStatusReject
					return
				}
				logEntry.WithFields(fields).Error(fmt.Sprintf("retry with error message: %v", message.Error))

				if len(delayRoutingKeys) > 0 {
					message.RoutingKey = delayRoutingKeys[0]
				} else {
					if !strings.HasSuffix(message.RoutingKey, ".delayed") {
						message.RoutingKey = fmt.Sprintf("%s.delayed", message.RoutingKey)
					}
				}
				if err := publisher.Publish(ctx, message); err != nil {
					message.Error = errors.Wrap(message.Error, fmt.Sprintf("failed to publish retry event. Error: %s", err))
					message.Status = eventbusclient.MessageStatusReject
				}
			}()
			next(ctx, message)
		}
	}

}

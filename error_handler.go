package eventbusclient

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"

	"bitbucket.org/snapmartinc/logger"
	newrelic "github.com/newrelic/go-agent"
	"github.com/streadway/amqp"
)

func LogFailedMessage(ctx context.Context, message *Message, err error) {
	logEntry := logger.EntryFromContext(ctx)
	if logEntry == nil {
		logEntry = loggerFactory.Logger(ctx)
	}

	fields := getLogFieldsFromMessage(err, message)
	logEntry.WithFields(fields).Error("MessageFailed")
}

//Log failed message to NewRelic
func LogErrorToNewRelic(nrApp newrelic.Application) ErrorHandler {
	return func(ctx context.Context, message *Message, err error) {
		var nrTxn newrelic.Transaction
		defer func() {
			_ = nrTxn.End()
		}()
		nrTxn = startTransactionForEvent(nrApp, message)
		_ = nrTxn.NoticeError(newrelic.Error{
			Message: err.Error(),
			Class:   "ConsumerError",
			Attributes: map[string]interface{}{
				"message_id": message.Id,
				"exchange":   message.Exchange,
				"routingKey": message.RoutingKey,
				"eventName":  message.Header.EventName,
				"publisher":  message.Header.Publisher,
				"timeStamp":  message.Header.Timestamp,
				"tracerId":   message.Header.TraceId,
				"userId":     message.Header.UserId,
				"retryCount": message.Header.XRetryCount,
				"entityId":   message.Payload.EntityId,
				"payload":    message.Payload.Data,
			},
		})
	}
}

func RetryWithError(publisher Producer, retryCount int) ErrorHandler {
	return func(ctx context.Context, msg *Message, err error) {
		if _, ok := err.(RetryErrorType); !ok {
			return
		}
		logEntry := logger.EntryFromContext(ctx)
		if logEntry == nil {
			logEntry = loggerFactory.Logger(ctx)
		}

		fields := getLogFieldFromMessage(msg)
		fields["trace"] = string(debug.Stack())

		msg.Header.XRetryCount = msg.Header.XRetryCount + 1

		if msg.Header.XRetryCount > int16(retryCount) {
			logEntry.WithFields(fields).Error(fmt.Sprintf("re: %v", err))
			msg.Status = MessageStatusReject
			return
		}

		logEntry.WithFields(fields).Error(fmt.Sprintf("retry with error message: %v", err))
		msg.RoutingKey = fmt.Sprintf("%s.delayed", msg.RoutingKey)
		if err := publisher.Publish(ctx, msg); err != nil {
			panic(err)
		}
	}
}

func getLogFieldsFromMessage(err error, msg *Message) logger.Fields {
	body, _ := json.Marshal(msg.Payload.Data)
	return logger.Fields{
		"error":      err.Error(),
		"id":         msg.Id,
		"exchange":   msg.Exchange,
		"routingKey": msg.RoutingKey,
		"body":       string(body),
	}
}

func ReQueueMessage(_ context.Context, delivery amqp.Delivery, _ error) {
	_ = delivery.Nack(false, true)
}

package eventbusclient

import (
	"context"
	"fmt"
	"runtime/debug"

	"bitbucket.org/snapmartinc/logger"
	newrelic "github.com/newrelic/go-agent"
	"github.com/streadway/amqp"
)

func LogFailedMessage(ctx context.Context, delivery amqp.Delivery, err error) {
	logEntry := logger.EntryFromContext(ctx)
	if logEntry == nil {
		logEntry = loggerFactory.Logger(ctx)
	}

	fields := getLogFieldsForDelivery(err, delivery)
	logEntry.WithFields(fields).Error("MessageFailed")
}

//Log failed message to NewRelic
func LogErrorToNewRelic(nrApp newrelic.Application) func(ctx context.Context, delivery amqp.Delivery, err error) {
	return func(ctx context.Context, delivery amqp.Delivery, err error) {
		var nrTxn newrelic.Transaction
		message, getMsgErr := getMessageFromDelivery(delivery)
		defer func() {
			_ = nrTxn.End()
		}()
		if getMsgErr != nil || message == nil {
			nrTxn = startTransactionForDelivery(nrApp, delivery)
			_ = nrTxn.NoticeError(newrelic.Error{
				Message: err.Error(),
				Class:   "ConsumerError",
			})
		} else {
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
}

func RetryWithError(publisher Producer, retryCount int) func(ctx context.Context, delivery amqp.Delivery, err error) {
	return func(ctx context.Context, delivery amqp.Delivery, err error) {
		if _, ok := err.(RetryErrorType); !ok {
			return
		}
		message, getMsgErr := getMessageFromDelivery(delivery)
		if getMsgErr != nil || message == nil {
			return
		}
		logEntry := logger.EntryFromContext(ctx)
		if logEntry == nil {
			logEntry = loggerFactory.Logger(ctx)
		}

		fields := getLogFieldFromMessage(message)
		fields["trace"] = string(debug.Stack())

		message.Header.XRetryCount = message.Header.XRetryCount + 1

		if message.Header.XRetryCount > int16(retryCount) {
			logEntry.WithFields(fields).Error(fmt.Sprintf("re: %v", err))
			if err := delivery.Reject(false); err != nil {
				panic(err)
			}
			err = messageRejectedError
			return
		}

		logEntry.WithFields(fields).Error(fmt.Sprintf("retry with error message: %v", err))
		message.RoutingKey = fmt.Sprintf("%s.delayed", message.RoutingKey)
		if err := publisher.Publish(ctx, message); err != nil {
			panic(err)
		}
	}
}

func getLogFieldsForDelivery(err error, delivery amqp.Delivery) logger.Fields {
	return logger.Fields{
		"error":      err.Error(),
		"id":         delivery.MessageId,
		"exchange":   delivery.Exchange,
		"routingKey": delivery.RoutingKey,
		"body":       string(delivery.Body),
	}
}

func ReQueueMessage(_ context.Context, delivery amqp.Delivery, _ error) {
	_ = delivery.Nack(false, true)
}

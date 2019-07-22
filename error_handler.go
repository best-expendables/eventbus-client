package eventbusclient

import (
	"bitbucket.org/snapmartinc/logger"
	"context"
	"github.com/newrelic/go-agent"
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
		if getMsgErr != nil || message == nil {
			nrTxn = startTransactionForDelivery(nrApp, delivery)
			nrTxn.NoticeError(newrelic.Error{
				Message: err.Error(),
				Class:   "ConsumerError",
			})
		} else {
			nrTxn = startTransactionForEvent(nrApp, message)
			nrTxn.NoticeError(newrelic.Error{
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

		defer nrTxn.End()
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

func RejectMessage(_ context.Context, delivery amqp.Delivery, _ error) {
	//delivery.Ack(false)
}

func ReQueueMessage(_ context.Context, delivery amqp.Delivery, _ error) {
	delivery.Nack(false, true)
}

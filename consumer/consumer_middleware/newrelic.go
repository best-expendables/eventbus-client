package consumer_middleware

import (
	"context"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"bitbucket.org/gank-global/eventbus-client/helper"
	newrelic "github.com/newrelic/go-agent"
)

func LogErrorToNewRelic(nrApp newrelic.Application) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *eventbusclient.Message) {
			defer func() {
				if message.Error == nil {
					return
				}
				var nrTxn newrelic.Transaction
				defer func() {
					_ = nrTxn.End()
				}()
				nrTxn = helper.StartTransactionForEvent(nrApp, message)
				_ = nrTxn.NoticeError(newrelic.Error{
					Message: message.Error.Error(),
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
			}()
			next(ctx, message)
		}
	}
}

func WithNewRelicForConsumer(nrApp newrelic.Application) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, msg *eventbusclient.Message) {
			txn := helper.StartTransactionForEvent(nrApp, msg)
			defer func() {
				_ = txn.End()
			}()
			ctx = newrelic.NewContext(ctx, txn)
			next(ctx, msg)
		}
	}
}

func NewrelicAcceptTraceId(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, msg *eventbusclient.Message) {
		if txn := newrelic.FromContext(ctx); txn != nil {
			if traceId := msg.Header.TraceId; traceId != "" {
				msg.Error = txn.AcceptDistributedTracePayload(newrelic.TransportAMQP, traceId)
			}
		}
		next(ctx, msg)
	}
}

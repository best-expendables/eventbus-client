package eventbusclient

import (
	"bitbucket.org/snapmartinc/logger"
	"bitbucket.org/snapmartinc/trace"
	"bitbucket.org/snapmartinc/user-service-client"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/newrelic/go-agent"
	"github.com/streadway/amqp"
	"strings"
	"time"
)

var loggerFactory logger.Factory

func init() {
	loggerFactory = logger.NewLoggerFactory(logger.InfoLevel)
}

type contextKey int

const gormContextKey contextKey = 0

var ErrDbEmpty = errors.New("db connection invalid")

const (
	EntityIdAttr      = "entityId"
	PayloadAttr       = "payload"
	transactionPrefix = "eventbus_consumer"
)

func SetGormToContext(ctx context.Context, dbConn *gorm.DB) context.Context {
	return context.WithValue(ctx, gormContextKey, dbConn)
}

func GetGormFromContext(ctx context.Context) *gorm.DB {
	if db := ctx.Value(gormContextKey); db != nil {
		return db.(*gorm.DB)
	}

	panic(ErrDbEmpty)
}

func startTransactionForEvent(nrApp newrelic.Application, job *Message) newrelic.Transaction {
	nrTxn := nrApp.StartTransaction(
		fmt.Sprintf("%s:%s", transactionPrefix, job.Header.EventName),
		nil,
		nil,
	)
	nrTxn.AddAttribute(logger.FieldTraceId, job.Header.TraceId)
	nrTxn.AddAttribute(logger.FieldUserId, job.Header.UserId)
	nrTxn.AddAttribute(EntityIdAttr, job.Payload.EntityId)
	nrTxn.AddAttribute(PayloadAttr, job.Payload.Data)

	return nrTxn
}

func startTransactionForDelivery(nrApp newrelic.Application, d amqp.Delivery) newrelic.Transaction {
	nrTxn := nrApp.StartTransaction(
		fmt.Sprintf("%s:%s", transactionPrefix, getHeader(d.Headers, "EventName")),
		nil,
		nil,
	)

	nrTxn.AddAttribute(logger.FieldTraceId, getHeader(d.Headers, "TraceId"))
	nrTxn.AddAttribute(logger.FieldUserId, getHeader(d.Headers, "UserId"))
	nrTxn.AddAttribute(PayloadAttr, string(d.Body))

	return nrTxn
}

//From Message data, build the fields needed for logger
func getLogFieldFromMessage(message *Message) logger.Fields {
	data, _ := json.Marshal(message.Payload.Data)
	fields := logger.Fields{
		"message_id":        message.Id,
		"exchange":          message.Exchange,
		"routing_key":       message.RoutingKey,
		"message_timestamp": message.Header.Timestamp.Unix(),
		"publisher":         message.Header.Publisher,
		"event_name":        message.Header.EventName,
		"trace_id":          message.Header.TraceId,
		"user_id":           message.Header.UserId,
		"x_retry_count":     message.Header.XRetryCount,
		"entity_id":         message.Payload.EntityId,
		"payload":           string(data),
	}
	return fields
}

//From rabbitmq delivery data, build the original message
func getMessageFromDelivery(d amqp.Delivery) (*Message, error) {
	payload := Payload{}
	err := json.Unmarshal(d.Body, &payload)
	if err != nil {
		return nil, err
	}

	t, _ := d.Headers["Timestamp"].(int64)
	if t == 0 {
		t, _ = d.Headers["timestamp"].(int64)
	}
	timestamp := time.Unix(t, 0)
	retry, _ := d.Headers["xRetryCount"].(int16)
	msg := Message{
		Id:         d.MessageId,
		Exchange:   d.Exchange,
		RoutingKey: d.RoutingKey,
		Header: Header{
			Timestamp:   timestamp,
			Publisher:   getHeader(d.Headers, "Publisher"),
			EventName:   getHeader(d.Headers, "EventName"),
			TraceId:     getHeader(d.Headers, "TraceId"),
			UserId:      getHeader(d.Headers, "UserId"),
			XRetryCount: retry,
		},
		Payload: payload,
	}

	return &msg, err
}

//Build the context from Message, the context now will have data for logging and tracing
func contextFromMessage(msg *Message) context.Context {
	ctx := trace.ContextWithRequestID(context.Background(), msg.Header.TraceId)
	ctx = userclient.ContextWithUser(ctx, &userclient.User{Id: msg.Header.UserId})
	loggerFactory := logger.NewLoggerFactory(logger.InfoLevel)

	return logger.ContextWithEntry(loggerFactory.Logger(ctx), ctx)
}

func getHeader(headers amqp.Table, key string) string {
	result := getString(headers[key])
	if result == "" {
		camelKey := strings.ToLower(key[0:1]) + key[1:]
		result = getString(headers[camelKey])
	}

	return result
}

func getString(input interface{}) string {
	result, _ := input.(string)

	return result
}

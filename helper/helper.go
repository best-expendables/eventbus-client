package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	eventbusclient "bitbucket.org/snapmartinc/eventbus-client"
	"bitbucket.org/snapmartinc/logger"
	"bitbucket.org/snapmartinc/trace"
	userclient "bitbucket.org/snapmartinc/user-service-client"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	newrelic "github.com/newrelic/go-agent"
	"github.com/streadway/amqp"
)

type gormKeyInt int
type redisClientKeyInt int

const gormContextKey gormKeyInt = iota
const redisClientContextKey redisClientKeyInt = iota

var ErrDbEmpty = errors.New("db connection invalid")
var ErrRedisEmpty = errors.New("Redis connection invalid")

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
func SetRedisClientToContext(ctx context.Context, c *redis.Client) context.Context {
	return context.WithValue(ctx, redisClientContextKey, c)
}

func GetRedisClientFromContext(ctx context.Context) *redis.Client {
	if db := ctx.Value(redisClientContextKey); db != nil {
		return db.(*redis.Client)
	}
	panic(ErrRedisEmpty)
}

func StartTransactionForEvent(nrApp newrelic.Application, job *eventbusclient.Message) newrelic.Transaction {
	nrTxn := nrApp.StartTransaction(fmt.Sprintf("%s:%s", transactionPrefix, job.Header.EventName), nil, nil)
	_ = nrTxn.AddAttribute(logger.FieldTraceId, job.Header.TraceId)
	_ = nrTxn.AddAttribute(logger.FieldUserId, job.Header.UserId)
	_ = nrTxn.AddAttribute(EntityIdAttr, job.Payload.EntityId)
	_ = nrTxn.AddAttribute(PayloadAttr, job.Payload.Data)
	return nrTxn
}

func StartTransactionForMessage(nrApp newrelic.Application, m *eventbusclient.Message) newrelic.Transaction {
	nrTxn := nrApp.StartTransaction(fmt.Sprintf("%s:%s", transactionPrefix, m.Header.EventName), nil, nil)
	_ = nrTxn.AddAttribute(logger.FieldTraceId, m.Header.TraceId)
	_ = nrTxn.AddAttribute(logger.FieldUserId, m.Header.UserId)
	_ = nrTxn.AddAttribute(PayloadAttr, m.Payload.Data)

	return nrTxn
}

//From Message data, build the fields needed for logger
func GetLogFieldFromMessage(message *eventbusclient.Message) logger.Fields {
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
func GetMessageFromDelivery(d amqp.Delivery) *eventbusclient.Message {
	payload := eventbusclient.Payload{}
	msg := &eventbusclient.Message{}
	err := json.Unmarshal(d.Body, &payload)
	if err != nil {
		msg.Error = err
		return msg
	}

	t, _ := d.Headers["Timestamp"].(int64)
	if t == 0 {
		t, _ = d.Headers["timestamp"].(int64)
	}
	timestamp := time.Unix(t, 0)
	retry, _ := d.Headers["xRetryCount"].(int16)
	msg = &eventbusclient.Message{
		Id:         d.MessageId,
		Exchange:   d.Exchange,
		RoutingKey: d.RoutingKey,
		Header: eventbusclient.Header{
			Timestamp:   timestamp,
			Publisher:   getHeader(d.Headers, "Publisher"),
			EventName:   getHeader(d.Headers, "EventName"),
			TraceId:     getHeader(d.Headers, "TraceId"),
			UserId:      getHeader(d.Headers, "UserId"),
			XRetryCount: retry,
		},
		Payload: payload,
		Status:  eventbusclient.MessageStatusAck,
	}

	return msg
}

//Build the context from Message, the context now will have data for logging and tracing
func ContextFromMessage(msg *eventbusclient.Message) context.Context {
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

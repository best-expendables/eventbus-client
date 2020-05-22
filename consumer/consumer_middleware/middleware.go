package consumer_middleware

import (
	"context"

	eventbusclient "bitbucket.org/gank-global/eventbus-client"
	"bitbucket.org/gank-global/eventbus-client/helper"
	nrcontext "bitbucket.org/gank-global/newrelic-context"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/gorm"
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

func SetDbManagerToCtx(dbConn *gorm.DB) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *eventbusclient.Message) {
			newdb := nrcontext.SetTxnToGorm(ctx, dbConn)
			ctx = helper.SetGormToContext(ctx, newdb)
			next(ctx, message)
		}
	}
}

func NewRelicToRedis(c *redis.Client) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *eventbusclient.Message) {
			redisClientWithNR := nrcontext.WrapRedisClient(ctx, c)
			ctx = helper.SetRedisClientToContext(ctx, redisClientWithNR)
			next(ctx, message)
		}
	}
}

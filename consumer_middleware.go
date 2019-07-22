package eventbusclient

import (
	"bitbucket.org/snapmartinc/lgo/logger"
	"bitbucket.org/snapmartinc/lgo/newrelic-context"
	"context"
	"fmt"
	"github.com/jinzhu/gorm"
	"github.com/newrelic/go-agent"
	"runtime/debug"
)

type (
	//HandlerFunc publish message
	ConsumeFunc func(ctx context.Context, message *Message) error
	//HandlerFuncMiddleware middleware
	ConsumerMiddleware func(next ConsumeFunc) ConsumeFunc
)

//Start new relic transaction before process 1 message, end transaction after finishing
func NewRelic(nrApp newrelic.Application) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *Message) error {
			txn := startTransactionForEvent(nrApp, message)
			defer txn.End()
			ctx = nrcontext.ContextWithTxn(ctx, txn)

			return next(ctx, message)
		}
	}
}

//Log every processing message
func MessageLog(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *Message) error {
		logEntry := logger.EntryFromContext(ctx)
		if logEntry == nil {
			logEntry = loggerFactory.Logger(ctx)
			logger.ContextWithEntry(logEntry, ctx)
		}

		fields := getLogFieldFromMessage(message)
		logEntry.WithFields(fields).Info("MessageConsuming")

		return next(ctx, message)
	}
}

//Recover the consumer from panic, prevent consumer from dying unexpectedly
func Recover(next ConsumeFunc) ConsumeFunc {
	return func(ctx context.Context, message *Message) error {
		defer func() {
			if r := recover(); r != nil {
				logEntry := logger.EntryFromContext(ctx)
				if logEntry == nil {
					logEntry = loggerFactory.Logger(ctx)
				}

				fields := getLogFieldFromMessage(message)
				fields["trace"] = string(debug.Stack())
				logEntry.WithFields(fields).Error(fmt.Sprintf("MessagePanic: %v", r))
			}
		}()

		return next(ctx, message)
	}
}

func RecoverWithRetry(publisher Producer) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *Message) error {
			defer func() {
				if r := recover(); r != nil {
					//Log first
					logEntry := logger.EntryFromContext(ctx)
					if logEntry == nil {
						logEntry = loggerFactory.Logger(ctx)
					}

					fields := getLogFieldFromMessage(message)
					fields["trace"] = string(debug.Stack())
					logEntry.WithFields(fields).Error(fmt.Sprintf("MessagePanic: %v", r))

					//Publish to retry queue
					message.Header.XRetryCount++
					message.RoutingKey = fmt.Sprintf("%s_delayed", message.RoutingKey)
					publisher.Publish(ctx, message)
				}
			}()

			return next(ctx, message)
		}
	}
}

//Put NewRelic transaction into gorm context
func Gorm(dbConn *gorm.DB) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *Message) error {
			dbConn = nrcontext.SetTxnToGorm(ctx, dbConn)
			ctx = SetGormToContext(ctx, dbConn)

			return next(ctx, message)
		}
	}
}

//Start 1 database transaction before processing message
func WithTransaction(dbConn *gorm.DB) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *Message) error {
			txn := dbConn.Begin()
			if txn.Error != nil {
				return txn.Error
			}
			SetGormToContext(ctx, nrcontext.SetTxnToGorm(ctx, txn))

			err := next(ctx, message)
			if err != nil {
				return txn.Rollback().Error
			}

			return txn.Commit().Error
		}
	}
}

func makeConsumerMiddlewareChain(middleWares []ConsumerMiddleware, head ConsumeFunc) ConsumeFunc {
	total := len(middleWares)
	if total == 0 {
		return head
	}

	h := middleWares[0](head)
	for i := 1; i <= total-1; i++ {
		h = middleWares[i](h)
	}

	return h
}

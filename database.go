package eventbusclient

import (
	"context"

	nrcontext "bitbucket.org/snapmartinc/newrelic-context"
	"github.com/jinzhu/gorm"
)

const (
	dbManagerKey = iota
)

func SetDBToCtxConsumer(dbConn *gorm.DB) func(next ConsumeFunc) ConsumeFunc {
	return func(next ConsumeFunc) ConsumeFunc {
		return func(ctx context.Context, message *Message) error {
			newdb := nrcontext.SetTxnToGorm(ctx, dbConn)
			ctx = context.WithValue(ctx, dbManagerKey, newdb)
			return next(ctx, message)
		}
	}
}

// keep it private to avoid use in strange places
func getDbManagerFromCtx(ctx context.Context) *gorm.DB {
	return ctx.Value(dbManagerKey).(*gorm.DB)
}

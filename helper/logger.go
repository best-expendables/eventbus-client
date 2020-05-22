package helper

import (
	"context"

	"github.com/best-expendables/logger"
)

var LoggerFactory logger.Factory

func init() {
	LoggerFactory = logger.NewLoggerFactory(logger.InfoLevel)
}

func LoggerFromCtx(ctx context.Context) logger.Entry {
	logEntry := logger.EntryFromContext(ctx)
	if logEntry == nil {
		logEntry = LoggerFactory.Logger(ctx)
		logger.ContextWithEntry(logEntry, ctx)
	}
	return logEntry
}

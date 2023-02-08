package logutil

import (
	"go.uber.org/zap"
)

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic(logger *zap.Logger) {
	if e := recover(); e != nil {
		logger.Fatal("panic", zap.Reflect("recover", e))
	}
}

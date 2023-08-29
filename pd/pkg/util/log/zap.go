package logutil

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// LogPanicAndExit logs the panic reason and stack, then exit the process.
// Should be used with a `defer`.
func LogPanicAndExit(logger *zap.Logger) {
	if e := recover(); e != nil {
		logger.Fatal("panic and exit", zap.Reflect("recover", e))
	}
}

// LogPanic logs the panic reason and stack
// Should be used with a `defer`.
func LogPanic(logger *zap.Logger) {
	if e := recover(); e != nil {
		logger.Error("panic", zap.Reflect("recover", e))
		panic(e)
	}
}

// IncreaseLevel increases the log level of logger if the level is enabled.
func IncreaseLevel(logger *zap.Logger, level zapcore.Level) *zap.Logger {
	if logger.Core().Enabled(level) {
		return logger.WithOptions(zap.IncreaseLevel(level))
	}
	return logger
}

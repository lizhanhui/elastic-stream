package logutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogPanicAndExit(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	obsZapCore, obsLogs := observer.New(zap.InfoLevel)
	obsLogger := zap.New(obsZapCore, zap.WithFatalHook(zapcore.WriteThenPanic))

	logPanic := func() {
		defer LogPanicAndExit(obsLogger)
		panic("test panic here")
	}

	recovered := make(chan interface{})
	go func() {
		defer func() {
			recovered <- recover()
		}()
		logPanic()
	}()
	<-recovered

	re.Equal([]observer.LoggedEntry{{
		Entry: zapcore.Entry{Level: zapcore.FatalLevel, Message: "panic and exit"},
		Context: []zapcore.Field{{
			Key:       "recover",
			Type:      zapcore.ReflectType,
			Interface: "test panic here",
		}},
	}}, obsLogs.AllUntimed())
}

func TestLogPanic(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	obsZapCore, obsLogs := observer.New(zap.InfoLevel)
	obsLogger := zap.New(obsZapCore)

	logPanic := func() {
		defer LogPanic(obsLogger)
		panic("test panic here")
	}

	recovered := make(chan interface{})
	go func() {
		defer func() {
			recovered <- recover()
		}()
		logPanic()
	}()
	<-recovered

	re.Equal([]observer.LoggedEntry{{
		Entry: zapcore.Entry{Level: zapcore.ErrorLevel, Message: "panic"},
		Context: []zapcore.Field{{
			Key:       "recover",
			Type:      zapcore.ReflectType,
			Interface: "test panic here",
		}},
	}}, obsLogs.AllUntimed())
}

func TestIncreaseLevel(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	logger := zap.NewExample()
	re.True(logger.Core().Enabled(zapcore.DebugLevel))

	// increase to an enabled level
	logger = IncreaseLevel(logger, zapcore.WarnLevel)
	re.False(logger.Core().Enabled(zapcore.InfoLevel))
	re.True(logger.Core().Enabled(zapcore.WarnLevel))

	// increase to a disabled level
	logger = IncreaseLevel(logger, zapcore.InfoLevel)
	re.False(logger.Core().Enabled(zapcore.InfoLevel))
	re.True(logger.Core().Enabled(zapcore.WarnLevel))
}

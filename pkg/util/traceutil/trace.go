package traceutil

import (
	"context"
)

type traceIDKey struct{}

// SetTraceID sets the traceID into the context.
func SetTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey{}, traceID)
}

// TraceID returns the traceID from the context.
func TraceID(ctx context.Context) string {
	if traceID, ok := ctx.Value(traceIDKey{}).(string); ok {
		return traceID
	}
	return ""
}

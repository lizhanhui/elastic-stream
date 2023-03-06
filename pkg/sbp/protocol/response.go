package protocol

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
)

// Response is an SBP response
type Response interface {
	// Marshal encodes the Response using the specified format
	Marshal(fmt format.Format) ([]byte, error)
}

// ListRangesResponse is a response to operation.ListRange
type ListRangesResponse struct {
	// The time in milliseconds to throttle the client, due to a quota violation or the server is too busy.
	ThrottleTimeMs ThrottleTimeMs

	// The responses of list ranges request
	ListResponses []ListRangesResult

	// The top level error code, or 0 if there was no error.
	ErrorCode ErrorCode

	// The error message, or omitted if there was no error.
	ErrorMessage string
}

//nolint:revive // EXC0012 comment already exists in interface
func (l *ListRangesResponse) Marshal(fmt format.Format) ([]byte, error) {
	return getFormatter(fmt).marshalListRangesResponse(l)
}

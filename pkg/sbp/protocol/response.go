package protocol

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/format"
)

// Response is an SBP response
type Response interface {
	// Marshal encodes the Response using the specified format
	Marshal(fmt format.Format) ([]byte, error)
}

// ListRangeResponse is a response to operation.ListRange
type ListRangeResponse struct {
	// The time in milliseconds to throttle the client, due to a quota violation or the server is too busy.
	ThrottleTimeMs uint32

	// The responses of list ranges request
	ListResponses []ListRangesResult
}

//nolint:revive // EXC0012 comment already exists in interface
func (l *ListRangeResponse) Marshal(fmt format.Format) ([]byte, error) {
	return getFormatter(fmt).marshalListRangeResponse(l)
}

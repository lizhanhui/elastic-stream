package protocol

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
)

// Response is an SBP response
type Response interface {
	// Marshal encodes the Response using the specified format.
	// The returned byte slice is not nil when and only when the error is nil.
	// The returned byte slice should be freed after use.
	Marshal(fmt format.Format) ([]byte, error)

	// IsEnd returns true if the response is the last response of a request.
	IsEnd() bool
}

// singleResponse represents a response that corresponds to a single request.
// It is used when a request is expected to have only one response.
type singleResponse struct{}

func (s *singleResponse) IsEnd() bool {
	return true
}

// SystemErrorResponse is used to return the error code and error message if the system error flag of sbp is set.
type SystemErrorResponse struct {
	singleResponse
	ErrorCode    ErrorCode
	ErrorMessage string
}

//nolint:revive // EXC0012 comment already exists in interface
func (se *SystemErrorResponse) Marshal(fmt format.Format) ([]byte, error) {
	return getFormatter(fmt).marshalSystemErrorResponse(se)
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

	// HasNext indicates whether there are more responses after this one.
	HasNext bool
}

//nolint:revive // EXC0012 comment already exists in interface
func (lr *ListRangesResponse) Marshal(fmt format.Format) ([]byte, error) {
	return getFormatter(fmt).marshalListRangesResponse(lr)
}

//nolint:revive // EXC0012 comment already exists in interface
func (lr *ListRangesResponse) IsEnd() bool {
	return !lr.HasNext
}

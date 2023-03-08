package protocol

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
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
	*rpcfb.SystemErrorResponseT
}

//nolint:revive // EXC0012 comment already exists in interface
func (se *SystemErrorResponse) Marshal(fmt format.Format) ([]byte, error) {
	switch fmt {
	case format.FlatBuffer():
		return fbutil.Marshal(se.SystemErrorResponseT), nil
	default:
		return nil, errUnsupported
	}
}

// ListRangesResponse is a response to operation.ListRange
type ListRangesResponse struct {
	*rpcfb.ListRangesResponseT

	// HasNext indicates whether there are more responses after this one.
	HasNext bool
}

//nolint:revive // EXC0012 comment already exists in interface
func (lr *ListRangesResponse) Marshal(fmt format.Format) ([]byte, error) {
	switch fmt {
	case format.FlatBuffer():
		return fbutil.Marshal(lr.ListRangesResponseT), nil
	default:
		return nil, errUnsupported
	}
}

//nolint:revive // EXC0012 comment already exists in interface
func (lr *ListRangesResponse) IsEnd() bool {
	return !lr.HasNext
}

package protocol

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/format"
)

// Request is an SBP request
type Request interface {
	// Unmarshal decodes data into the Request using the specified format
	Unmarshal(fmt format.Format, data []byte) error
}

// ListRangeRequest is a request to operation.ListRange
type ListRangeRequest struct {
	TimeoutMs uint32

	// The range owner could be a data node or a list of streams.
	RangeOwners []RangeOwner
}

//nolint:revive // EXC0012 comment already exists in interface
func (l *ListRangeRequest) Unmarshal(fmt format.Format, data []byte) error {
	return getFormatter(fmt).unmarshalListRangeRequest(data, l)
}

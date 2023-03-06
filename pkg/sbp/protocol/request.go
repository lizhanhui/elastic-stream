package protocol

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
)

// Request is an SBP request
type Request interface {
	// Unmarshal decodes data into the Request using the specified format
	Unmarshal(fmt format.Format, data []byte) error
}

// ListRangesRequest is a request to operation.ListRange
type ListRangesRequest struct {
	TimeoutMs TimeoutMs

	// The range owner could be a data node or a list of streams.
	RangeOwners []RangeOwner
}

//nolint:revive // EXC0012 comment already exists in interface
func (l *ListRangesRequest) Unmarshal(fmt format.Format, data []byte) error {
	return getFormatter(fmt).unmarshalListRangesRequest(data, l)
}

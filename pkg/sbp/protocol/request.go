package protocol

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
)

var errUnsupported = errors.New("unsupported format")

// Request is an SBP request
type Request interface {
	// Unmarshal decodes data into the Request using the specified format
	// data is expired after the call, so the implementation should copy the data if needed
	Unmarshal(fmt format.Format, data []byte) error
}

// ListRangesRequest is a request to operation.ListRange
type ListRangesRequest struct {
	*rpcfb.ListRangesRequestT
}

//nolint:revive // EXC0012 comment already exists in interface
func (l *ListRangesRequest) Unmarshal(fmt format.Format, data []byte) error {
	switch fmt {
	case format.FlatBuffer():
		rpcfb.GetRootAsListRangesRequest(data, 0).UnPackTo(l.ListRangesRequestT)
		return nil
	default:
		return errUnsupported
	}
}

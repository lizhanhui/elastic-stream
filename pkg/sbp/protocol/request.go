package protocol

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
)

const (
	_unsupportedReqErrMsg = "unsupported request format: %s"
)

// Request is an SBP request
type Request interface {
	// Unmarshal decodes data into the Request using the specified format
	// data is expired after the call, so the implementation should copy the data if needed
	Unmarshal(fmt format.Format, data []byte) error
}

type unmarshaler interface {
	flatBufferUnmarshaler
	protoBufferUnmarshaler
	jsonUnmarshaler
}

type flatBufferUnmarshaler interface {
	unmarshalFlatBuffer(data []byte) error
}

type protoBufferUnmarshaler interface {
	unmarshalProtoBuffer(data []byte) error
}

type jsonUnmarshaler interface {
	unmarshalJSON(data []byte) error
}

// baseRequest is a default implementation of unmarshaler.
type baseRequest struct{}

func (b *baseRequest) unmarshalFlatBuffer(_ []byte) error {
	return errors.Errorf(_unsupportedReqErrMsg, format.FlatBuffer())
}

func (b *baseRequest) unmarshalProtoBuffer(_ []byte) error {
	return errors.Errorf(_unsupportedReqErrMsg, format.ProtoBuffer())
}

func (b *baseRequest) unmarshalJSON(_ []byte) error {
	return errors.Errorf(_unsupportedReqErrMsg, format.JSON())
}

// ListRangesRequest is a request to operation.ListRange
type ListRangesRequest struct {
	baseRequest
	*rpcfb.ListRangesRequestT
}

func (l *ListRangesRequest) unmarshalFlatBuffer(data []byte) error {
	l.ListRangesRequestT = rpcfb.GetRootAsListRangesRequest(data, 0).UnPack()
	return nil
}

func (l *ListRangesRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(l, fmt, data)
}

func unmarshal(request unmarshaler, fmt format.Format, data []byte) error {
	switch fmt {
	case format.FlatBuffer():
		return request.unmarshalFlatBuffer(data)
	case format.ProtoBuffer():
		return request.unmarshalProtoBuffer(data)
	case format.JSON():
		return request.unmarshalJSON(data)
	default:
		return errors.Errorf(_unsupportedReqErrMsg, fmt)
	}
}

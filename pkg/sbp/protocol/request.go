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

// ListRangesRequest is a request to operation.OpListRanges
type ListRangesRequest struct {
	baseRequest
	*rpcfb.ListRangesRequestT
}

func (lr *ListRangesRequest) unmarshalFlatBuffer(data []byte) error {
	lr.ListRangesRequestT = rpcfb.GetRootAsListRangesRequest(data, 0).UnPack()
	return nil
}

func (lr *ListRangesRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(lr, fmt, data)
}

// CreateStreamsRequest is a request to operation.OpCreateStreams
type CreateStreamsRequest struct {
	baseRequest
	*rpcfb.CreateStreamsRequestT
}

func (cs *CreateStreamsRequest) unmarshalFlatBuffer(data []byte) error {
	cs.CreateStreamsRequestT = rpcfb.GetRootAsCreateStreamsRequest(data, 0).UnPack()
	return nil
}

func (cs *CreateStreamsRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(cs, fmt, data)
}

// DeleteStreamsRequest is a request to operation.OpDeleteStreams
type DeleteStreamsRequest struct {
	baseRequest
	*rpcfb.DeleteStreamsRequestT
}

func (ds *DeleteStreamsRequest) unmarshalFlatBuffer(data []byte) error {
	ds.DeleteStreamsRequestT = rpcfb.GetRootAsDeleteStreamsRequest(data, 0).UnPack()
	return nil
}

func (ds *DeleteStreamsRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(ds, fmt, data)
}

// UpdateStreamsRequest is a request to operation.OpUpdateStreams
type UpdateStreamsRequest struct {
	baseRequest
	*rpcfb.UpdateStreamsRequestT
}

func (us *UpdateStreamsRequest) unmarshalFlatBuffer(data []byte) error {
	us.UpdateStreamsRequestT = rpcfb.GetRootAsUpdateStreamsRequest(data, 0).UnPack()
	return nil
}

func (us *UpdateStreamsRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(us, fmt, data)
}

// DescribeStreamsRequest is a request to operation.OpDescribeStreams
type DescribeStreamsRequest struct {
	baseRequest
	*rpcfb.DescribeStreamsRequestT
}

func (ds *DescribeStreamsRequest) unmarshalFlatBuffer(data []byte) error {
	ds.DescribeStreamsRequestT = rpcfb.GetRootAsDescribeStreamsRequest(data, 0).UnPack()
	return nil
}

func (ds *DescribeStreamsRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(ds, fmt, data)
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

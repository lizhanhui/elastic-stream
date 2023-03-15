package protocol

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
)

const (
	_unsupportedRespErrMsg = "unsupported response format: %s"
)

// Response is an SBP response
type Response interface {
	// Marshal encodes the Response using the specified format.
	// The returned byte slice is not nil when and only when the error is nil.
	// The returned byte slice should be freed after use.
	Marshal(fmt format.Format) ([]byte, error)

	// Error sets the error status of the response.
	Error(status *rpcfb.StatusT)

	// IsEnd returns true if the response is the last response of a request.
	IsEnd() bool
}

type marshaller interface {
	flatBufferMarshaller
	protoBufferMarshaller
	jsonMarshaller
}

type flatBufferMarshaller interface {
	marshalFlatBuffer() ([]byte, error)
}

type protoBufferMarshaller interface {
	marshalProtoBuffer() ([]byte, error)
}

type jsonMarshaller interface {
	marshalJSON() ([]byte, error)
}

// baseResponse is a default implementation of marshaller.
type baseResponse struct{}

func (b *baseResponse) marshalFlatBuffer() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedRespErrMsg, format.FlatBuffer())
}

func (b *baseResponse) marshalProtoBuffer() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedRespErrMsg, format.ProtoBuffer())
}

func (b *baseResponse) marshalJSON() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedRespErrMsg, format.JSON())
}

// singleResponse represents a response that corresponds to a single request.
// It is used when a request is expected to have only one response.
type singleResponse struct{}

func (s *singleResponse) IsEnd() bool {
	return true
}

// SystemErrorResponse is used to return the error code and error message if the system error flag of sbp is set.
type SystemErrorResponse struct {
	baseResponse
	singleResponse
	*rpcfb.SystemErrorResponseT
}

func (se *SystemErrorResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(se.SystemErrorResponseT), nil
}

func (se *SystemErrorResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(se, fmt)
}

func (se *SystemErrorResponse) Error(status *rpcfb.StatusT) {
	se.Status = status
}

// ListRangesResponse is a response to operation.OpListRanges
type ListRangesResponse struct {
	baseResponse
	*rpcfb.ListRangesResponseT

	// HasNext indicates whether there are more responses after this one.
	HasNext bool
}

func (lr *ListRangesResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(lr.ListRangesResponseT), nil
}

func (lr *ListRangesResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(lr, fmt)
}

func (lr *ListRangesResponse) Error(status *rpcfb.StatusT) {
	lr.Status = status
}

func (lr *ListRangesResponse) IsEnd() bool {
	return !lr.HasNext
}

// CreateStreamsResponse is a response to operation.OpCreateStreams
type CreateStreamsResponse struct {
	baseResponse
	singleResponse
	*rpcfb.CreateStreamsResponseT
}

func (cs *CreateStreamsResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(cs.CreateStreamsResponseT), nil
}

func (cs *CreateStreamsResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(cs, fmt)
}

func (cs *CreateStreamsResponse) Error(status *rpcfb.StatusT) {
	cs.Status = status
}

// DeleteStreamsResponse is a response to operation.OpDeleteStreams
type DeleteStreamsResponse struct {
	baseResponse
	singleResponse
	*rpcfb.DeleteStreamsResponseT
}

func (ds *DeleteStreamsResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(ds.DeleteStreamsResponseT), nil
}

func (ds *DeleteStreamsResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(ds, fmt)
}

func (ds *DeleteStreamsResponse) Error(status *rpcfb.StatusT) {
	ds.Status = status
}

// UpdateStreamsResponse is a response to operation.OpUpdateStreams
type UpdateStreamsResponse struct {
	baseResponse
	singleResponse
	*rpcfb.UpdateStreamsResponseT
}

func (us *UpdateStreamsResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(us.UpdateStreamsResponseT), nil
}

func (us *UpdateStreamsResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(us, fmt)
}

func (us *UpdateStreamsResponse) Error(status *rpcfb.StatusT) {
	us.Status = status
}

// DescribeStreamsResponse is a response to operation.OpDescribeStreams
type DescribeStreamsResponse struct {
	baseResponse
	singleResponse
	*rpcfb.DescribeStreamsResponseT
}

func (ds *DescribeStreamsResponse) marshalFlatBuffer() ([]byte, error) {
	return fbutil.Marshal(ds.DescribeStreamsResponseT), nil
}

func (ds *DescribeStreamsResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(ds, fmt)
}

func (ds *DescribeStreamsResponse) Error(status *rpcfb.StatusT) {
	ds.Status = status
}

func marshal(response marshaller, fmt format.Format) ([]byte, error) {
	switch fmt {
	case format.FlatBuffer():
		return response.marshalFlatBuffer()
	case format.ProtoBuffer():
		return response.marshalProtoBuffer()
	case format.JSON():
		return response.marshalJSON()
	default:
		return nil, errors.Errorf(_unsupportedRespErrMsg, fmt)
	}
}

package protocol

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
)

const (
	_unsupportedFmtErrMsg     = "unsupported format: %s"
	_unsupportedRespFmtErrMsg = "unsupported response format: %s"
	_unsupportedReqFmtErrMsg  = "unsupported request format: %s"
)

type base interface {
	// Unmarshal decodes data into the Request using the specified format.
	// data is expired after the call, so the implementation should copy the data if needed.
	Unmarshal(fmt format.Format, data []byte) error

	// Marshal encodes the Response using the specified format.
	// The returned byte slice is not nil when and only when the error is nil.
	// The returned byte slice should be freed after use.
	Marshal(fmt format.Format) ([]byte, error)
}

func (req *baseRequest) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(req, fmt, data)
}

func (req *baseRequest) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(req, fmt)
}

func (req *baseRequest) marshalFlatBuffer() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedReqFmtErrMsg, format.FlatBuffer())
}

func (req *baseRequest) marshalProtoBuffer() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedReqFmtErrMsg, format.ProtoBuffer())
}

func (req *baseRequest) marshalJSON() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedReqFmtErrMsg, format.JSON())
}

func (req *baseRequest) unmarshalFlatBuffer(_ []byte) error {
	return errors.Errorf(_unsupportedReqFmtErrMsg, format.FlatBuffer())
}

func (req *baseRequest) unmarshalProtoBuffer(_ []byte) error {
	return errors.Errorf(_unsupportedReqFmtErrMsg, format.ProtoBuffer())
}

func (req *baseRequest) unmarshalJSON(_ []byte) error {
	return errors.Errorf(_unsupportedReqFmtErrMsg, format.JSON())
}

func (resp *baseResponse) Unmarshal(fmt format.Format, data []byte) error {
	return unmarshal(resp, fmt, data)
}

func (resp *baseResponse) Marshal(fmt format.Format) ([]byte, error) {
	return marshal(resp, fmt)
}

func (resp *baseResponse) marshalFlatBuffer() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedRespFmtErrMsg, format.FlatBuffer())
}

func (resp *baseResponse) marshalProtoBuffer() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedRespFmtErrMsg, format.ProtoBuffer())
}

func (resp *baseResponse) marshalJSON() ([]byte, error) {
	return nil, errors.Errorf(_unsupportedRespFmtErrMsg, format.JSON())
}

func (resp *baseResponse) unmarshalFlatBuffer(_ []byte) error {
	return errors.Errorf(_unsupportedRespFmtErrMsg, format.FlatBuffer())
}

func (resp *baseResponse) unmarshalProtoBuffer(_ []byte) error {
	return errors.Errorf(_unsupportedRespFmtErrMsg, format.ProtoBuffer())
}

func (resp *baseResponse) unmarshalJSON(_ []byte) error {
	return errors.Errorf(_unsupportedRespFmtErrMsg, format.JSON())
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

func marshal(m marshaller, fmt format.Format) ([]byte, error) {
	switch fmt {
	case format.FlatBuffer():
		return m.marshalFlatBuffer()
	case format.ProtoBuffer():
		return m.marshalProtoBuffer()
	case format.JSON():
		return m.marshalJSON()
	default:
		return nil, errors.Errorf(_unsupportedFmtErrMsg, fmt)
	}
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

func unmarshal(m unmarshaler, fmt format.Format, data []byte) error {
	switch fmt {
	case format.FlatBuffer():
		return m.unmarshalFlatBuffer(data)
	case format.ProtoBuffer():
		return m.unmarshalProtoBuffer(data)
	case format.JSON():
		return m.unmarshalJSON(data)
	default:
		return errors.Errorf(_unsupportedFmtErrMsg, fmt)
	}
}

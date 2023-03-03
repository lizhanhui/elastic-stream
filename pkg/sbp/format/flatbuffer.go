package format

import (
	"sync"

	flatbuffers "github.com/google/flatbuffers/go"
)

// TODO

type flatBufferFormatter struct {
	builderPool sync.Pool
}

func newFlatBufferFormatter() *flatBufferFormatter {
	return &flatBufferFormatter{
		builderPool: sync.Pool{New: func() interface{} {
			return flatbuffers.NewBuilder(1024)
		}},
	}
}

func (f *flatBufferFormatter) MarshalListRangeRequest(request *ListRangeRequest) ([]byte, error) {
	_ = request
	panic("implement me")
}

func (f *flatBufferFormatter) UnmarshalListRangeRequest(bytes []byte, request *ListRangeRequest) error {
	_ = bytes
	_ = request
	panic("implement me")
}

func (f *flatBufferFormatter) MarshalListRangeResponse(response *ListRangeResponse) ([]byte, error) {
	_ = response
	panic("implement me")
}

func (f *flatBufferFormatter) UnmarshalListRangeResponse(bytes []byte, response *ListRangeResponse) error {
	_ = bytes
	_ = response
	panic("implement me")
}

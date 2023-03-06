package protocol

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

func (f *flatBufferFormatter) unmarshalListRangeRequest(bytes []byte, request *ListRangeRequest) error {
	_ = bytes
	_ = request
	panic("implement me")
}

func (f *flatBufferFormatter) marshalListRangeResponse(response *ListRangeResponse) ([]byte, error) {
	_ = response
	panic("implement me")
}

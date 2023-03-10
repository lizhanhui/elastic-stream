package endpoint

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// Stream defines operations on stream.
type Stream interface {
	CreateStream(stream *rpcfb.StreamT) (*rpcfb.StreamT, error)
	DeleteStream(streamID int64) (*rpcfb.StreamT, error)
	UpdateStream(stream *rpcfb.StreamT) (*rpcfb.StreamT, error)
	ForEachStream(f func(stream *rpcfb.StreamT)) error
}

// CreateStream creates a new stream based on the given stream and returns it.
func (e *Endpoint) CreateStream(stream *rpcfb.StreamT) (*rpcfb.StreamT, error) {
	//TODO implement me
	panic("implement me")
}

// DeleteStream deletes the stream associated with the given stream ID and returns the deleted stream.
func (e *Endpoint) DeleteStream(streamID int64) (*rpcfb.StreamT, error) {
	//TODO implement me
	panic("implement me")
}

// UpdateStream updates the properties of the stream and returns it.
func (e *Endpoint) UpdateStream(stream *rpcfb.StreamT) (*rpcfb.StreamT, error) {
	//TODO implement me
	panic("implement me")
}

// ForEachStream calls the given function for each stream in the storage.
func (e *Endpoint) ForEachStream(f func(stream *rpcfb.StreamT)) error {
	//TODO implement me
	panic("implement me")
}

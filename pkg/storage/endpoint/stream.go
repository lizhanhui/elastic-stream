package endpoint

import (
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/storage/kv"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
)

const (
	_streamPath       = "stream"
	_streamFormat     = _streamPath + kv.KeySeparator + "%020d" // max length of int64 is 20
	_streamKeyLen     = len(_streamPath) + len(kv.KeySeparator) + 20
	_streamRangeLimit = 1e4
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
	logger := e.lg

	stream.StreamId = e.nextStreamID()
	streamInfo := fbutil.Marshal(stream)
	prev, err := e.Put(streamPath(stream.StreamId), streamInfo)
	mcache.Free(streamInfo)
	if err != nil {
		logger.Error("failed to save stream", zap.Int64("stream-id", stream.StreamId), zap.Error(err))
		return nil, errors.Wrap(err, "save stream")
	}
	if prev != nil {
		logger.Warn("stream already exists", zap.Int64("stream-id", stream.StreamId))
	}

	return stream, nil
}

// DeleteStream deletes the stream associated with the given stream ID and returns the deleted stream.
func (e *Endpoint) DeleteStream(streamID int64) (*rpcfb.StreamT, error) {
	logger := e.lg

	prev, err := e.Delete(streamPath(streamID))
	if err != nil {
		logger.Error("failed to delete stream", zap.Int64("stream-id", streamID), zap.Error(err))
		return nil, errors.Wrap(err, "delete stream")
	}
	if prev == nil {
		logger.Warn("stream not found when delete stream", zap.Int64("stream-id", streamID))
		return nil, nil
	}

	return rpcfb.GetRootAsStream(prev, 0).UnPack(), nil
}

// UpdateStream updates the properties of the stream and returns it.
func (e *Endpoint) UpdateStream(stream *rpcfb.StreamT) (*rpcfb.StreamT, error) {
	logger := e.lg

	streamInfo := fbutil.Marshal(stream)
	prev, err := e.Put(streamPath(stream.StreamId), streamInfo)
	mcache.Free(streamInfo)
	if err != nil {
		logger.Error("failed to update stream", zap.Int64("stream-id", stream.StreamId), zap.Error(err))
		return nil, errors.Wrap(err, "update stream")
	}
	if prev == nil {
		logger.Warn("stream not found when update stream", zap.Int64("stream-id", stream.StreamId))
	}

	return stream, nil
}

// ForEachStream calls the given function for every stream in the storage.
func (e *Endpoint) ForEachStream(f func(stream *rpcfb.StreamT)) error {
	var startID int64 = 1
	for startID > 0 {
		nextID, err := e.forEachStreamLimited(f, startID, _streamRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachStreamLimited(f func(stream *rpcfb.StreamT), startID int64, limit int64) (nextID int64, err error) {
	logger := e.lg

	startKey := streamPath(startID)
	kvs, err := e.GetByRange(kv.Range{StartKey: startKey, EndKey: e.endStreamPath()}, limit)
	if err != nil {
		logger.Error("failed to get streams", zap.Int64("start-id", startID), zap.Int64("limit", limit), zap.Error(err))
		return 0, errors.Wrap(err, "get streams")
	}

	for _, streamKV := range kvs {
		stream := rpcfb.GetRootAsStream(streamKV.Value, 0).UnPack()
		nextID = stream.StreamId + 1
		f(stream)
	}

	// return 0 if no more streams
	if int64(len(kvs)) < limit {
		nextID = 0
	}
	return
}

func (e *Endpoint) nextStreamID() int64 {
	return e.streamID.Add(1)
}

func (e *Endpoint) endStreamPath() []byte {
	return e.GetPrefixRangeEnd([]byte(_streamPath + kv.KeySeparator))
}

func streamPath(streamID int64) []byte {
	res := make([]byte, 0, _streamKeyLen)
	res = fmt.Appendf(res, _streamFormat, streamID)
	return res
}

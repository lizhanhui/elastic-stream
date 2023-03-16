package endpoint

import (
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/kv"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
)

const (
	_streamPath       = "stream"
	_streamPrefix     = _streamPath + kv.KeySeparator
	_streamFormat     = _streamPath + kv.KeySeparator + "%020d" // max length of int64 is 20
	_streamKeyLen     = len(_streamPath) + len(kv.KeySeparator) + 20
	_streamRangeLimit = 1e4
)

// Stream defines operations on stream.
type Stream interface {
	CreateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error)
	DeleteStreams(streamIDs []int64) ([]*rpcfb.StreamT, error)
	UpdateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error)
	GetStream(streamID int64) (*rpcfb.StreamT, error)
	ForEachStream(f func(stream *rpcfb.StreamT) error) error
}

// CreateStreams creates new streams based on the given streams and returns them.
func (e *Endpoint) CreateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error) {
	logger := e.lg

	kvs := make([]kv.KeyValue, 0, len(streams))
	for _, stream := range streams {
		streamInfo := fbutil.Marshal(stream)
		kvs = append(kvs, kv.KeyValue{
			Key:   streamPath(stream.StreamId),
			Value: streamInfo,
		})
	}

	prevKvs, err := e.BatchPut(kvs, true)
	for _, keyValue := range kvs {
		mcache.Free(keyValue.Value)
	}
	if err != nil {
		streamIDs := make([]int64, 0, len(streams))
		for _, stream := range streams {
			streamIDs = append(streamIDs, stream.StreamId)
		}
		logger.Error("failed to save streams", zap.Int64s("stream-ids", streamIDs), zap.Error(err))
		return nil, errors.Wrap(err, "save streams")
	}
	if len(prevKvs) != 0 {
		streamKeys := make([][]byte, 0, len(prevKvs))
		for _, prevKv := range prevKvs {
			streamKeys = append(streamKeys, prevKv.Key)
		}
		logger.Warn("streams already exist, will override them", zap.ByteStrings("existed-stream-ids", streamKeys))
	}

	return streams, nil
}

// DeleteStreams deletes the streams with the given stream ids and returns them.
func (e *Endpoint) DeleteStreams(streamIDs []int64) ([]*rpcfb.StreamT, error) {
	logger := e.lg

	streamPaths := make([][]byte, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		streamPaths = append(streamPaths, streamPath(streamID))
	}
	prevKvs, err := e.BatchDelete(streamPaths, true)
	if err != nil {
		logger.Error("failed to delete stream", zap.Int64s("stream-ids", streamIDs), zap.Error(err))
		return nil, errors.Wrap(err, "delete stream")
	}
	if len(prevKvs) < len(streamIDs) {
		streamKeys := make([][]byte, 0, len(prevKvs))
		for _, prevKv := range prevKvs {
			streamKeys = append(streamKeys, prevKv.Key)
		}
		logger.Warn("stream not found when delete stream", zap.ByteStrings("existed-stream-ids", streamKeys), zap.Int64s("stream-ids", streamIDs))
		return nil, nil
	}

	streams := make([]*rpcfb.StreamT, 0, len(prevKvs))
	for _, prevKv := range prevKvs {
		streams = append(streams, rpcfb.GetRootAsStream(prevKv.Value, 0).UnPack())
	}

	return streams, nil
}

// UpdateStreams updates the streams with the given streams and returns them.
func (e *Endpoint) UpdateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error) {
	logger := e.lg

	kvs := make([]kv.KeyValue, 0, len(streams))
	for _, stream := range streams {
		if stream.StreamId <= 0 {
			return nil, errors.New("invalid stream id")
		}
		streamInfo := fbutil.Marshal(stream)
		kvs = append(kvs, kv.KeyValue{
			Key:   streamPath(stream.StreamId),
			Value: streamInfo,
		})
	}

	prevKvs, err := e.BatchPut(kvs, true)
	for _, keyValue := range kvs {
		mcache.Free(keyValue.Value)
	}
	if err != nil {
		streamIDs := make([]int64, 0, len(streams))
		for _, stream := range streams {
			streamIDs = append(streamIDs, stream.StreamId)
		}
		logger.Error("failed to update stream", zap.Int64s("stream-ids", streamIDs), zap.Error(err))
		return nil, errors.Wrap(err, "update stream")
	}
	if len(prevKvs) < len(streams) {
		streamKeys := make([][]byte, 0, len(prevKvs))
		for _, prevKv := range prevKvs {
			streamKeys = append(streamKeys, prevKv.Key)
		}
		streamIDs := make([]int64, 0, len(streams))
		for _, stream := range streams {
			streamIDs = append(streamIDs, stream.StreamId)
		}
		logger.Warn("streams not found when update streams, will create them", zap.ByteStrings("existed-stream-ids", streamKeys), zap.Int64s("stream-ids", streamIDs))
		return nil, nil
	}

	return streams, nil
}

// GetStream gets the stream with the given stream id.
func (e *Endpoint) GetStream(streamID int64) (*rpcfb.StreamT, error) {
	logger := e.lg

	value, err := e.Get(streamPath(streamID))
	if err != nil {
		logger.Error("failed to get stream", zap.Int64("stream-id", streamID), zap.Error(err))
		return nil, errors.Wrap(err, "get stream")
	}
	if value == nil {
		return nil, nil
	}

	return rpcfb.GetRootAsStream(value, 0).UnPack(), nil
}

// ForEachStream calls the given function for every stream in the storage.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachStream(f func(stream *rpcfb.StreamT) error) error {
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

func (e *Endpoint) forEachStreamLimited(f func(stream *rpcfb.StreamT) error, startID int64, limit int64) (nextID int64, err error) {
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
		err = f(stream)
		if err != nil {
			return 0, err
		}
	}

	// return 0 if no more streams
	if int64(len(kvs)) < limit {
		nextID = 0
	}
	return
}

func (e *Endpoint) endStreamPath() []byte {
	return e.GetPrefixRangeEnd([]byte(_streamPrefix))
}

func streamPath(streamID int64) []byte {
	res := make([]byte, 0, _streamKeyLen)
	res = fmt.Appendf(res, _streamFormat, streamID)
	return res
}

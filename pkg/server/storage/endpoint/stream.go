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
	_streamIDFormat = _int64Format
	_streamIDLen    = _int64Len

	_streamPath   = "stream"
	_streamPrefix = _streamPath + kv.KeySeparator
	_streamFormat = _streamPath + kv.KeySeparator + _streamIDFormat // max length of int64 is 20
	_streamKeyLen = len(_streamPath) + len(kv.KeySeparator) + _streamIDLen

	_streamByRangeLimit = 1e4
)

// CreateStreamParam defines the parameters of creating a stream.
type CreateStreamParam struct {
	*rpcfb.StreamT
	*rpcfb.RangeT
}

// Stream defines operations on stream.
type Stream interface {
	CreateStreams(params []*CreateStreamParam) ([]*rpcfb.CreateStreamResultT, error)
	DeleteStreams(streamIDs []int64) ([]*rpcfb.StreamT, error)
	UpdateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error)
	GetStream(streamID int64) (*rpcfb.StreamT, error)
	ForEachStream(f func(stream *rpcfb.StreamT) error) error
}

// CreateStreams creates new streams based on the given streams and returns them.
func (e *Endpoint) CreateStreams(params []*CreateStreamParam) ([]*rpcfb.CreateStreamResultT, error) {
	logger := e.lg

	results := make([]*rpcfb.CreateStreamResultT, 0, len(params))
	kvs := make([]kv.KeyValue, 0, len(params)*2)
	for _, param := range params {
		kvs = append(kvs, kv.KeyValue{
			Key:   streamPath(param.StreamT.StreamId),
			Value: fbutil.Marshal(param.StreamT),
		})
		kvs = append(kvs, kv.KeyValue{
			Key:   rangePathInSteam(param.StreamT.StreamId, param.RangeT.RangeIndex),
			Value: fbutil.Marshal(param.RangeT),
		})
		results = append(results, &rpcfb.CreateStreamResultT{
			Stream: param.StreamT,
			Range:  param.RangeT,
		})
	}

	prevKvs, err := e.BatchPut(kvs, true)
	for _, keyValue := range kvs {
		mcache.Free(keyValue.Value)
	}
	if err != nil {
		streamIDs := make([]int64, 0, len(params))
		for _, param := range params {
			streamIDs = append(streamIDs, param.StreamT.StreamId)
		}
		logger.Error("failed to save streams", zap.Int64s("stream-ids", streamIDs), zap.Error(err))
		return nil, errors.WithMessage(err, "save streams")
	}
	if len(prevKvs) != 0 {
		existedStreamIDs := streamIDsFromPaths(prevKvs)
		logger.Warn("streams already exist, will override them", zap.Int64s("existed-stream-ids", existedStreamIDs))
	}

	return results, nil
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
		return nil, errors.WithMessage(err, "delete stream")
	}
	if len(prevKvs) < len(streamIDs) {
		existedStreamIDs := streamIDsFromPaths(prevKvs)
		logger.Warn("streams not found when delete streams", zap.Int64s("existed-stream-ids", existedStreamIDs), zap.Int64s("stream-ids", streamIDs))
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
		if stream.StreamId < MinStreamID {
			return nil, errors.Errorf("invalid stream id: %d < %d", stream.StreamId, MinStreamID)
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
		return nil, errors.WithMessage(err, "update stream")
	}
	if len(prevKvs) < len(streams) {
		existedStreamIDs := streamIDsFromPaths(prevKvs)
		streamIDs := make([]int64, 0, len(streams))
		for _, stream := range streams {
			streamIDs = append(streamIDs, stream.StreamId)
		}
		logger.Warn("streams not found when update streams, will create them", zap.Int64s("existed-stream-ids", existedStreamIDs), zap.Int64s("stream-ids", streamIDs))
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
		return nil, errors.WithMessage(err, "get stream")
	}
	if value == nil {
		return nil, nil
	}

	return rpcfb.GetRootAsStream(value, 0).UnPack(), nil
}

// ForEachStream calls the given function for every stream in the storage.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachStream(f func(stream *rpcfb.StreamT) error) error {
	var startID = MinStreamID
	for startID >= MinStreamID {
		nextID, err := e.forEachStreamLimited(f, startID, _streamByRangeLimit)
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
		return MinStreamID - 1, errors.WithMessage(err, "get streams")
	}

	for _, streamKV := range kvs {
		stream := rpcfb.GetRootAsStream(streamKV.Value, 0).UnPack()
		nextID = stream.StreamId + 1
		err = f(stream)
		if err != nil {
			return MinStreamID - 1, err
		}
	}

	if int64(len(kvs)) < limit {
		// no more streams
		nextID = MinStreamID - 1
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

func streamIDsFromPaths(prevKvs []kv.KeyValue) []int64 {
	streamIDs := make([]int64, 0, len(prevKvs))
	for _, prevKv := range prevKvs {
		streamID, _ := streamIDFromPath(prevKv.Key)
		streamIDs = append(streamIDs, streamID)
	}
	return streamIDs
}

func streamIDFromPath(path []byte) (streamID int64, err error) {
	_, err = fmt.Sscanf(string(path), _streamFormat, &streamID)
	if err != nil {
		err = errors.WithMessagef(err, "parse stream id from path %s", string(path))
	}
	return
}

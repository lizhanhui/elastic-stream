package endpoint

import (
	"fmt"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/kv"
)

const (
	_rangeIDFormat = _int32Format
	_rangeIDLen    = _int32Len

	// ranges in stream
	_rangeStreamPath         = "range"
	_rangeStreamPrefix       = "s"
	_rangeStreamPrefixFormat = _rangeStreamPrefix + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeStreamPath + kv.KeySeparator
	_rangeStreamFormat       = _rangeStreamPrefix + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeStreamPath + kv.KeySeparator + _rangeIDFormat
	_rangeStreamKeyLen       = len(_rangeStreamPrefix) + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + len(_rangeStreamPath) + len(kv.KeySeparator) + _rangeIDLen

	// ranges on data node
	_rangeNodePath         = "stream-range"
	_rangeNodePrefix       = "n"
	_rangeNodePrefixFormat = _rangeNodePrefix + kv.KeySeparator + _dataNodeIDFormat + kv.KeySeparator + _rangeNodePath + kv.KeySeparator
	_rangeNodeFormat       = _rangeNodePrefix + kv.KeySeparator + _dataNodeIDFormat + kv.KeySeparator + _rangeNodePath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeIDFormat
	_rangeNodeKeyLen       = len(_rangeNodePrefix) + len(kv.KeySeparator) + _dataNodeIDLen + len(kv.KeySeparator) + len(_rangeNodePath) + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + _rangeIDLen

	_rangeByRangeLimit = 1e4
)

type Range interface {
	GetRange(t *rpcfb.RangeIdT) (*rpcfb.RangeT, error)
	GetRangesByStream(streamID int64) ([]*rpcfb.RangeT, error)
	ForEachRangeInStream(streamID int64, f func(r *rpcfb.RangeT) error) error
	GetRangeIDsByDataNode(dataNodeID int32) ([]*rpcfb.RangeIdT, error)
	ForEachRangeIDOnDataNode(dataNodeID int32, f func(rangeID *rpcfb.RangeIdT) error) error
}

func (e *Endpoint) GetRange(rangeID *rpcfb.RangeIdT) (*rpcfb.RangeT, error) {
	logger := e.lg

	key := rangePathInSteam(rangeID.StreamId, rangeID.RangeIndex)
	value, err := e.Get(key)
	if err != nil {
		logger.Error("failed to get range", zap.Int64("stream-id", rangeID.StreamId), zap.Int32("range-index", rangeID.RangeIndex), zap.Error(err))
		return nil, errors.WithMessage(err, "get range")
	}

	return rpcfb.GetRootAsRange(value, 0).UnPack(), nil
}

// GetRangesByStream returns the ranges of the given stream.
func (e *Endpoint) GetRangesByStream(streamID int64) ([]*rpcfb.RangeT, error) {
	logger := e.lg

	// TODO set capacity
	ranges := make([]*rpcfb.RangeT, 0)
	err := e.ForEachRangeInStream(streamID, func(r *rpcfb.RangeT) error {
		ranges = append(ranges, r)
		return nil
	})
	if err != nil {
		logger.Error("failed to get ranges", zap.Int64("stream-id", streamID), zap.Error(err))
		return nil, errors.WithMessage(err, "get ranges")
	}

	return ranges, nil
}

// ForEachRangeInStream calls the given function f for each range in the stream.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachRangeInStream(streamID int64, f func(r *rpcfb.RangeT) error) error {
	var startID = MinRangeIndex
	for startID >= MinRangeIndex {
		nextID, err := e.forEachRangeInStreamLimited(streamID, f, startID, _rangeByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachRangeInStreamLimited(streamID int64, f func(r *rpcfb.RangeT) error, startID int32, limit int64) (nextID int32, err error) {
	logger := e.lg

	startKey := rangePathInSteam(streamID, startID)
	kvs, err := e.GetByRange(kv.Range{StartKey: startKey, EndKey: e.endRangePathInStream(streamID)}, limit)
	if err != nil {
		logger.Error("failed to get ranges", zap.Int64("stream-id", streamID), zap.Int32("start-id", startID), zap.Int64("limit", limit), zap.Error(err))
		return MinRangeIndex - 1, errors.WithMessage(err, "get ranges")
	}

	for _, rangeKV := range kvs {
		r := rpcfb.GetRootAsRange(rangeKV.Value, 0).UnPack()
		nextID = r.RangeIndex + 1
		err = f(r)
		if err != nil {
			return MinRangeIndex - 1, err
		}
	}

	if int64(len(kvs)) < limit {
		// no more ranges
		nextID = MinRangeIndex - 1
	}
	return
}

func (e *Endpoint) endRangePathInStream(streamID int64) []byte {
	return e.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeStreamPrefixFormat, streamID)))
}

func rangePathInSteam(streamID int64, rangeIndex int32) []byte {
	res := make([]byte, 0, _rangeStreamKeyLen)
	res = fmt.Appendf(res, _rangeStreamFormat, streamID, rangeIndex)
	return res
}

func (e *Endpoint) GetRangeIDsByDataNode(dataNodeID int32) ([]*rpcfb.RangeIdT, error) {
	logger := e.lg

	// TODO set capacity
	rangeIDs := make([]*rpcfb.RangeIdT, 0)
	err := e.ForEachRangeIDOnDataNode(dataNodeID, func(rangeID *rpcfb.RangeIdT) error {
		rangeIDs = append(rangeIDs, rangeID)
		return nil
	})
	if err != nil {
		logger.Error("failed to get range ids", zap.Int32("data-node-id", dataNodeID), zap.Error(err))
		return nil, errors.WithMessage(err, "get range ids")
	}

	return rangeIDs, nil
}

// ForEachRangeIDOnDataNode calls the given function f for each range on the data node.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachRangeIDOnDataNode(dataNodeID int32, f func(rangeID *rpcfb.RangeIdT) error) error {
	startID := &rpcfb.RangeIdT{StreamId: MinStreamID, RangeIndex: MinRangeIndex}
	for startID != nil && startID.StreamId >= MinStreamID && startID.RangeIndex >= MinRangeIndex {
		nextID, err := e.forEachRangeIDOnDataNodeLimited(dataNodeID, f, startID, _rangeByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachRangeIDOnDataNodeLimited(dataNodeID int32, f func(rangeID *rpcfb.RangeIdT) error, startID *rpcfb.RangeIdT, limit int64) (nextID *rpcfb.RangeIdT, err error) {
	logger := e.lg

	startKey := rangePathOnDataNode(dataNodeID, startID.StreamId, startID.RangeIndex)
	kvs, err := e.GetByRange(kv.Range{StartKey: startKey, EndKey: e.endRangePathOnDataNode(dataNodeID)}, limit)
	if err != nil {
		logger.Error("failed to get range ids", zap.Int32("data-node-id", dataNodeID), zap.Int64("start-stream-id", startID.StreamId), zap.Int32("start-range-index", startID.RangeIndex), zap.Int64("limit", limit), zap.Error(err))
		return nil, errors.WithMessage(err, "get range ids")
	}

	nextID = startID
	for _, rangeKV := range kvs {
		_, streamID, rangeIndex, err := rangeIDFromPath(rangeKV.Key)
		if err != nil {
			return nil, err
		}
		nextID.StreamId = streamID
		nextID.RangeIndex = rangeIndex + 1
		err = f(&rpcfb.RangeIdT{StreamId: streamID, RangeIndex: rangeIndex})
		if err != nil {
			return nil, err
		}
	}

	// return nil if no more ranges
	if int64(len(kvs)) < limit {
		nextID = nil
	}
	return
}

func (e *Endpoint) endRangePathOnDataNode(dataNodeID int32) []byte {
	return e.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeNodePrefixFormat, dataNodeID)))
}

func rangePathOnDataNode(dataNodeID int32, streamID int64, rangeIndex int32) []byte {
	res := make([]byte, 0, _rangeNodeKeyLen)
	res = fmt.Appendf(res, _rangeNodeFormat, dataNodeID, streamID, rangeIndex)
	return res
}

func rangeIDFromPath(path []byte) (dataNodeID int32, streamID int64, rangeIndex int32, err error) {
	_, err = fmt.Sscanf(string(path), _rangeNodeFormat, &dataNodeID, &streamID, &rangeIndex)
	if err != nil {
		err = errors.WithMessagef(err, "parse range path: %s", string(path))
	}
	return
}

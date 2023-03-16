package endpoint

import (
	"fmt"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/kv"
)

const (
	_rangeStreamPrefix = "s"
	_rangePath         = "range"
	_rangePrefixFormat = _rangeStreamPrefix + kv.KeySeparator + "%020d" + kv.KeySeparator + _rangePath + kv.KeySeparator
	_rangeFormat       = _rangeStreamPrefix + kv.KeySeparator + "%020d" + kv.KeySeparator + _rangePath + kv.KeySeparator + "%011d"
	_rangeKeyLen       = len(_rangeStreamPrefix) + len(kv.KeySeparator) + 20 + len(kv.KeySeparator) + len(_rangePath) + len(kv.KeySeparator) + 11
	_rangeRangeLimit   = 1e4
)

type Range interface {
	GetRanges(streamID int64) ([]*rpcfb.RangeT, error)
	ForEachRange(streamID int64, f func(r *rpcfb.RangeT) error) error
}

// GetRanges returns the ranges of the given stream.
func (e *Endpoint) GetRanges(streamID int64) ([]*rpcfb.RangeT, error) {
	logger := e.lg

	// TODO set capacity
	ranges := make([]*rpcfb.RangeT, 0)
	err := e.ForEachRange(streamID, func(r *rpcfb.RangeT) error {
		ranges = append(ranges, r)
		return nil
	})
	if err != nil {
		logger.Error("failed to get ranges", zap.Int64("stream-id", streamID), zap.Error(err))
		return nil, errors.Wrap(err, "get ranges")
	}

	return ranges, nil
}

// ForEachRange calls the given function f for each range in the stream.
// If f returns an error, the iteration is stopped and the error is returned.
func (e *Endpoint) ForEachRange(streamID int64, f func(r *rpcfb.RangeT) error) error {
	var startID int32 = 1
	for startID > 0 {
		nextID, err := e.ForEachRangeLimited(streamID, f, startID, _rangeRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) ForEachRangeLimited(streamID int64, f func(r *rpcfb.RangeT) error, startID int32, limit int64) (nextID int32, err error) {
	logger := e.lg

	startKey := rangePath(streamID, startID)
	kvs, err := e.GetByRange(kv.Range{StartKey: startKey, EndKey: e.endRangePath(streamID)}, limit)
	if err != nil {
		logger.Error("failed to get ranges", zap.Int64("stream-id", streamID), zap.Int32("start-id", startID), zap.Int64("limit", limit), zap.Error(err))
		return 0, errors.Wrap(err, "get ranges")
	}

	for _, rangeKV := range kvs {
		r := rpcfb.GetRootAsRange(rangeKV.Value, 0).UnPack()
		nextID = r.RangeIndex + 1
		err = f(r)
		if err != nil {
			return 0, err
		}
	}

	// return 0 if no more ranges
	if int64(len(kvs)) < limit {
		nextID = 0
	}
	return
}

func (e *Endpoint) endRangePath(streamID int64) []byte {
	return e.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangePrefixFormat, streamID)))
}

func rangePath(streamID int64, rangeID int32) []byte {
	res := make([]byte, 0, _rangeKeyLen)
	res = fmt.Appendf(res, _rangeFormat, streamID, rangeID)
	return res
}

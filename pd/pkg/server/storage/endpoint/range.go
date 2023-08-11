package endpoint

import (
	"context"
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/fbutil"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

const (
	_writableRangeEnd int64 = -1

	_rangeIDFormat = _int32Format
	_rangeIDLen    = _int32Len

	// ranges in stream
	_rangeStreamPath         = "ranges"
	_rangePrefix             = _rangeStreamPath + kv.KeySeparator
	_rangeStreamPrefixFormat = _rangeStreamPath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator
	_rangeStreamFormat       = _rangeStreamPath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeIDFormat
	_rangeStreamKeyLen       = len(_rangeStreamPath) + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + _rangeIDLen

	// ranges on range server
	_rangeOnServerPath               = "ranges-rs"
	_rangeOnServerPrefixFormat       = _rangeOnServerPath + kv.KeySeparator + _rangeServerIDFormat + kv.KeySeparator
	_rangeOnServerStreamPrefixFormat = _rangeOnServerPath + kv.KeySeparator + _rangeServerIDFormat + kv.KeySeparator + _streamIDFormat + kv.KeySeparator
	_rangeOnServerFormat             = _rangeOnServerPath + kv.KeySeparator + _rangeServerIDFormat + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeIDFormat
	_rangeOnServerKeyLen             = len(_rangeOnServerPath) + len(kv.KeySeparator) + _rangeServerIDLen + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + _rangeIDLen

	_rangeByRangeLimit = 1e4
)

type RangeID struct {
	StreamID int64
	Index    int32
}

// ChooseServersFunc is the function to choose range servers.
// It returns model.ErrNotEnoughRangeServers if there are not enough range servers to allocate.
type ChooseServersFunc func(replica int8, blackList []*rpcfb.RangeServerT) ([]*rpcfb.RangeServerT, error)

type RangeEndpoint interface {
	// CreateRange creates the range and returns it.
	// It returns model.ErrStreamNotFound if the stream does not exist.
	// It returns model.ErrInvalidStreamEpoch if the stream epoch mismatches.
	// It returns model.ErrCreateRangeTwice and the range if the range is already created with the same parameters.
	// It returns model.ErrRangeAlreadyExist if the range is already created.
	// It returns model.ErrInvalidRangeIndex if the range index is invalid.
	// It returns model.ErrCreateRangeBeforeSeal if the last range is not sealed.
	// It returns model.ErrInvalidRangeStart if the start offset is invalid.
	// It returns model.ErrNotEnoughRangeServers if there are not enough range servers to allocate.
	CreateRange(ctx context.Context, p *model.CreateRangeParam, f ChooseServersFunc) (*rpcfb.RangeT, error)
	// SealRange seals the range and returns it.
	// It returns model.ErrStreamNotFound if the steam does not exist.
	// It returns model.ErrRangeNotFound if the range does not exist.
	// It returns model.ErrSealRangeTwice and the range if the range is already sealed with the same end offset.
	// It returns model.ErrRangeAlreadySealed if the range is already sealed.
	// It returns model.ErrInvalidRangeEnd if the end offset is invalid.
	// It returns model.ErrInvalidStreamEpoch if the stream epoch mismatches.
	SealRange(ctx context.Context, p *model.SealRangeParam) (*rpcfb.RangeT, error)
	// GetRange returns the range by the range ID.
	GetRange(ctx context.Context, rangeID *RangeID) (*rpcfb.RangeT, error)
	// GetRanges returns ranges by range IDs.
	GetRanges(ctx context.Context, rangeIDs []*RangeID) ([]*rpcfb.RangeT, error)
	// GetLastRange returns the last range of the given stream.
	// It returns nil and no error if there is no range in the stream.
	GetLastRange(ctx context.Context, streamID int64) (*rpcfb.RangeT, error)
	// GetRangesByStream returns the ranges of the given stream.
	GetRangesByStream(ctx context.Context, streamID int64) ([]*rpcfb.RangeT, error)
	// ForEachRangeInStream calls the given function f for each range in the stream.
	// If f returns an error, the iteration is stopped and the error is returned.
	ForEachRangeInStream(ctx context.Context, streamID int64, f func(r *rpcfb.RangeT) error) error
	// GetRangeIDsByRangeServer returns the range IDs on the range server.
	GetRangeIDsByRangeServer(ctx context.Context, rangeServerID int32) ([]*RangeID, error)
	// ForEachRangeIDOnRangeServer calls the given function f for each range on the range server.
	// If f returns an error, the iteration is stopped and the error is returned.
	ForEachRangeIDOnRangeServer(ctx context.Context, rangeServerID int32, f func(rangeID *RangeID) error) error
	// GetRangeIDsByRangeServerAndStream returns the range IDs on the range server and the stream.
	GetRangeIDsByRangeServerAndStream(ctx context.Context, streamID int64, rangeServerID int32) ([]*RangeID, error)
}

func (e *Endpoint) CreateRange(ctx context.Context, p *model.CreateRangeParam, f ChooseServersFunc) (*rpcfb.RangeT, error) {
	logger := e.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	var newRange *rpcfb.RangeT
	err := e.KV.ExecInTxn(ctx, func(kv kv.BasicKV) error {
		// get and check the stream
		sk := streamPath(p.StreamID)
		sv, err := kv.Get(ctx, sk)
		if err != nil {
			logger.Error("failed to get stream", zap.Error(err))
			return errors.Wrapf(err, "get stream %d", p.StreamID)
		}
		if sv == nil {
			logger.Error("stream not found")
			return errors.Wrapf(model.ErrStreamNotFound, "stream %d", p.StreamID)
		}
		s := rpcfb.GetRootAsStream(sv, 0).UnPack()
		if s.Epoch != p.Epoch {
			logger.Error("invalid epoch", zap.Int64("stream-epoch", s.Epoch))
			return errors.Wrapf(model.ErrInvalidStreamEpoch, "range %d-%d epoch %d != %d", p.StreamID, p.Index, p.Epoch, s.Epoch)
		}

		// make sure the range does not exist
		rk := rangePathInSteam(p.StreamID, p.Index)
		rv, err := kv.Get(ctx, rk)
		if err != nil {
			logger.Error("failed to get range", zap.Error(err))
			return errors.Wrapf(err, "get range %d-%d", p.StreamID, p.Index)
		}
		if rv != nil {
			r := rpcfb.GetRootAsRange(rv, 0).UnPack()
			if r.Epoch == p.Epoch && r.Start == p.Start {
				logger.Info("create range twice")
				newRange = r
				return errors.Wrapf(model.ErrCreateRangeTwice, "range %d-%d", p.StreamID, p.Index)
			}
			// try to create the same range with different parameters
			logger.Error("range already exists")
			return errors.Wrapf(model.ErrRangeAlreadyExist, "range %d-%d", p.StreamID, p.Index)
		}

		// check the previous range
		var pr *rpcfb.RangeT
		if p.Index == model.MinRangeIndex {
			// create the first range, mock the previous range
			pr = &rpcfb.RangeT{
				StreamId: p.StreamID,
				Index:    model.MinRangeIndex - 1,
				End:      0,
			}
		} else {
			prk := rangePathInSteam(p.StreamID, p.Index-1)
			prv, err := kv.Get(ctx, prk)
			if err != nil {
				logger.Error("failed to get previous range", zap.Error(err))
				return errors.Wrapf(err, "get previous range %d-%d", p.StreamID, p.Index-1)
			}
			if prv == nil {
				logger.Error("previous range not found")
				return errors.Wrapf(model.ErrInvalidRangeIndex, "previous range %d-%d not found", p.StreamID, p.Index-1)
			}
			pr = rpcfb.GetRootAsRange(prv, 0).UnPack()
		}
		if pr.End == _writableRangeEnd {
			logger.Error("create range before sealing the previous range")
			return errors.Wrapf(model.ErrCreateRangeBeforeSeal, "create range %d-%d before sealing the previous range %d-%d", p.StreamID, p.Index, p.StreamID, p.Index-1)
		}
		if p.Start != pr.End {
			logger.Error("invalid range start", zap.Int64("previous-end", pr.End))
			return errors.Wrapf(model.ErrInvalidRangeStart, "range %d-%d start %d != %d", p.StreamID, p.Index, p.Start, pr.End)
		}

		// all check passed, create the range
		servers, err := f(s.Replica, pr.Servers)
		if err != nil {
			logger.Error("failed to choose servers", zap.Error(err))
			return errors.Wrapf(err, "choose servers for range %d-%d", p.StreamID, p.Index)
		}
		newRange = &rpcfb.RangeT{
			StreamId:     p.StreamID,
			Epoch:        p.Epoch,
			Index:        p.Index,
			Start:        p.Start,
			End:          _writableRangeEnd,
			Servers:      servers,
			ReplicaCount: s.Replica,
			AckCount:     s.AckCount,
			// TODO: choose offload owner by some strategy.
			OffloadOwner: &rpcfb.OffloadOwnerT{ServerId: servers[0].ServerId},
		}
		rangeInfo := fbutil.Marshal(newRange)
		_, _ = kv.Put(ctx, rk, rangeInfo, false)
		for _, server := range newRange.Servers {
			_, _ = kv.Put(ctx, rangePathOnRangeServer(server.ServerId, newRange.StreamId, newRange.Index), nil, false)
		}
		mcache.Free(rangeInfo)

		return nil
	})

	if err != nil {
		err := errors.Wrapf(err, "create range %d-%d", p.StreamID, p.Index)
		if errors.Is(err, model.ErrCreateRangeTwice) {
			return newRange, err
		}
		return nil, err
	}

	return newRange, nil
}

func (e *Endpoint) SealRange(ctx context.Context, p *model.SealRangeParam) (*rpcfb.RangeT, error) {
	logger := e.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	var sealedRange *rpcfb.RangeT
	err := e.KV.ExecInTxn(ctx, func(kv kv.BasicKV) error {
		// get and check the stream
		sk := streamPath(p.StreamID)
		sv, err := kv.Get(ctx, sk)
		if err != nil {
			logger.Error("failed to get stream", zap.Error(err))
			return errors.Wrapf(err, "get stream %d", p.StreamID)
		}
		if sv == nil {
			logger.Error("stream not found")
			return errors.Wrapf(model.ErrStreamNotFound, "stream %d", p.StreamID)
		}
		s := rpcfb.GetRootAsStream(sv, 0).UnPack()
		if s.Epoch != p.Epoch {
			logger.Error("invalid epoch", zap.Int64("stream-epoch", s.Epoch))
			return errors.Wrapf(model.ErrInvalidStreamEpoch, "range %d-%d epoch %d != %d", p.StreamID, p.Index, p.Epoch, s.Epoch)
		}

		// get and check the range
		rk := rangePathInSteam(p.StreamID, p.Index)
		rv, err := kv.Get(ctx, rk)
		if err != nil {
			logger.Error("failed to get range", zap.Error(err))
			return errors.Wrapf(err, "get range %d-%d", p.StreamID, p.Index)
		}
		if rv == nil {
			logger.Error("range not found")
			return errors.Wrapf(model.ErrRangeNotFound, "range %d-%d", p.StreamID, p.Index)
		}
		r := rpcfb.GetRootAsRange(rv, 0).UnPack()
		if r.End != _writableRangeEnd {
			if r.End == p.End {
				logger.Info("seal range twice")
				sealedRange = r
				return errors.Wrapf(model.ErrSealRangeTwice, "range %d-%d", p.StreamID, p.Index)
			}
			// try to seal a sealed range with different end offset
			logger.Error("range already sealed", zap.Int64("end", r.End))
			return errors.Wrapf(model.ErrRangeAlreadySealed, "range %d-%d", p.StreamID, p.Index)
		}
		if p.End < r.Start {
			logger.Error("invalid end offset", zap.Int64("start", r.Start))
			return errors.Wrapf(model.ErrInvalidRangeEnd, "range %d-%d end %d < start %d", p.StreamID, p.Index, p.End, r.Start)
		}

		// all check passed, seal the range
		r.End = p.End
		sealedRange = r
		rangeInfo := fbutil.Marshal(sealedRange)
		_, _ = kv.Put(ctx, rk, rangeInfo, false)
		mcache.Free(rangeInfo)

		return nil
	})
	if err != nil {
		err := errors.Wrapf(err, "seal range %d-%d", p.StreamID, p.Index)
		if errors.Is(err, model.ErrSealRangeTwice) {
			return sealedRange, err
		}
		return nil, err
	}

	return sealedRange, nil
}

func (e *Endpoint) GetRange(ctx context.Context, rangeID *RangeID) (*rpcfb.RangeT, error) {
	ranges, err := e.GetRanges(ctx, []*RangeID{rangeID})
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return nil, nil
	}

	return ranges[0], nil
}

func (e *Endpoint) GetRanges(ctx context.Context, rangeIDs []*RangeID) ([]*rpcfb.RangeT, error) {
	logger := e.lg.With(traceutil.TraceLogField(ctx))

	keys := make([][]byte, len(rangeIDs))
	for i, rangeID := range rangeIDs {
		keys[i] = rangePathInSteam(rangeID.StreamID, rangeID.Index)
	}

	kvs, err := e.KV.BatchGet(ctx, keys, false)
	if err != nil {
		logger.Error("failed to get ranges", zap.Error(err))
		return nil, errors.Wrap(err, "get ranges")
	}

	ranges := make([]*rpcfb.RangeT, 0, len(kvs))
	for _, rangeKV := range kvs {
		if rangeKV.Value == nil {
			continue
		}
		ranges = append(ranges, rpcfb.GetRootAsRange(rangeKV.Value, 0).UnPack())
	}

	if len(ranges) < len(rangeIDs) {
		logger.Warn("some ranges not found", zap.Int("expected-count", len(rangeIDs)), zap.Int("got-count", len(ranges)),
			zap.Any("expected", rangeIDs), zap.Any("got", rangeIDsFromPathsInStream(kvs)))
	}

	return ranges, nil
}

func (e *Endpoint) GetLastRange(ctx context.Context, streamID int64) (*rpcfb.RangeT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	kvs, _, _, err := e.KV.GetByRange(ctx, kv.Range{StartKey: rangePathInSteam(streamID, model.MinRangeIndex), EndKey: e.endRangePathInStream(streamID)}, 0, 1, true)
	if err != nil {
		logger.Error("failed to get last range", zap.Error(err))
		return nil, err
	}
	if len(kvs) < 1 {
		logger.Info("no range in stream")
		return nil, nil
	}

	return rpcfb.GetRootAsRange(kvs[0].Value, 0).UnPack(), nil
}

func (e *Endpoint) GetRangesByStream(ctx context.Context, streamID int64) ([]*rpcfb.RangeT, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	// TODO set capacity
	ranges := make([]*rpcfb.RangeT, 0)
	err := e.ForEachRangeInStream(ctx, streamID, func(r *rpcfb.RangeT) error {
		ranges = append(ranges, r)
		return nil
	})
	if err != nil {
		logger.Error("failed to get ranges", zap.Error(err))
		return nil, errors.Wrapf(err, "get ranges in stream %d", streamID)
	}

	return ranges, nil
}

func (e *Endpoint) ForEachRangeInStream(ctx context.Context, streamID int64, f func(r *rpcfb.RangeT) error) error {
	var startID = model.MinRangeIndex
	for startID >= model.MinRangeIndex {
		nextID, err := e.forEachRangeInStreamLimited(ctx, streamID, f, startID, _rangeByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachRangeInStreamLimited(ctx context.Context, streamID int64, f func(r *rpcfb.RangeT) error, startID int32, limit int64) (nextID int32, err error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	startKey := rangePathInSteam(streamID, startID)
	kvs, _, more, err := e.KV.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endRangePathInStream(streamID)}, 0, limit, false)
	if err != nil {
		logger.Error("failed to get ranges", zap.Int32("start-id", startID), zap.Int64("limit", limit), zap.Error(err))
		return model.MinRangeIndex - 1, errors.Wrap(err, "get ranges")
	}

	for _, rangeKV := range kvs {
		r := rpcfb.GetRootAsRange(rangeKV.Value, 0).UnPack()
		nextID = r.Index + 1
		err = f(r)
		if err != nil {
			return model.MinRangeIndex - 1, err
		}
	}

	if !more {
		// no more ranges
		nextID = model.MinRangeIndex - 1
	}
	return
}

func (e *Endpoint) endRangePathInStream(streamID int64) []byte {
	return e.KV.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeStreamPrefixFormat, streamID)))
}

func rangePathInSteam(streamID int64, index int32) []byte {
	res := make([]byte, 0, _rangeStreamKeyLen)
	res = fmt.Appendf(res, _rangeStreamFormat, streamID, index)
	return res
}

func rangeIDsFromPathsInStream(kvs []kv.KeyValue) []*RangeID {
	rangeIDs := make([]*RangeID, 0, len(kvs))
	for _, rangeKV := range kvs {
		streamID, index, err := rangeIDFromPathInStream(rangeKV.Key)
		if err != nil {
			continue
		}
		rangeIDs = append(rangeIDs, &RangeID{StreamID: streamID, Index: index})
	}
	return rangeIDs
}

func rangeIDFromPathInStream(path []byte) (streamID int64, index int32, err error) {
	_, err = fmt.Sscanf(string(path), _rangeStreamFormat, &streamID, &index)
	if err != nil {
		err = errors.Wrapf(err, "invalid range path %s", string(path))
	}
	return
}

func (e *Endpoint) GetRangeIDsByRangeServer(ctx context.Context, rangeServerID int32) ([]*RangeID, error) {
	logger := e.lg.With(zap.Int32("range-server-id", rangeServerID), traceutil.TraceLogField(ctx))

	// TODO set capacity
	rangeIDs := make([]*RangeID, 0)
	err := e.ForEachRangeIDOnRangeServer(ctx, rangeServerID, func(rangeID *RangeID) error {
		rangeIDs = append(rangeIDs, rangeID)
		return nil
	})
	if err != nil {
		logger.Error("failed to get range ids by range server", zap.Error(err))
		return nil, errors.Wrapf(err, "get range ids by range server %d", rangeServerID)
	}

	return rangeIDs, nil
}

func (e *Endpoint) ForEachRangeIDOnRangeServer(ctx context.Context, rangeServerID int32, f func(rangeID *RangeID) error) error {
	startID := &RangeID{StreamID: model.MinStreamID, Index: model.MinRangeIndex}
	for startID != nil && startID.StreamID >= model.MinStreamID && startID.Index >= model.MinRangeIndex {
		nextID, err := e.forEachRangeIDOnRangeServerLimited(ctx, rangeServerID, f, startID, _rangeByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachRangeIDOnRangeServerLimited(ctx context.Context, rangeServerID int32, f func(rangeID *RangeID) error, startID *RangeID, limit int64) (nextID *RangeID, err error) {
	logger := e.lg.With(zap.Int32("range-server-id", rangeServerID), traceutil.TraceLogField(ctx))

	startKey := rangePathOnRangeServer(rangeServerID, startID.StreamID, startID.Index)
	kvs, _, more, err := e.KV.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endRangePathOnRangeServer(rangeServerID)}, 0, limit, false)
	if err != nil {
		logger.Error("failed to get range ids by range server", zap.Int64("start-stream-id", startID.StreamID), zap.Int32("start-range-index", startID.Index), zap.Int64("limit", limit), zap.Error(err))
		return nil, errors.Wrap(err, "get range ids by range server")
	}

	nextID = startID
	for _, rangeKV := range kvs {
		_, streamID, index, err := rangeIDFromPathOnRangeServer(rangeKV.Key)
		if err != nil {
			return nil, err
		}
		nextID.StreamID = streamID
		nextID.Index = index + 1
		err = f(&RangeID{StreamID: streamID, Index: index})
		if err != nil {
			return nil, err
		}
	}

	// return nil if no more ranges
	if !more {
		nextID = nil
	}
	return
}

func (e *Endpoint) endRangePathOnRangeServer(rangeServerID int32) []byte {
	return e.KV.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeOnServerPrefixFormat, rangeServerID)))
}

func (e *Endpoint) GetRangeIDsByRangeServerAndStream(ctx context.Context, streamID int64, rangeServerID int32) ([]*RangeID, error) {
	logger := e.lg.With(zap.Int64("stream-id", streamID), zap.Int32("range-server-id", rangeServerID), traceutil.TraceLogField(ctx))

	startKey := rangePathOnRangeServer(rangeServerID, streamID, model.MinRangeIndex)
	kvs, _, _, err := e.KV.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endRangePathOnRangeServerInStream(rangeServerID, streamID)}, 0, 0, false)
	if err != nil {
		logger.Error("failed to get range ids by range server and stream", zap.Error(err))
		return nil, errors.Wrapf(err, "get range ids by range server %d and stream %d", rangeServerID, streamID)
	}

	rangeIDs := make([]*RangeID, 0, len(kvs))
	for _, rangeKV := range kvs {
		_, _, index, err := rangeIDFromPathOnRangeServer(rangeKV.Key)
		if err != nil {
			return nil, err
		}
		rangeIDs = append(rangeIDs, &RangeID{StreamID: streamID, Index: index})
	}

	return rangeIDs, nil
}

func (e *Endpoint) endRangePathOnRangeServerInStream(rangeServerID int32, streamID int64) []byte {
	return e.KV.GetPrefixRangeEnd([]byte(fmt.Sprintf(_rangeOnServerStreamPrefixFormat, rangeServerID, streamID)))
}

func rangePathOnRangeServer(rangeServerID int32, streamID int64, index int32) []byte {
	res := make([]byte, 0, _rangeOnServerKeyLen)
	res = fmt.Appendf(res, _rangeOnServerFormat, rangeServerID, streamID, index)
	return res
}

func rangeIDFromPathOnRangeServer(path []byte) (rangeServerID int32, streamID int64, index int32, err error) {
	_, err = fmt.Sscanf(string(path), _rangeOnServerFormat, &rangeServerID, &streamID, &index)
	if err != nil {
		err = errors.Wrapf(err, "parse range path: %s", string(path))
	}
	return
}

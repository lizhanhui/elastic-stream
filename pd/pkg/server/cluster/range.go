package cluster

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/model"
	traceutil "github.com/AutoMQ/pd/pkg/util/trace"
)

type RangeService interface {
	// ListRange lists ranges of
	// 1. a stream
	// 2. a range server
	// 3. a range server and a stream
	// It returns model.ErrPDNotLeader if the current PD node is not the leader.
	ListRange(ctx context.Context, criteria *rpcfb.ListRangeCriteriaT) (ranges []*rpcfb.RangeT, err error)
	// SealRange seals a range. It returns the sealed range if success.
	// It returns model.ErrPDNotLeader if the current PD node is not the leader.
	// It returns model.ErrStreamNotFound if the steam does not exist.
	// It returns model.ErrRangeNotFound if the range does not exist.
	// It returns model.ErrSealRangeTwice and the range if the range is already sealed with the same end offset.
	// It returns model.ErrRangeAlreadySealed if the range is already sealed.
	// It returns model.ErrInvalidRangeEnd if the end offset is invalid.
	// It returns model.ErrInvalidStreamEpoch if the stream epoch mismatches.
	SealRange(ctx context.Context, p *model.SealRangeParam) (*rpcfb.RangeT, error)
	// CreateRange creates a range. It returns the created range if success.
	// It returns model.ErrPDNotLeader if the current PD node is not the leader.
	// It returns model.ErrStreamNotFound if the stream does not exist.
	// It returns model.ErrInvalidStreamEpoch if the stream epoch mismatches.
	// It returns model.ErrCreateRangeTwice and the range if the range is already created with the same parameters.
	// It returns model.ErrRangeAlreadyExist if the range is already created.
	// It returns model.ErrInvalidRangeIndex if the range index is invalid.
	// It returns model.ErrCreateRangeBeforeSeal if the last range is not sealed.
	// It returns model.ErrInvalidRangeStart if the start offset is invalid.
	// It returns model.ErrNotEnoughRangeServers if there are not enough range servers to allocate.
	CreateRange(ctx context.Context, p *model.CreateRangeParam) (*rpcfb.RangeT, error)
}

func (c *RaftCluster) ListRange(ctx context.Context, criteria *rpcfb.ListRangeCriteriaT) (ranges []*rpcfb.RangeT, err error) {
	byStream := criteria.StreamId >= model.MinStreamID
	byRangeServer := criteria.ServerId >= model.MinRangeServerID
	switch {
	case byStream && byRangeServer:
		ranges, err = c.listRangeOnRangeServerInStream(ctx, criteria.StreamId, criteria.ServerId)
	case byStream && !byRangeServer:
		ranges, err = c.listRangeInStream(ctx, criteria.StreamId)
	case !byStream && byRangeServer:
		ranges, err = c.listRangeOnRangeServer(ctx, criteria.ServerId)
	default:
		// do not support list all ranges
		ranges = make([]*rpcfb.RangeT, 0)
	}
	if errors.Is(err, model.ErrKVTxnFailed) {
		err = model.ErrPDNotLeader
	}

	for _, r := range ranges {
		c.fillRangeServersInfo(r)
	}
	return
}

// listRangeOnRangeServerInStream lists ranges on a range server in a stream.
func (c *RaftCluster) listRangeOnRangeServerInStream(ctx context.Context, streamID int64, rangeServerID int32) ([]*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int64("stream-id", streamID), zap.Int32("range-server-id", rangeServerID), traceutil.TraceLogField(ctx))

	logger.Debug("start to list ranges on range server in stream")
	select {
	case <-c.rangeLoadedNotify():
	case <-ctx.Done():
		logger.Warn("context is done when waiting for range loaded")
		return nil, ctx.Err()
	}
	rangeIDs := make([]model.RangeID, 0)
	for id := range c.cache.RangesOnServer(rangeServerID).Iter() {
		if id.StreamID == streamID {
			rangeIDs = append(rangeIDs, id)
		}
	}

	ranges, err := c.storage.GetRanges(ctx, rangeIDs)
	logger.Debug("finish listing ranges on range server in stream", zap.Int("range-cnt", len(ranges)))
	if err != nil {
		return nil, err
	}

	return ranges, nil
}

// listRangeInStream lists ranges in a stream.
func (c *RaftCluster) listRangeInStream(ctx context.Context, streamID int64) ([]*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	logger.Debug("start to list ranges in stream")
	ranges, err := c.storage.GetRangesByStream(ctx, streamID)
	logger.Debug("finish listing ranges in stream", zap.Int("range-cnt", len(ranges)), zap.Error(err))

	return ranges, err
}

// listRangeOnRangeServer lists ranges on a range server.
func (c *RaftCluster) listRangeOnRangeServer(ctx context.Context, rangeServerID int32) ([]*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int32("range-server-id", rangeServerID), traceutil.TraceLogField(ctx))

	logger.Debug("start to list ranges on range server")
	select {
	case <-c.rangeLoadedNotify():
	case <-ctx.Done():
		logger.Warn("context is done when waiting for range loaded")
		return nil, ctx.Err()
	}
	rangeIDs := c.cache.RangesOnServer(rangeServerID)
	ranges, err := c.storage.GetRanges(ctx, rangeIDs.ToSlice())
	logger.Debug("finish listing ranges on range server", zap.Int("range-cnt", len(ranges)))
	if err != nil {
		return nil, err
	}

	return ranges, nil
}

func (c *RaftCluster) SealRange(ctx context.Context, p *model.SealRangeParam) (*rpcfb.RangeT, error) {
	logger := c.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	logger.Info("start to seal range")
	sealedRange, err := c.storage.SealRange(ctx, p)
	logger.Info("finish sealing range", zap.Error(err))
	if errors.Is(err, model.ErrKVTxnFailed) {
		err = model.ErrPDNotLeader
	}
	c.fillRangeServersInfo(sealedRange)
	return sealedRange, err
}

func (c *RaftCluster) CreateRange(ctx context.Context, p *model.CreateRangeParam) (*rpcfb.RangeT, error) {
	logger := c.lg.With(p.Fields()...).With(traceutil.TraceLogField(ctx))

	var chooseServers = func(replica int8, blackList []*rpcfb.RangeServerT) ([]*rpcfb.RangeServerT, error) {
		blackServerIDs := mapset.NewThreadUnsafeSetWithSize[int32](len(blackList))
		for _, rs := range blackList {
			blackServerIDs.Add(rs.ServerId)
		}
		servers, err := c.chooseRangeServers(int(replica), blackServerIDs, logger)
		if err != nil {
			return nil, err
		}
		ids := make([]int32, 0, len(servers))
		for _, rs := range servers {
			ids = append(ids, rs.ServerId)
		}
		logger = logger.With(zap.Int32s("server-ids", ids))
		return servers, nil
	}

	logger.Info("start to create range")
	newRange, err := c.storage.CreateRange(ctx, p, chooseServers)
	logger.Info("finish creating range", zap.Error(err))
	if errors.Is(err, model.ErrKVTxnFailed) {
		err = model.ErrPDNotLeader
	}
	c.fillRangeServersInfo(newRange)
	return newRange, err
}

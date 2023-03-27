package cluster

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/endpoint"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

var (
	// ErrRangeNotFound is returned when the specified range is not found.
	ErrRangeNotFound = errors.New("range not found")
)

// ListRanges lists the ranges of
// 1. a stream
// 2. a data node
// 3. a data node and a stream
func (c *RaftCluster) ListRanges(ctx context.Context, rangeCriteria *rpcfb.RangeCriteriaT) ([]*rpcfb.RangeT, error) {
	if rangeCriteria.StreamId >= endpoint.MinStreamID && rangeCriteria.DataNode != nil && rangeCriteria.DataNode.NodeId >= endpoint.MinDataNodeID {
		return c.listRangesOnDataNodeInStream(ctx, rangeCriteria.StreamId, rangeCriteria.DataNode.NodeId)
	}
	if rangeCriteria.StreamId >= endpoint.MinStreamID {
		return c.listRangesInStream(ctx, rangeCriteria.StreamId)
	}
	if rangeCriteria.DataNode != nil && rangeCriteria.DataNode.NodeId >= endpoint.MinDataNodeID {
		return c.listRangesOnDataNode(ctx, rangeCriteria.DataNode.NodeId)
	}
	return nil, nil
}

// listRangesOnDataNodeInStream lists the ranges on a data node in a stream.
func (c *RaftCluster) listRangesOnDataNodeInStream(ctx context.Context, streamID int64, dataNodeID int32) ([]*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int64("stream-id", streamID), zap.Int32("data-node-id", dataNodeID), traceutil.TraceLogField(ctx))

	logger.Info("start to list ranges on data node in stream")
	rangeIDs, err := c.storage.GetRangeIDsByDataNodeAndStream(ctx, streamID, dataNodeID)
	if err != nil {
		return nil, err
	}

	ranges, err := c.getRanges(ctx, rangeIDs, logger)
	if err != nil {
		return nil, err
	}

	logger.Info("finish listing ranges on data node in stream", zap.Int("range-cnt", len(ranges)))
	return ranges, nil
}

// listRangesInStream lists the ranges of a stream.
func (c *RaftCluster) listRangesInStream(ctx context.Context, streamID int64) ([]*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	logger.Info("start to list ranges in stream")
	ranges, err := c.storage.GetRangesByStream(ctx, streamID)
	logger.Info("finish listing ranges in stream", zap.Int("range-cnt", len(ranges)), zap.Error(err))

	return ranges, err
}

// listRangesOnDataNode lists the ranges on a data node.
func (c *RaftCluster) listRangesOnDataNode(ctx context.Context, dataNodeID int32) ([]*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int32("data-node-id", dataNodeID), traceutil.TraceLogField(ctx))

	logger.Info("start to list ranges on data node")
	rangeIDs, err := c.storage.GetRangeIDsByDataNode(ctx, dataNodeID)
	if err != nil {
		return nil, err
	}

	ranges, err := c.getRanges(ctx, rangeIDs, logger)
	if err != nil {
		return nil, err
	}

	logger.Info("finish listing ranges on data node", zap.Int("range-cnt", len(ranges)))
	return ranges, nil
}

// SealRange seals a range.
// It returns the current writable range and an optional error.
// It returns a nil range if and only if the ctx is done or the stream does not exist.
// It returns ErrRangeNotFound if the range does not exist.
func (c *RaftCluster) SealRange(ctx context.Context, rangeID *rpcfb.RangeIdT) (*rpcfb.RangeT, error) {
	logger := c.lg.With(zap.Int64("range-stream-id", rangeID.StreamId), zap.Int32("range-index", rangeID.RangeIndex), traceutil.TraceLogField(ctx))

	writableRange := c.cache.WritableRange(rangeID.StreamId)
	mu := writableRange.Mu()

	select {
	case mu <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() {
		<-mu
	}()

	if writableRange.RangeT != nil && writableRange.RangeIndex > rangeID.RangeIndex {
		return writableRange.RangeT, nil
	}

	lastRange, err := c.storage.GetLastRange(ctx, rangeID.StreamId)
	if lastRange == nil || err != nil {
		// The stream does not exist.
		logger.Error("failed to get last range", zap.Error(err))
		return nil, ErrRangeNotFound
	}
	writableRange.RangeT = lastRange

	if writableRange.RangeIndex > rangeID.RangeIndex {
		// The range is already sealed.
		return writableRange.RangeT, nil
	}
	if writableRange.RangeIndex < rangeID.RangeIndex {
		// The range is not found.
		return writableRange.RangeT, ErrRangeNotFound
	}

	// Here, writableRange.RangeIndex == rangeID.RangeIndex.
	// TODO Query range offset from the data nodes.
	// TODO Seal the range.

	return nil, nil
}

func (c *RaftCluster) getRanges(ctx context.Context, rangeIDs []*rpcfb.RangeIdT, logger *zap.Logger) ([]*rpcfb.RangeT, error) {
	ranges := make([]*rpcfb.RangeT, 0, len(rangeIDs))
	for _, rangeID := range rangeIDs {
		r, err := c.storage.GetRange(ctx, rangeID)
		if err != nil {
			return nil, err
		}
		if r == nil {
			logger.Warn("range not found", zap.Int64("range-stream-id", rangeID.StreamId), zap.Int32("range-index", rangeID.RangeIndex))
			continue
		}
		ranges = append(ranges, r)
	}
	return ranges, nil
}

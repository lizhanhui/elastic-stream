package cluster

import (
	"context"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/endpoint"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
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

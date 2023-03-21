package cluster

import (
	"context"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/endpoint"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

// ListRanges lists the ranges of a stream or a data node.
func (c *RaftCluster) ListRanges(ctx context.Context, rangeOwner *rpcfb.RangeCriteriaT) ([]*rpcfb.RangeT, error) {
	if rangeOwner.StreamId >= endpoint.MinStreamID {
		return c.listRangesInStream(ctx, rangeOwner.StreamId)
	}
	if rangeOwner.DataNode != nil && rangeOwner.DataNode.NodeId >= endpoint.MinDataNodeID {
		return c.listRangesOnDataNode(ctx, rangeOwner.DataNode.NodeId)
	}
	return nil, nil
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
	logger.Info("finish getting range ids on data node", zap.Int("range-ids-length", len(rangeIDs)))

	ranges := make([]*rpcfb.RangeT, 0, len(rangeIDs))
	for _, rangeID := range rangeIDs {
		r, err := c.storage.GetRange(ctx, rangeID)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}

	logger.Info("finish listing ranges on data node", zap.Int("range-cnt", len(ranges)))
	return ranges, nil
}

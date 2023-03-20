package cluster

import (
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/endpoint"
)

// ListRanges lists the ranges of a stream or a data node.
func (c *RaftCluster) ListRanges(rangeOwner *rpcfb.RangeCriteriaT) ([]*rpcfb.RangeT, error) {
	if rangeOwner.StreamId >= endpoint.MinStreamID {
		return c.listRangesInStream(rangeOwner.StreamId)
	}
	if rangeOwner.DataNode != nil && rangeOwner.DataNode.NodeId >= endpoint.MinDataNodeID {
		return c.listRangesOnDataNode(rangeOwner.DataNode.NodeId)
	}
	return nil, nil
}

// listRangesInStream lists the ranges of a stream.
func (c *RaftCluster) listRangesInStream(streamID int64) ([]*rpcfb.RangeT, error) {
	c.lg.Info("start to list ranges in stream", zap.Int64("stream-id", streamID))
	ranges, err := c.storage.GetRangesByStream(streamID)
	c.lg.Info("finish listing ranges in stream", zap.Int64("stream-id", streamID), zap.Int("length", len(ranges)), zap.Error(err))
	return ranges, err
}

// listRangesOnDataNode lists the ranges on a data node.
func (c *RaftCluster) listRangesOnDataNode(dataNodeID int32) ([]*rpcfb.RangeT, error) {
	c.lg.Info("start to list ranges on data node", zap.Int32("data-node-id", dataNodeID))

	rangeIDs, err := c.storage.GetRangeIDsByDataNode(dataNodeID)
	if err != nil {
		return nil, err
	}

	ranges := make([]*rpcfb.RangeT, 0, len(rangeIDs))
	for _, rangeID := range rangeIDs {
		r, err := c.storage.GetRange(rangeID)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, r)
	}

	c.lg.Info("finish listing ranges on data node", zap.Int32("data-node-id", dataNodeID), zap.Int("length", len(ranges)))
	return ranges, nil
}

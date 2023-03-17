package cluster

import (
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
	return c.storage.GetRangesByStream(streamID)
}

// listRangesOnDataNode lists the ranges on a data node.
func (c *RaftCluster) listRangesOnDataNode(dataNodeID int32) ([]*rpcfb.RangeT, error) {
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

	return ranges, nil
}

package cluster

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// ListRanges lists the ranges of a stream or a data node.
func (c *RaftCluster) ListRanges(rangeOwner *rpcfb.RangeCriteriaT) ([]*rpcfb.RangeT, error) {
	if rangeOwner.StreamId > 0 {
		return c.listRangesInStream(rangeOwner.StreamId)
	}
	if rangeOwner.DataNode.NodeId > 0 {
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
	// TODO
	// return c.storage.GetRangesByDataNode(dataNodeID)
	_ = dataNodeID
	return nil, nil
}

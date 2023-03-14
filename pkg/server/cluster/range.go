package cluster

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// ListRanges lists the ranges of a stream or a data node.
func (c *RaftCluster) ListRanges(rangeOwner *rpcfb.RangeCriteriaT) ([]*rpcfb.RangeT, error) {
	// TODO
	_ = rangeOwner
	return nil, nil
}

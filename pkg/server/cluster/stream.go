package cluster

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// CreateStreams creates streams in transaction.
func (c *RaftCluster) CreateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error) {
	return c.storage.CreateStreams(streams)
}

// DeleteStreams deletes streams in transaction.
func (c *RaftCluster) DeleteStreams(streamIDs []int64) ([]*rpcfb.StreamT, error) {
	return c.storage.DeleteStreams(streamIDs)
}

// UpdateStreams updates streams in transaction.
func (c *RaftCluster) UpdateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error) {
	return c.storage.UpdateStreams(streams)
}

// DescribeStreams describes streams.
func (c *RaftCluster) DescribeStreams(streamIDs []int64) []*rpcfb.DescribeStreamResultT {
	results := make([]*rpcfb.DescribeStreamResultT, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		stream, err := c.storage.GetStream(streamID)
		if err != nil {
			results = append(results, &rpcfb.DescribeStreamResultT{
				Status: &rpcfb.StatusT{
					Code:    rpcfb.ErrorCodeUNKNOWN,
					Message: err.Error(),
				},
			})
			continue
		}
		results = append(results, &rpcfb.DescribeStreamResultT{
			Stream: stream,
		})
	}
	return results
}

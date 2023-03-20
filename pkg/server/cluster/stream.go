package cluster

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/endpoint"
)

// CreateStreams creates streams in transaction.
func (c *RaftCluster) CreateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.CreateStreamResultT, error) {
	params := make([]*endpoint.CreateStreamParam, 0, len(streams))
	for _, stream := range streams {
		stream.StreamId = c.nextStreamID()
		nodes, err := c.chooseDataNodes(stream.ReplicaNums)
		if err != nil {
			return nil, err
		}
		params = append(params, &endpoint.CreateStreamParam{
			StreamT: stream,
			RangeT: &rpcfb.RangeT{
				StreamId:     stream.StreamId,
				RangeIndex:   0,
				StartOffset:  0,
				EndOffset:    -1,
				ReplicaNodes: nodes,
			},
		})
	}

	results, err := c.storage.CreateStreams(params)

	// TODO sync new ranges to data nodes
	return results, err
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
					Code:    rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR,
					Message: err.Error(),
				},
			})
			continue
		}
		results = append(results, &rpcfb.DescribeStreamResultT{
			Stream: stream,
			Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
		})
	}
	return results
}

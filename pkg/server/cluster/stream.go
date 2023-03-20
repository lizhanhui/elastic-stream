package cluster

import (
	"go.uber.org/zap"

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

	streamIDs := make([]int64, 0, len(streams))
	for _, stream := range streams {
		streamIDs = append(streamIDs, stream.StreamId)
	}
	c.lg.Info("start to create streams", zap.Int("length", len(params)), zap.Int64s("stream-ids", streamIDs))
	results, err := c.storage.CreateStreams(params)
	c.lg.Info("finish creating streams", zap.Int("length", len(params)), zap.Error(err))

	// TODO sync new ranges to data nodes
	return results, err
}

// DeleteStreams deletes streams in transaction.
func (c *RaftCluster) DeleteStreams(streamIDs []int64) ([]*rpcfb.StreamT, error) {
	c.lg.Info("start to delete streams", zap.Int("length", len(streamIDs)), zap.Int64s("stream-ids", streamIDs))
	streams, err := c.storage.DeleteStreams(streamIDs)
	c.lg.Info("finish deleting streams", zap.Int("length", len(streamIDs)), zap.Error(err))
	return streams, err
}

// UpdateStreams updates streams in transaction.
func (c *RaftCluster) UpdateStreams(streams []*rpcfb.StreamT) ([]*rpcfb.StreamT, error) {
	streamIDs := make([]int64, 0, len(streams))
	for _, stream := range streams {
		streamIDs = append(streamIDs, stream.StreamId)
	}
	c.lg.Info("start to update streams", zap.Int("length", len(streams)), zap.Int64s("stream-ids", streamIDs))
	upStreams, err := c.storage.UpdateStreams(streams)
	c.lg.Info("finish updating streams", zap.Int("length", len(streams)), zap.Error(err))
	return upStreams, err
}

// DescribeStreams describes streams.
func (c *RaftCluster) DescribeStreams(streamIDs []int64) []*rpcfb.DescribeStreamResultT {
	c.lg.Info("start to describe streams", zap.Int("length", len(streamIDs)), zap.Int64s("stream-ids", streamIDs))

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
	c.lg.Info("finish describing streams", zap.Int("length", len(streamIDs)))
	return results
}

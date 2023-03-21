package handler

import (
	"context"

	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
)

func (s *Sbp) CreateStreams(_ context.Context, req *protocol.CreateStreamsRequest, resp *protocol.CreateStreamsResponse) {
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	results, err := s.c.CreateStreams(req.Streams)
	if err != nil {
		if errors.Is(err, cluster.ErrNotEnoughDataNodes) {
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_NO_AVAILABLE_DN, Message: err.Error()})
			return
		}
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()})
		return
	}

	for _, result := range results {
		result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
	}
	resp.CreateResponses = results
	resp.OK()
}

func (s *Sbp) DeleteStreams(_ context.Context, req *protocol.DeleteStreamsRequest, resp *protocol.DeleteStreamsResponse) {
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	streamIDs := make([]int64, 0, len(req.Streams))
	for _, stream := range req.Streams {
		streamIDs = append(streamIDs, stream.StreamId)
	}
	streams, err := s.c.DeleteStreams(streamIDs)
	if err != nil {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()})
		return
	}

	resp.DeleteResponses = make([]*rpcfb.DeleteStreamResultT, 0, len(streams))
	for _, stream := range streams {
		resp.DeleteResponses = append(resp.DeleteResponses, &rpcfb.DeleteStreamResultT{
			DeletedStream: stream,
			Status:        &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
		})
	}
	resp.OK()
}

func (s *Sbp) UpdateStreams(_ context.Context, req *protocol.UpdateStreamsRequest, resp *protocol.UpdateStreamsResponse) {
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	streams, err := s.c.UpdateStreams(req.Streams)
	if err != nil {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()})
		return
	}

	resp.UpdateResponses = make([]*rpcfb.UpdateStreamResultT, 0, len(streams))
	for _, stream := range streams {
		resp.UpdateResponses = append(resp.UpdateResponses, &rpcfb.UpdateStreamResultT{
			Stream: stream,
			Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
		})
	}
	resp.OK()
}

func (s *Sbp) DescribeStreams(_ context.Context, req *protocol.DescribeStreamsRequest, resp *protocol.DescribeStreamsResponse) {
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	result := s.c.DescribeStreams(req.StreamIds)
	resp.DescribeResponses = result
	resp.OK()
}

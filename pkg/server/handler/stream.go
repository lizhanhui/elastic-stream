package handler

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
)

func (h *Handler) CreateStreams(req *protocol.CreateStreamsRequest, resp *protocol.CreateStreamsResponse) {
	ctx := req.Context()
	if !h.c.IsLeader() {
		h.notLeaderError(ctx, resp)
		return
	}

	results, err := h.c.CreateStreams(ctx, req.Streams)
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

func (h *Handler) DeleteStreams(req *protocol.DeleteStreamsRequest, resp *protocol.DeleteStreamsResponse) {
	ctx := req.Context()
	if !h.c.IsLeader() {
		h.notLeaderError(ctx, resp)
		return
	}

	streamIDs := make([]int64, 0, len(req.Streams))
	for _, stream := range req.Streams {
		streamIDs = append(streamIDs, stream.StreamId)
	}
	streams, err := h.c.DeleteStreams(ctx, streamIDs)
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

func (h *Handler) UpdateStreams(req *protocol.UpdateStreamsRequest, resp *protocol.UpdateStreamsResponse) {
	ctx := req.Context()
	if !h.c.IsLeader() {
		h.notLeaderError(ctx, resp)
		return
	}

	streams, err := h.c.UpdateStreams(ctx, req.Streams)
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

func (h *Handler) DescribeStreams(req *protocol.DescribeStreamsRequest, resp *protocol.DescribeStreamsResponse) {
	ctx := req.Context()
	if !h.c.IsLeader() {
		h.notLeaderError(ctx, resp)
		return
	}

	result := h.c.DescribeStreams(ctx, req.StreamIds)
	resp.DescribeResponses = result
	resp.OK()
}

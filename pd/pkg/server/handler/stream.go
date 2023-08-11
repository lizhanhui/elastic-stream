package handler

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	"github.com/AutoMQ/pd/pkg/server/cluster"
	"github.com/AutoMQ/pd/pkg/server/model"
)

func (h *Handler) CreateStream(req *protocol.CreateStreamRequest, resp *protocol.CreateStreamResponse) {
	ctx := req.Context()

	param, err := model.NewCreateStreamParam(req.Stream)
	if err != nil {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: err.Error()})
		return
	}

	stream, err := h.c.CreateStream(ctx, param)
	if err != nil {
		switch {
		case errors.Is(err, cluster.ErrNotLeader):
			resp.Error(h.notLeaderError(ctx))
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.Stream = stream
	resp.OK()
}

func (h *Handler) DeleteStream(req *protocol.DeleteStreamRequest, resp *protocol.DeleteStreamResponse) {
	ctx := req.Context()

	if req.StreamId < model.MinStreamID {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: fmt.Sprintf("invalid stream id %d", req.StreamId)})
		return
	}

	stream, err := h.c.DeleteStream(ctx, req.StreamId)
	if err != nil {
		switch {
		case errors.Is(err, cluster.ErrNotLeader):
			resp.Error(h.notLeaderError(ctx))
		case errors.Is(err, cluster.ErrStreamNotFound):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeNOT_FOUND, Message: err.Error()})
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.Stream = stream
	resp.OK()
}

func (h *Handler) UpdateStream(req *protocol.UpdateStreamRequest, resp *protocol.UpdateStreamResponse) {
	ctx := req.Context()

	param, err := model.NewUpdateStreamParam(req.Stream)
	if err != nil {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: err.Error()})
		return
	}

	stream, err := h.c.UpdateStream(ctx, param)
	if err != nil {
		switch {
		case errors.Is(err, cluster.ErrNotLeader):
			resp.Error(h.notLeaderError(ctx))
		case errors.Is(err, cluster.ErrStreamNotFound):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeNOT_FOUND, Message: err.Error()})
		case errors.Is(err, cluster.ErrExpiredStreamEpoch):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH, Message: err.Error()})
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.Stream = stream
	resp.OK()
}

func (h *Handler) DescribeStream(req *protocol.DescribeStreamRequest, resp *protocol.DescribeStreamResponse) {
	ctx := req.Context()

	if req.StreamId < model.MinStreamID {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: fmt.Sprintf("invalid stream id %d", req.StreamId)})
		return
	}

	stream, err := h.c.DescribeStream(ctx, req.StreamId)
	if err != nil {
		switch {
		case errors.Is(err, cluster.ErrNotLeader):
			resp.Error(h.notLeaderError(ctx))
		case errors.Is(err, cluster.ErrStreamNotFound):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeNOT_FOUND, Message: err.Error()})
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.Stream = stream
	resp.OK()
}

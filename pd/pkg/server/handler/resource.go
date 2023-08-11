package handler

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	"github.com/AutoMQ/pd/pkg/server/model"
)

func (h *Handler) ListResource(req *protocol.ListResourceRequest, resp *protocol.ListResourceResponse) {
	ctx := req.Context()

	if len(req.ResourceType) == 0 {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: "resource type is empty"})
		return
	}

	resources, rv, cont, err := h.c.ListResource(ctx, req.ResourceType, req.Limit, req.Continuation)
	if err != nil {
		switch {
		case errors.Is(err, model.ErrPDNotLeader):
			resp.Error(h.notLeaderError(ctx))
		case errors.Is(err, model.ErrResourceVersionCompacted):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_COMPACTED, Message: err.Error()})
		case errors.Is(err, model.ErrInvalidResourceType):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: err.Error()})
		case errors.Is(err, model.ErrInvalidResourceContinuation):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: err.Error()})
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.Resources = resources
	resp.ResourceVersion = rv
	resp.Continuation = cont
	resp.OK()
}

func (h *Handler) WatchResource(req *protocol.WatchResourceRequest, resp *protocol.WatchResourceResponse) {
	ctx := req.Context()

	if len(req.ResourceType) == 0 {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: "resource type is empty"})
		return
	}

	events, rv, err := h.c.WatchResource(ctx, req.ResourceVersion, req.ResourceType)
	if err != nil {
		switch {
		case errors.Is(err, model.ErrResourceVersionCompacted):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_COMPACTED, Message: err.Error()})
		case errors.Is(err, model.ErrInvalidResourceType):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: err.Error()})
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.Events = events
	resp.ResourceVersion = rv
	resp.OK()
}

package handler

import (
	"github.com/pkg/errors"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	"github.com/AutoMQ/pd/pkg/server/cluster"
)

func (h *Handler) CommitObject(req *protocol.CommitObjectRequest, resp *protocol.CommitObjectResponse) {
	ctx := req.Context()

	if req.Object == nil {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeBAD_REQUEST, Message: "object is nil"})
		return
	}

	_, err := h.c.CommitObject(ctx, req.Object)
	if err != nil {
		switch {
		case errors.Is(err, cluster.ErrNotLeader):
			resp.Error(h.notLeaderError(ctx))
		case errors.Is(err, cluster.ErrRangeNotFound):
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeRANGE_NOT_FOUND, Message: err.Error()})
		default:
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: err.Error()})
		}
		return
	}

	resp.OK()
}

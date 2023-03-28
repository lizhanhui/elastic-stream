package handler

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

func (h *Handler) ListRanges(req *protocol.ListRangesRequest, resp *protocol.ListRangesResponse) {
	ctx := req.Context()
	if !h.c.IsLeader() {
		h.notLeaderError(ctx, resp)
		return
	}

	listResponses := make([]*rpcfb.ListRangesResultT, 0, len(req.RangeCriteria))
	for _, owner := range req.RangeCriteria {
		ranges, err := h.c.ListRanges(ctx, owner)

		result := &rpcfb.ListRangesResultT{
			RangeCriteria: owner,
		}
		if err != nil {
			result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()}
		} else {
			result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
			result.Ranges = ranges
		}

		listResponses = append(listResponses, result)
	}
	resp.ListResponses = listResponses
	resp.OK()
}

func (h *Handler) SealRanges(req *protocol.SealRangesRequest, resp *protocol.SealRangesResponse) {
	ctx := req.Context()
	if !h.c.IsLeader() {
		h.notLeaderError(ctx, resp)
		return
	}
	logger := h.lg.With(traceutil.TraceLogField(ctx))

	sealResponses := make([]*rpcfb.SealRangesResultT, 0, len(req.Ranges))
	for _, rangeID := range req.Ranges {
		r, err := h.c.SealRange(ctx, rangeID)

		result := &rpcfb.SealRangesResultT{
			Range: r,
		}
		if err != nil {
			logger.Error("failed to seal range", zap.Int64("stream-id", rangeID.StreamId), zap.Int32("range-index", rangeID.RangeIndex), zap.Error(err))
			switch {
			case errors.Is(err, cluster.ErrRangeNotFound):
				result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodePM_SEAL_RANGE_NOT_FOUND, Message: err.Error()}
			case errors.Is(err, cluster.ErrNoDataNodeResponded):
				result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodePM_SEAL_RANGE_NO_DN_RESPONDED, Message: err.Error()}
			case errors.Is(err, cluster.ErrNotEnoughDataNodes):
				result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodePM_NO_AVAILABLE_DN, Message: err.Error()}
			default:
				result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()}
			}
		} else {
			result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
		}

		sealResponses = append(sealResponses, result)
	}
	resp.SealResponses = sealResponses
	resp.OK()
}

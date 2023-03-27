package handler

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

func (s *Sbp) ListRanges(req *protocol.ListRangesRequest, resp *protocol.ListRangesResponse) {
	ctx := req.Context()
	if !s.c.IsLeader() {
		s.notLeaderError(ctx, resp)
		return
	}

	listResponses := make([]*rpcfb.ListRangesResultT, 0, len(req.RangeCriteria))
	for _, owner := range req.RangeCriteria {
		ranges, err := s.c.ListRanges(ctx, owner)

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

func (s *Sbp) SealRanges(req *protocol.SealRangesRequest, resp *protocol.SealRangesResponse) {
	ctx := req.Context()
	if !s.c.IsLeader() {
		s.notLeaderError(ctx, resp)
		return
	}

	sealResponses := make([]*rpcfb.SealRangesResultT, 0, len(req.Ranges))
	for _, rangeID := range req.Ranges {
		r, err := s.c.SealRange(ctx, rangeID)

		result := &rpcfb.SealRangesResultT{
			Range: r,
		}
		if err != nil {
			result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()}
		} else {
			result.Status = &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}
		}

		sealResponses = append(sealResponses, result)
	}
	resp.SealResponses = sealResponses
	resp.OK()
}

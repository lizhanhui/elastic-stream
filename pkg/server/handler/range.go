package handler

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

func (s *Sbp) ListRange(req *protocol.ListRangesRequest) (resp *protocol.ListRangesResponse) {
	resp = &protocol.ListRangesResponse{}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	listResponses := make([]*rpcfb.ListRangesResultT, 0, len(req.RangeCriteria))
	for _, owner := range req.RangeCriteria {
		ranges, err := s.c.ListRanges(owner)

		result := &rpcfb.ListRangesResultT{
			RangeCriteria: owner,
		}
		if err != nil {
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_INTERNAL_SERVER_ERROR, Message: err.Error()})
		} else {
			result.Ranges = ranges
		}

		listResponses = append(listResponses, result)
	}
	resp.ListResponses = listResponses
	resp.OK()
	return
}

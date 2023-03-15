package handler

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
)

// Sbp is an sbp handler, implements server.Handler
type Sbp struct {
	// TODO use an interface (or multiple interfaces) instead of *cluster.RaftCluster
	c *cluster.RaftCluster
}

// NewSbp creates a sbp handler
func NewSbp(cluster *cluster.RaftCluster) *Sbp {
	return &Sbp{
		c: cluster,
	}
}

func (s *Sbp) ListRange(req *protocol.ListRangesRequest) (resp *protocol.ListRangesResponse) {
	resp = &protocol.ListRangesResponse{}
	if !s.c.IsLeader() {
		resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_NOT_LEADER, Message: "not leader"})
		return
	}

	listResponses := make([]*rpcfb.ListRangesResultT, 0, len(req.RangeCriteria))
	for _, owner := range req.RangeCriteria {
		ranges, err := s.c.ListRanges(owner)

		result := &rpcfb.ListRangesResultT{
			RangeCriteria: owner,
		}
		if err != nil {
			result.Status.Code = rpcfb.ErrorCodeUNKNOWN
			result.Status.Message = err.Error()
		} else {
			result.Ranges = ranges
		}

		listResponses = append(listResponses, result)
	}
	resp.ListResponses = listResponses
	return
}

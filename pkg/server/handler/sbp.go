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

func (s *Sbp) ListRange(req *protocol.ListRangesRequest) *protocol.ListRangesResponse {
	listResponses := make([]*rpcfb.ListRangesResultT, 0, len(req.RangeOwners))
	for _, owner := range req.RangeOwners {
		ranges, err := s.c.ListRanges(owner)

		result := &rpcfb.ListRangesResultT{
			RangeOwner: owner,
		}
		if err != nil {
			result.ErrorCode = rpcfb.ErrorCodeUNKNOWN
			result.ErrorMessage = err.Error()
		} else {
			result.Ranges = ranges
		}

		listResponses = append(listResponses, result)
	}
	return &protocol.ListRangesResponse{
		ListRangesResponseT: &rpcfb.ListRangesResponseT{
			ListResponses: listResponses,
		},
		HasNext: false,
	}
}

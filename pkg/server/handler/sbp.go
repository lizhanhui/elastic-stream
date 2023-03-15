package handler

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
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

// notLeaderError sets "PM_NOT_LEADER" error in the response
func (s *Sbp) notLeaderError(response protocol.Response) {
	response.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_NOT_LEADER, Message: "not leader", Detail: s.pmInfo()})
}

func (s *Sbp) pmInfo() []byte {
	leader := s.c.Leader()
	pm := &rpcfb.PlacementManagerT{
		Nodes: []*rpcfb.PlacementManagerNodeT{
			{
				Name:          leader.Name,
				AdvertiseAddr: leader.SbpAddr,
				IsLeader:      true,
			},
		},
	}
	return fbutil.Marshal(pm)
}

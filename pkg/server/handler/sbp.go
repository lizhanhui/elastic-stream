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

func (s *Sbp) CreateStreams(req *protocol.CreateStreamsRequest) (resp *protocol.CreateStreamsResponse) {
	resp = &protocol.CreateStreamsResponse{}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}
	// TODO implement me
	_ = req
	panic("implement me")
}

func (s *Sbp) DeleteStreams(req *protocol.DeleteStreamsRequest) (resp *protocol.DeleteStreamsResponse) {
	resp = &protocol.DeleteStreamsResponse{}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}
	// TODO implement me
	_ = req
	panic("implement me")
}

func (s *Sbp) UpdateStreams(req *protocol.UpdateStreamsRequest) (resp *protocol.UpdateStreamsResponse) {
	resp = &protocol.UpdateStreamsResponse{}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}
	// TODO implement me
	_ = req
	panic("implement me")
}

func (s *Sbp) DescribeStreams(req *protocol.DescribeStreamsRequest) (resp *protocol.DescribeStreamsResponse) {
	resp = &protocol.DescribeStreamsResponse{}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}
	// TODO implement me
	_ = req
	panic("implement me")
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

package handler

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

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

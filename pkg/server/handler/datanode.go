package handler

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

func (s *Sbp) Heartbeat(req *protocol.HeartbeatRequest) (resp *protocol.HeartbeatResponse) {
	resp = &protocol.HeartbeatResponse{}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	// TODO: implement
	_ = req
	return
}

package handler

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

func (s *Sbp) Heartbeat(req *protocol.HeartbeatRequest) (resp *protocol.HeartbeatResponse) {
	resp = &protocol.HeartbeatResponse{
		HeartbeatResponseT: rpcfb.HeartbeatResponseT{
			ClientId:   req.ClientId,
			ClientRole: req.ClientRole,
			DataNode:   req.DataNode,
		},
	}
	if !s.c.IsLeader() {
		s.notLeaderError(resp)
		return
	}

	if req.ClientRole == rpcfb.ClientRoleCLIENT_ROLE_DATA_NODE {
		err := s.c.Heartbeat(req.DataNode)
		if err != nil {
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeUNKNOWN, Message: err.Error()})
			return
		}
	}
	return
}

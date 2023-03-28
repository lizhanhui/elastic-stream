package handler

import (
	"context"

	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

type Cluster interface {
	cluster.DataNode
	cluster.Range
	cluster.Stream
	cluster.Member
}

// Sbp is an sbp handler, implements server.Handler
type Sbp struct {
	c  Cluster
	lg *zap.Logger
}

// NewSbp creates a sbp handler
func NewSbp(c Cluster, lg *zap.Logger) *Sbp {
	return &Sbp{
		c:  c,
		lg: lg,
	}
}

// notLeaderError sets "PM_NOT_LEADER" error in the response
func (s *Sbp) notLeaderError(ctx context.Context, response protocol.OutResponse) {
	s.lg.Warn("not leader", traceutil.TraceLogField(ctx))
	response.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodePM_NOT_LEADER, Message: "not leader", Detail: s.pmInfo()})
}

func (s *Sbp) pmInfo() []byte {
	pm := &rpcfb.PlacementManagerT{Nodes: make([]*rpcfb.PlacementManagerNodeT, 0, 1)}
	leader := s.c.Leader()
	if leader != nil {
		pm.Nodes = append(pm.Nodes, &rpcfb.PlacementManagerNodeT{
			Name:          leader.Name,
			AdvertiseAddr: leader.SbpAddr,
			IsLeader:      true,
		})
	}
	return fbutil.Marshal(pm)
}

func (s *Sbp) Logger() *zap.Logger {
	return s.lg
}

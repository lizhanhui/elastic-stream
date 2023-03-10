package handler

import (
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

// Sbp is an sbp handler, implements server.Handler
type Sbp struct {
	// TODO
}

// NewSbp creates a sbp handler
func NewSbp() *Sbp {
	return &Sbp{}
}

func (s *Sbp) ListRange(req *protocol.ListRangesRequest) *protocol.ListRangesResponse {
	// TODO implement me
	_ = req
	panic("implement me")
}

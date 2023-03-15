package server

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/operation"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

// Handler responds to a request
// TODO support streaming
type Handler interface {
	// ListRange lists the ranges of a batch of streams. Or it could list the ranges of all the streams in a specific data node.
	ListRange(req *protocol.ListRangesRequest) *protocol.ListRangesResponse
}

var (
	_actionMap = map[operation.Operation]Action{
		{Code: operation.OpListRanges}: {
			newReq: func() protocol.Request { return &protocol.ListRangesRequest{} },
			act: func(handler Handler, req protocol.Request) (resp protocol.Response) {
				return handler.ListRange(req.(*protocol.ListRangesRequest))
			},
		},
	}
	_unknownAction = Action{
		newReq: func() protocol.Request { return nil },
		act: func(_ Handler, _ protocol.Request) (resp protocol.Response) {
			resp = &protocol.SystemErrorResponse{}
			resp.Error(rpcfb.ErrorCodeINVALID_REQUEST, "unknown operation")
			return
		},
	}
)

// Action is an action used to handle a request
type Action struct {
	newReq func() protocol.Request
	act    func(handler Handler, req protocol.Request) (resp protocol.Response)
}

// GetAction returns the action for the specified operation
func GetAction(op operation.Operation) *Action {
	if action, ok := _actionMap[op]; ok {
		return &action
	}
	return &_unknownAction
}

package server

import (
	"context"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/operation"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

// Handler responds to a request
// TODO support streaming
type Handler interface {
	// Heartbeat handles heartbeat requests.
	Heartbeat(ctx context.Context, req *protocol.HeartbeatRequest, resp *protocol.HeartbeatResponse)
	// ListRange lists the ranges of a batch of streams. Or it could list the ranges of all the streams in a specific data node.
	ListRange(ctx context.Context, req *protocol.ListRangesRequest, resp *protocol.ListRangesResponse)
	// CreateStreams creates a batch of streams.
	CreateStreams(ctx context.Context, req *protocol.CreateStreamsRequest, resp *protocol.CreateStreamsResponse)
	// DeleteStreams deletes a batch of streams.
	DeleteStreams(ctx context.Context, req *protocol.DeleteStreamsRequest, resp *protocol.DeleteStreamsResponse)
	// UpdateStreams updates a batch of streams.
	UpdateStreams(ctx context.Context, req *protocol.UpdateStreamsRequest, resp *protocol.UpdateStreamsResponse)
	// DescribeStreams describes a batch of streams.
	DescribeStreams(ctx context.Context, req *protocol.DescribeStreamsRequest, resp *protocol.DescribeStreamsResponse)
}

var (
	_actionMap = map[operation.Operation]Action{
		{Code: operation.OpHeartbeat}: {
			newReq:  func() protocol.Request { return &protocol.HeartbeatRequest{} },
			newResp: func() protocol.Response { return &protocol.HeartbeatResponse{} },
			act: func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response) {
				handler.Heartbeat(ctx, req.(*protocol.HeartbeatRequest), resp.(*protocol.HeartbeatResponse))
			},
		},
		{Code: operation.OpListRanges}: {
			newReq:  func() protocol.Request { return &protocol.ListRangesRequest{} },
			newResp: func() protocol.Response { return &protocol.ListRangesResponse{} },
			act: func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response) {
				handler.ListRange(ctx, req.(*protocol.ListRangesRequest), resp.(*protocol.ListRangesResponse))
			},
		},
		{Code: operation.OpCreateStreams}: {
			newReq:  func() protocol.Request { return &protocol.CreateStreamsRequest{} },
			newResp: func() protocol.Response { return &protocol.CreateStreamsResponse{} },
			act: func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response) {
				handler.CreateStreams(ctx, req.(*protocol.CreateStreamsRequest), resp.(*protocol.CreateStreamsResponse))
			},
		},
		{Code: operation.OpDeleteStreams}: {
			newReq:  func() protocol.Request { return &protocol.DeleteStreamsRequest{} },
			newResp: func() protocol.Response { return &protocol.DeleteStreamsResponse{} },
			act: func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response) {
				handler.DeleteStreams(ctx, req.(*protocol.DeleteStreamsRequest), resp.(*protocol.DeleteStreamsResponse))
			},
		},
		{Code: operation.OpUpdateStreams}: {
			newReq:  func() protocol.Request { return &protocol.UpdateStreamsRequest{} },
			newResp: func() protocol.Response { return &protocol.UpdateStreamsResponse{} },
			act: func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response) {
				handler.UpdateStreams(ctx, req.(*protocol.UpdateStreamsRequest), resp.(*protocol.UpdateStreamsResponse))
			},
		},
		{Code: operation.OpDescribeStreams}: {
			newReq:  func() protocol.Request { return &protocol.DescribeStreamsRequest{} },
			newResp: func() protocol.Response { return &protocol.DescribeStreamsResponse{} },
			act: func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response) {
				handler.DescribeStreams(ctx, req.(*protocol.DescribeStreamsRequest), resp.(*protocol.DescribeStreamsResponse))
			},
		},
	}
	_unknownAction = Action{
		newReq:  func() protocol.Request { return nil },
		newResp: func() protocol.Response { return &protocol.SystemErrorResponse{} },
		act: func(_ context.Context, _ Handler, _ protocol.Request, resp protocol.Response) {
			resp.Error(&rpcfb.StatusT{Code: rpcfb.ErrorCodeNOT_FOUND, Message: "unknown operation"})
		},
	}
)

// Action is an action used to handle a request
type Action struct {
	newReq  func() protocol.Request
	newResp func() protocol.Response
	act     func(ctx context.Context, handler Handler, req protocol.Request, resp protocol.Response)
}

// GetAction returns the action for the specified operation
func GetAction(op operation.Operation) *Action {
	if action, ok := _actionMap[op]; ok {
		return &action
	}
	return &_unknownAction
}

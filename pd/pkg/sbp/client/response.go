package client

import (
	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
)

// newInResponse returns a new empty response for the given operation.
// It returns nil if the operation is not supported.
func newInResponse(op rpcfb.OperationCode) protocol.InResponse {
	switch op {
	case rpcfb.OperationCodeSEAL_RANGE:
		return &protocol.SealRangeResponse{}
	case rpcfb.OperationCodeCREATE_RANGE:
		return &protocol.CreateRangeResponse{}
	case rpcfb.OperationCodeCREATE_STREAM:
		return &protocol.CreateStreamResponse{}
	case rpcfb.OperationCodeREPORT_METRICS:
		return &protocol.ReportMetricsResponse{}
	case rpcfb.OperationCodeCOMMIT_OBJECT:
		return &protocol.CommitObjectResponse{}
	default:
		return nil
	}
}

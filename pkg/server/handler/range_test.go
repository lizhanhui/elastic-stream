package handler

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
)

func TestListRange(t *testing.T) {
	re := require.New(t)

	sbp, closeFunc := startSbp(t)
	defer closeFunc()

	preHeartbeat(t, sbp, 0)
	preHeartbeat(t, sbp, 1)
	preHeartbeat(t, sbp, 2)
	preCreateStreams(t, sbp, 3, 3)

	// test list range by stream id
	req := &protocol.ListRangesRequest{ListRangesRequestT: rpcfb.ListRangesRequestT{
		RangeCriteria: []*rpcfb.RangeCriteriaT{
			{StreamId: 0, DataNode: &rpcfb.DataNodeT{NodeId: -1}},
			{StreamId: 1, DataNode: &rpcfb.DataNodeT{NodeId: -1}},
			{StreamId: 2, DataNode: &rpcfb.DataNodeT{NodeId: -1}},
		},
	}}
	resp := &protocol.ListRangesResponse{}
	sbp.ListRange(context.Background(), req, resp)

	re.Equal(resp.Status.Code, rpcfb.ErrorCodeOK)
	re.Len(resp.ListResponses, 3)
	for i, result := range resp.ListResponses {
		re.Equal(result.RangeCriteria, req.RangeCriteria[i])
		re.Equal(result.Status.Code, rpcfb.ErrorCodeOK)
		re.Len(result.Ranges, 1)
	}

	// test list range by data node id
	req = &protocol.ListRangesRequest{ListRangesRequestT: rpcfb.ListRangesRequestT{
		RangeCriteria: []*rpcfb.RangeCriteriaT{
			{StreamId: -1, DataNode: &rpcfb.DataNodeT{NodeId: 0}},
			{StreamId: -1, DataNode: &rpcfb.DataNodeT{NodeId: 1}},
			{StreamId: -1, DataNode: &rpcfb.DataNodeT{NodeId: 2}},
		},
	}}
	resp = &protocol.ListRangesResponse{}
	sbp.ListRange(context.Background(), req, resp)

	re.Equal(resp.Status.Code, rpcfb.ErrorCodeOK)
	re.Len(resp.ListResponses, 3)
	for i, result := range resp.ListResponses {
		re.Equal(result.RangeCriteria, req.RangeCriteria[i])
		re.Equal(result.Status.Code, rpcfb.ErrorCodeOK)
		re.Len(result.Ranges, 3)
	}

	// test list range by stream id and data node id
	req = &protocol.ListRangesRequest{ListRangesRequestT: rpcfb.ListRangesRequestT{
		RangeCriteria: []*rpcfb.RangeCriteriaT{
			{StreamId: 0, DataNode: &rpcfb.DataNodeT{NodeId: 0}},
			{StreamId: 1, DataNode: &rpcfb.DataNodeT{NodeId: 1}},
			{StreamId: 2, DataNode: &rpcfb.DataNodeT{NodeId: 2}},
		},
	}}
	resp = &protocol.ListRangesResponse{}
	sbp.ListRange(context.Background(), req, resp)

	re.Equal(resp.Status.Code, rpcfb.ErrorCodeOK)
	re.Len(resp.ListResponses, 3)
	for i, result := range resp.ListResponses {
		re.Equal(result.RangeCriteria, req.RangeCriteria[i])
		re.Equal(result.Status.Code, rpcfb.ErrorCodeOK)
		re.Len(result.Ranges, 1)
	}
}

func preHeartbeat(tb testing.TB, sbp *Sbp, nodeID int32) {
	re := require.New(tb)

	req := &protocol.HeartbeatRequest{HeartbeatRequestT: rpcfb.HeartbeatRequestT{
		ClientRole: rpcfb.ClientRoleCLIENT_ROLE_DATA_NODE,
		DataNode: &rpcfb.DataNodeT{
			NodeId:        nodeID,
			AdvertiseAddr: fmt.Sprintf("addr-%d", nodeID),
		}}}
	resp := &protocol.HeartbeatResponse{}
	sbp.Heartbeat(context.Background(), req, resp)

	re.Equal(resp.Status.Code, rpcfb.ErrorCodeOK)
}

func preCreateStreams(tb testing.TB, sbp *Sbp, num int, replicaNum int8) {
	re := require.New(tb)

	req := &protocol.CreateStreamsRequest{CreateStreamsRequestT: rpcfb.CreateStreamsRequestT{
		Streams: make([]*rpcfb.StreamT, num),
	}}
	for i := 0; i < num; i++ {
		req.Streams[i] = &rpcfb.StreamT{ReplicaNums: replicaNum}
	}
	resp := &protocol.CreateStreamsResponse{}
	sbp.CreateStreams(context.Background(), req, resp)

	re.Equal(resp.Status.Code, rpcfb.ErrorCodeOK)
}

package protocol

import (
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

func TestMarshalListRangesResponse(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	formatter := newFlatBufferFormatter()
	buf, _ := formatter.marshalListRangesResponse(&ListRangesResponse{
		ThrottleTimeMs: 1,
		ListResponses: []ListRangesResult{
			{
				RangeOwner: RangeOwner{
					DataNode: DataNode{
						NodeID:        2,
						AdvertiseAddr: "a",
					},
					StreamID: 3,
				},
				ErrorCode:    4,
				ErrorMessage: "b",
				Ranges: []Range{
					{
						StreamID:    5,
						RangeIndex:  6,
						StartOffset: 7,
						EndOffset:   8,
						NextOffset:  9,
						ReplicaNodes: []ReplicaNode{
							{
								DataNode: DataNode{
									NodeID:        10,
									AdvertiseAddr: "c",
								},
								IsPrimary: true,
							},
							{
								DataNode: DataNode{
									NodeID:        11,
									AdvertiseAddr: "d",
								},
								IsPrimary: false,
							},
						},
					},
				},
			},
		},
		ErrorCode:    12,
		ErrorMessage: "e",
	})

	response := rpcfb.GetRootAsListRangesResponse(buf, 0)
	re.Equal(int32(1), response.ThrottleTimeMs())

	re.Equal(1, response.ListResponsesLength())
	listRangesResult := new(rpcfb.ListRangesResult)
	response.ListResponses(listRangesResult, 0)

	fbRangeOwner := new(rpcfb.RangeOwner)
	listRangesResult.RangeOwner(fbRangeOwner)

	fbDataNode := new(rpcfb.DataNode)
	fbRangeOwner.DataNode(fbDataNode)
	re.Equal(int32(2), fbDataNode.NodeId())
	re.Equal("a", string(fbDataNode.AdvertiseAddr()))

	re.Equal(int64(3), fbRangeOwner.StreamId())

	re.Equal(rpcfb.ErrorCode(4), listRangesResult.ErrorCode())
	re.Equal("b", string(listRangesResult.ErrorMessage()))

	re.Equal(1, listRangesResult.RangesLength())
	fbRange := new(rpcfb.Range)
	listRangesResult.Ranges(fbRange, 0)
	re.Equal(int64(5), fbRange.StreamId())
	re.Equal(int32(6), fbRange.RangeIndex())
	re.Equal(int64(7), fbRange.StartOffset())
	re.Equal(int64(8), fbRange.EndOffset())
	re.Equal(int64(9), fbRange.NextOffset())

	re.Equal(2, fbRange.ReplicaNodesLength())
	fbReplicaNode := new(rpcfb.ReplicaNode)

	fbRange.ReplicaNodes(fbReplicaNode, 0)
	fbReplicaNode.DataNode(fbDataNode)
	re.Equal(int32(10), fbDataNode.NodeId())
	re.Equal("c", string(fbDataNode.AdvertiseAddr()))
	re.True(fbReplicaNode.IsPrimary())

	fbRange.ReplicaNodes(fbReplicaNode, 1)
	fbReplicaNode.DataNode(fbDataNode)
	re.Equal(int32(11), fbDataNode.NodeId())
	re.Equal("d", string(fbDataNode.AdvertiseAddr()))
	re.False(fbReplicaNode.IsPrimary())

	re.Equal(rpcfb.ErrorCode(12), response.ErrorCode())
	re.Equal("e", string(response.ErrorMessage()))
}

func TestUnmarshalListRangesRequest(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	want := &ListRangesRequest{
		TimeoutMs: 1,
		RangeOwners: []RangeOwner{
			{
				DataNode: DataNode{
					NodeID: 2,
				},
			},
			{
				StreamID: 3,
			},
		},
	}
	builder := flatbuffers.NewBuilder(1024)
	rangeOwnerOffsets := make([]flatbuffers.UOffsetT, len(want.RangeOwners))
	for i := range want.RangeOwners {
		advertiseAddr := builder.CreateString(want.RangeOwners[i].DataNode.AdvertiseAddr)
		rpcfb.DataNodeStart(builder)
		rpcfb.DataNodeAddNodeId(builder, want.RangeOwners[i].DataNode.NodeID)
		rpcfb.DataNodeAddAdvertiseAddr(builder, advertiseAddr)
		dataNode := rpcfb.DataNodeEnd(builder)
		rpcfb.RangeOwnerStart(builder)
		rpcfb.RangeOwnerAddDataNode(builder, dataNode)
		rpcfb.RangeOwnerAddStreamId(builder, want.RangeOwners[i].StreamID)
		rangeOwnerOffsets[i] = rpcfb.RangeOwnerEnd(builder)
	}
	rpcfb.ListRangesRequestStartRangeOwnersVector(builder, len(want.RangeOwners))
	for i := len(want.RangeOwners) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(rangeOwnerOffsets[i])
	}
	rangeOwners := builder.EndVector(len(want.RangeOwners))
	rpcfb.ListRangesRequestStart(builder)
	rpcfb.ListRangesRequestAddTimeoutMs(builder, want.TimeoutMs)
	rpcfb.ListRangesRequestAddRangeOwners(builder, rangeOwners)
	builder.Finish(rpcfb.ListRangesRequestEnd(builder))
	buf := builder.FinishedBytes()

	formatter := newFlatBufferFormatter()
	request := &ListRangesRequest{}
	err := formatter.unmarshalListRangesRequest(buf, request)
	re.NoError(err)
	re.Equal(want, request)
}

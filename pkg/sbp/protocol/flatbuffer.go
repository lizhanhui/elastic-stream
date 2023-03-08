package protocol

import (
	"sync"

	"github.com/bytedance/gopkg/lang/mcache"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

type flatBufferFormatter struct {
	builderPool sync.Pool

	dataNodePool   sync.Pool
	rangeOwnerPool sync.Pool
}

func newFlatBufferFormatter() *flatBufferFormatter {
	return &flatBufferFormatter{
		builderPool:    sync.Pool{New: func() interface{} { return flatbuffers.NewBuilder(1024) }},
		dataNodePool:   sync.Pool{New: func() interface{} { return new(rpcfb.DataNode) }},
		rangeOwnerPool: sync.Pool{New: func() interface{} { return new(rpcfb.RangeOwner) }},
	}
}

func (f *flatBufferFormatter) unmarshalListRangesRequest(bytes []byte, request *ListRangesRequest) error {
	fbRequest := rpcfb.GetRootAsListRangesRequest(bytes, 0)

	request.TimeoutMs = fbRequest.TimeoutMs()

	request.RangeOwners = make([]RangeOwner, fbRequest.RangeOwnersLength())
	fbRangeOwner := f.rangeOwnerPool.Get().(*rpcfb.RangeOwner)
	defer f.rangeOwnerPool.Put(fbRangeOwner)
	for i := 0; i < fbRequest.RangeOwnersLength(); i++ {
		fbRequest.RangeOwners(fbRangeOwner, i)
		f.unmarshalRangeOwner(fbRangeOwner, &request.RangeOwners[i])
	}

	return nil
}

func (f *flatBufferFormatter) unmarshalRangeOwner(fbRangeOwner *rpcfb.RangeOwner, rangeOwner *RangeOwner) {
	rangeOwner.StreamID = fbRangeOwner.StreamId()

	fbDataNode := f.dataNodePool.Get().(*rpcfb.DataNode)
	defer f.dataNodePool.Put(fbDataNode)
	fbRangeOwner.DataNode(fbDataNode)
	rangeOwner.DataNode = DataNode{
		NodeID:        fbDataNode.NodeId(),
		AdvertiseAddr: string(fbDataNode.AdvertiseAddr()),
	}
}

func (f *flatBufferFormatter) marshalSystemErrorResponse(response *SystemErrorResponse) ([]byte, error) {
	builder := f.builderPool.Get().(*flatbuffers.Builder)
	defer func() {
		builder.Reset()
		f.builderPool.Put(builder)
	}()

	errorMessage := builder.CreateString(response.ErrorMessage)

	rpcfb.SystemErrorResponseStart(builder)
	rpcfb.SystemErrorResponseAddErrorCode(builder, rpcfb.ErrorCode(response.ErrorCode))
	rpcfb.SystemErrorResponseAddErrorMessage(builder, errorMessage)

	builder.Finish(rpcfb.SystemErrorResponseEnd(builder))
	return finishedBytes(builder), nil
}

func (f *flatBufferFormatter) marshalListRangesResponse(response *ListRangesResponse) ([]byte, error) {
	builder := f.builderPool.Get().(*flatbuffers.Builder)
	defer func() {
		builder.Reset()
		f.builderPool.Put(builder)
	}()

	errorMessage := builder.CreateString(response.ErrorMessage)

	listResponsesOffsets := make([]flatbuffers.UOffsetT, len(response.ListResponses))
	for i := range response.ListResponses {
		listResponsesOffsets[i] = f.marshalListResponse(builder, &response.ListResponses[i])
	}
	rpcfb.ListRangesResponseStartListResponsesVector(builder, len(response.ListResponses))
	for i := len(response.ListResponses) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(listResponsesOffsets[i])
	}
	listResponses := builder.EndVector(len(response.ListResponses))

	rpcfb.ListRangesResponseStart(builder)
	rpcfb.ListRangesResponseAddThrottleTimeMs(builder, response.ThrottleTimeMs)
	rpcfb.ListRangesResponseAddErrorCode(builder, rpcfb.ErrorCode(response.ErrorCode))
	rpcfb.ListRangesResponseAddErrorMessage(builder, errorMessage)
	rpcfb.ListRangesResponseAddListResponses(builder, listResponses)

	fbListRangesResponse := rpcfb.ListRangesResponseEnd(builder)
	builder.Finish(fbListRangesResponse)
	return finishedBytes(builder), nil
}

func (f *flatBufferFormatter) marshalListResponse(builder *flatbuffers.Builder, listResponse *ListRangesResult) flatbuffers.UOffsetT {
	errorMessage := builder.CreateString(listResponse.ErrorMessage)

	rangeOwner := f.marshalRangeOwner(builder, &listResponse.RangeOwner)

	rangeOffsets := make([]flatbuffers.UOffsetT, len(listResponse.Ranges))
	for i := range listResponse.Ranges {
		rangeOffsets[i] = f.marshalRange(builder, &listResponse.Ranges[i])
	}
	rpcfb.ListRangesResultStartRangesVector(builder, len(listResponse.Ranges))
	for i := len(listResponse.Ranges) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(rangeOffsets[i])
	}
	ranges := builder.EndVector(len(listResponse.Ranges))

	rpcfb.ListRangesResultStart(builder)
	rpcfb.ListRangesResultAddErrorCode(builder, rpcfb.ErrorCode(listResponse.ErrorCode))
	rpcfb.ListRangesResultAddErrorMessage(builder, errorMessage)
	rpcfb.ListRangesResultAddRangeOwner(builder, rangeOwner)
	rpcfb.ListRangesResultAddRanges(builder, ranges)
	return rpcfb.ListRangesResultEnd(builder)
}

func (f *flatBufferFormatter) marshalRangeOwner(builder *flatbuffers.Builder, rangeOwner *RangeOwner) flatbuffers.UOffsetT {
	dataNode := f.marshalDataNode(builder, &rangeOwner.DataNode)

	rpcfb.RangeOwnerStart(builder)
	rpcfb.RangeOwnerAddStreamId(builder, rangeOwner.StreamID)
	rpcfb.RangeOwnerAddDataNode(builder, dataNode)
	return rpcfb.RangeOwnerEnd(builder)
}

func (f *flatBufferFormatter) marshalDataNode(builder *flatbuffers.Builder, dataNode *DataNode) flatbuffers.UOffsetT {
	advertiseAddr := builder.CreateString(dataNode.AdvertiseAddr)

	rpcfb.DataNodeStart(builder)
	rpcfb.DataNodeAddNodeId(builder, dataNode.NodeID)
	rpcfb.DataNodeAddAdvertiseAddr(builder, advertiseAddr)
	return rpcfb.DataNodeEnd(builder)
}

func (f *flatBufferFormatter) marshalRange(builder *flatbuffers.Builder, r *Range) flatbuffers.UOffsetT {
	replicaNodeOffsets := make([]flatbuffers.UOffsetT, len(r.ReplicaNodes))
	for i := range r.ReplicaNodes {
		replicaNodeOffsets[i] = f.marshalReplicaNode(builder, &r.ReplicaNodes[i])
	}
	rpcfb.RangeStartReplicaNodesVector(builder, len(r.ReplicaNodes))
	for i := len(r.ReplicaNodes) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(replicaNodeOffsets[i])
	}
	replicaNodes := builder.EndVector(len(r.ReplicaNodes))

	rpcfb.RangeStart(builder)
	rpcfb.RangeAddStreamId(builder, r.StreamID)
	rpcfb.RangeAddRangeIndex(builder, r.RangeIndex)
	rpcfb.RangeAddStartOffset(builder, r.StartOffset)
	rpcfb.RangeAddEndOffset(builder, r.EndOffset)
	rpcfb.RangeAddNextOffset(builder, r.NextOffset)
	rpcfb.RangeAddReplicaNodes(builder, replicaNodes)
	return rpcfb.RangeEnd(builder)
}

func (f *flatBufferFormatter) marshalReplicaNode(builder *flatbuffers.Builder, replicaNode *ReplicaNode) flatbuffers.UOffsetT {
	dataNode := f.marshalDataNode(builder, &replicaNode.DataNode)

	rpcfb.ReplicaNodeStart(builder)
	rpcfb.ReplicaNodeAddDataNode(builder, dataNode)
	rpcfb.ReplicaNodeAddIsPrimary(builder, replicaNode.IsPrimary)
	return rpcfb.ReplicaNodeEnd(builder)
}

func finishedBytes(builder *flatbuffers.Builder) []byte {
	bytes := builder.FinishedBytes()
	result := mcache.Malloc(len(bytes))
	copy(result, bytes)
	return result
}

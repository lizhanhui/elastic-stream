// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package rpcfb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type DeleteStreamsRequestT struct {
	TimeoutMs int32 `json:"timeout_ms"`
	Streams []*StreamT `json:"streams"`
}

func (t *DeleteStreamsRequestT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil { return 0 }
	streamsOffset := flatbuffers.UOffsetT(0)
	if t.Streams != nil {
		streamsLength := len(t.Streams)
		streamsOffsets := make([]flatbuffers.UOffsetT, streamsLength)
		for j := 0; j < streamsLength; j++ {
			streamsOffsets[j] = t.Streams[j].Pack(builder)
		}
		DeleteStreamsRequestStartStreamsVector(builder, streamsLength)
		for j := streamsLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(streamsOffsets[j])
		}
		streamsOffset = builder.EndVector(streamsLength)
	}
	DeleteStreamsRequestStart(builder)
	DeleteStreamsRequestAddTimeoutMs(builder, t.TimeoutMs)
	DeleteStreamsRequestAddStreams(builder, streamsOffset)
	return DeleteStreamsRequestEnd(builder)
}

func (rcv *DeleteStreamsRequest) UnPackTo(t *DeleteStreamsRequestT) {
	t.TimeoutMs = rcv.TimeoutMs()
	streamsLength := rcv.StreamsLength()
	t.Streams = make([]*StreamT, streamsLength)
	for j := 0; j < streamsLength; j++ {
		x := Stream{}
		rcv.Streams(&x, j)
		t.Streams[j] = x.UnPack()
	}
}

func (rcv *DeleteStreamsRequest) UnPack() *DeleteStreamsRequestT {
	if rcv == nil { return nil }
	t := &DeleteStreamsRequestT{}
	rcv.UnPackTo(t)
	return t
}

type DeleteStreamsRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsDeleteStreamsRequest(buf []byte, offset flatbuffers.UOffsetT) *DeleteStreamsRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &DeleteStreamsRequest{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsDeleteStreamsRequest(buf []byte, offset flatbuffers.UOffsetT) *DeleteStreamsRequest {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &DeleteStreamsRequest{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *DeleteStreamsRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *DeleteStreamsRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *DeleteStreamsRequest) TimeoutMs() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DeleteStreamsRequest) MutateTimeoutMs(n int32) bool {
	return rcv._tab.MutateInt32Slot(4, n)
}

func (rcv *DeleteStreamsRequest) Streams(obj *Stream, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *DeleteStreamsRequest) StreamsLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func DeleteStreamsRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func DeleteStreamsRequestAddTimeoutMs(builder *flatbuffers.Builder, timeoutMs int32) {
	builder.PrependInt32Slot(0, timeoutMs, 0)
}
func DeleteStreamsRequestAddStreams(builder *flatbuffers.Builder, streams flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(streams), 0)
}
func DeleteStreamsRequestStartStreamsVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func DeleteStreamsRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package rpcfb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type CreateRangeRequestT struct {
	TimeoutMs int32 `json:"timeout_ms"`
	Range *RangeT `json:"range"`
}

func (t *CreateRangeRequestT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil { return 0 }
	range_Offset := t.Range.Pack(builder)
	CreateRangeRequestStart(builder)
	CreateRangeRequestAddTimeoutMs(builder, t.TimeoutMs)
	CreateRangeRequestAddRange(builder, range_Offset)
	return CreateRangeRequestEnd(builder)
}

func (rcv *CreateRangeRequest) UnPackTo(t *CreateRangeRequestT) {
	t.TimeoutMs = rcv.TimeoutMs()
	t.Range = rcv.Range(nil).UnPack()
}

func (rcv *CreateRangeRequest) UnPack() *CreateRangeRequestT {
	if rcv == nil { return nil }
	t := &CreateRangeRequestT{}
	rcv.UnPackTo(t)
	return t
}

type CreateRangeRequest struct {
	_tab flatbuffers.Table
}

func GetRootAsCreateRangeRequest(buf []byte, offset flatbuffers.UOffsetT) *CreateRangeRequest {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &CreateRangeRequest{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsCreateRangeRequest(buf []byte, offset flatbuffers.UOffsetT) *CreateRangeRequest {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &CreateRangeRequest{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *CreateRangeRequest) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *CreateRangeRequest) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *CreateRangeRequest) TimeoutMs() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *CreateRangeRequest) MutateTimeoutMs(n int32) bool {
	return rcv._tab.MutateInt32Slot(4, n)
}

func (rcv *CreateRangeRequest) Range(obj *Range) *Range {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Range)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func CreateRangeRequestStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func CreateRangeRequestAddTimeoutMs(builder *flatbuffers.Builder, timeoutMs int32) {
	builder.PrependInt32Slot(0, timeoutMs, 0)
}
func CreateRangeRequestAddRange(builder *flatbuffers.Builder, range_ flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(range_), 0)
}
func CreateRangeRequestEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
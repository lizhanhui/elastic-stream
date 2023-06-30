// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package rpcfb

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type FetchResponseT struct {
	Status *StatusT `json:"status"`
	Entries []*FetchResultEntryT `json:"entries"`
	ThrottleTimeMs int32 `json:"throttle_time_ms"`
}

func (t *FetchResponseT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil { return 0 }
	statusOffset := t.Status.Pack(builder)
	entriesOffset := flatbuffers.UOffsetT(0)
	if t.Entries != nil {
		entriesLength := len(t.Entries)
		entriesOffsets := make([]flatbuffers.UOffsetT, entriesLength)
		for j := 0; j < entriesLength; j++ {
			entriesOffsets[j] = t.Entries[j].Pack(builder)
		}
		FetchResponseStartEntriesVector(builder, entriesLength)
		for j := entriesLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(entriesOffsets[j])
		}
		entriesOffset = builder.EndVector(entriesLength)
	}
	FetchResponseStart(builder)
	FetchResponseAddStatus(builder, statusOffset)
	FetchResponseAddEntries(builder, entriesOffset)
	FetchResponseAddThrottleTimeMs(builder, t.ThrottleTimeMs)
	return FetchResponseEnd(builder)
}

func (rcv *FetchResponse) UnPackTo(t *FetchResponseT) {
	t.Status = rcv.Status(nil).UnPack()
	entriesLength := rcv.EntriesLength()
	t.Entries = make([]*FetchResultEntryT, entriesLength)
	for j := 0; j < entriesLength; j++ {
		x := FetchResultEntry{}
		rcv.Entries(&x, j)
		t.Entries[j] = x.UnPack()
	}
	t.ThrottleTimeMs = rcv.ThrottleTimeMs()
}

func (rcv *FetchResponse) UnPack() *FetchResponseT {
	if rcv == nil { return nil }
	t := &FetchResponseT{}
	rcv.UnPackTo(t)
	return t
}

type FetchResponse struct {
	_tab flatbuffers.Table
}

func GetRootAsFetchResponse(buf []byte, offset flatbuffers.UOffsetT) *FetchResponse {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &FetchResponse{}
	x.Init(buf, n+offset)
	return x
}

func GetSizePrefixedRootAsFetchResponse(buf []byte, offset flatbuffers.UOffsetT) *FetchResponse {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &FetchResponse{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func (rcv *FetchResponse) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *FetchResponse) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *FetchResponse) Status(obj *Status) *Status {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Status)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *FetchResponse) Entries(obj *FetchResultEntry, j int) bool {
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

func (rcv *FetchResponse) EntriesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *FetchResponse) ThrottleTimeMs() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *FetchResponse) MutateThrottleTimeMs(n int32) bool {
	return rcv._tab.MutateInt32Slot(8, n)
}

func FetchResponseStart(builder *flatbuffers.Builder) {
	builder.StartObject(3)
}
func FetchResponseAddStatus(builder *flatbuffers.Builder, status flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(status), 0)
}
func FetchResponseAddEntries(builder *flatbuffers.Builder, entries flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(entries), 0)
}
func FetchResponseStartEntriesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func FetchResponseAddThrottleTimeMs(builder *flatbuffers.Builder, throttleTimeMs int32) {
	builder.PrependInt32Slot(2, throttleTimeMs, 0)
}
func FetchResponseEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
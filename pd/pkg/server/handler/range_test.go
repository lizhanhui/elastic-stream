package handler

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
)

func TestHandler_ListRange(t *testing.T) {
	type args struct {
		StreamID      int64
		RangeServerID int32
	}
	tests := []struct {
		name string
		args args
		want []*rpcfb.RangeT
	}{
		{
			name: "list range by stream id",
			args: args{StreamID: 1, RangeServerID: -1},
			want: []*rpcfb.RangeT{
				{StreamId: 1, Epoch: 1, Index: 0, Start: 0, End: 42},
				{StreamId: 1, Epoch: 2, Index: 1, Start: 42, End: -1},
			},
		},
		{
			name: "list range by non-exist stream id",
			args: args{StreamID: 10, RangeServerID: -1},
			want: []*rpcfb.RangeT{},
		},
		{
			name: "list range by range server id",
			args: args{StreamID: -1, RangeServerID: 1},
			want: []*rpcfb.RangeT{
				{StreamId: 0, Epoch: 1, Index: 0, Start: 0, End: 42},
				{StreamId: 0, Epoch: 2, Index: 1, Start: 42, End: -1},
				{StreamId: 1, Epoch: 1, Index: 0, Start: 0, End: 42},
				{StreamId: 1, Epoch: 2, Index: 1, Start: 42, End: -1},
				{StreamId: 2, Epoch: 1, Index: 0, Start: 0, End: 42},
				{StreamId: 2, Epoch: 2, Index: 1, Start: 42, End: -1},
			},
		},
		{
			name: "list range by non-exist range server id",
			args: args{StreamID: -1, RangeServerID: 10},
			want: []*rpcfb.RangeT{},
		},
		{
			name: "list range by stream id and range server id",
			args: args{StreamID: 2, RangeServerID: 2},
			want: []*rpcfb.RangeT{
				{StreamId: 2, Epoch: 1, Index: 0, Start: 0, End: 42},
				{StreamId: 2, Epoch: 2, Index: 1, Start: 42, End: -1},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			h, closeFunc := startSbpHandler(t, nil, true)
			defer closeFunc()

			// prepare: 3 range servers, 3 streams, 1 sealed range and 1 writable range per stream
			preHeartbeats(t, h, 0, 1, 2)
			streamIDs := preCreateStreams(t, h, 3, 3)
			re.Equal([]int64{0, 1, 2}, streamIDs)
			for _, id := range streamIDs {
				r := preNewRange(t, h, id, true, 42)
				re.Equal(int32(0), r.Index)
				r = preNewRange(t, h, id, false)
				re.Equal(int32(1), r.Index)
			}

			req := &protocol.ListRangeRequest{ListRangeRequestT: rpcfb.ListRangeRequestT{
				Criteria: &rpcfb.ListRangeCriteriaT{StreamId: tt.args.StreamID, ServerId: tt.args.RangeServerID},
			}}
			resp := &protocol.ListRangeResponse{}
			h.ListRange(req, resp)

			re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
			for _, r := range resp.Ranges {
				fmtRangeServers(r)
			}
			for _, r := range tt.want {
				fillRangeInfo(r)
			}
			re.Equal(tt.want, resp.Ranges)
		})
	}
}

type preRange struct {
	index int32
	start int64
	end   int64
}

func prepareRanges(t *testing.T, h *Handler, streamID int64, ranges []preRange) {
	re := require.New(t)
	for _, r := range ranges {
		var rr *rpcfb.RangeT
		if r.end == -1 {
			rr = preNewRange(t, h, streamID, false)
		} else {
			rr = preNewRange(t, h, streamID, true, r.end-r.start)
		}
		re.Equal(r.index, rr.Index)
		re.Equal(r.start, rr.Start)
		re.Equal(r.end, rr.End)
	}
}

func TestSealRange(t *testing.T) {
	type args struct {
		kind rpcfb.SealKind
		r    *rpcfb.RangeT
	}
	type want struct {
		returned *rpcfb.RangeT
		wantErr  bool
		errCode  rpcfb.ErrorCode
		errMsg   string
		after    []*rpcfb.RangeT
	}
	tests := []struct {
		name    string
		prepare []preRange
		args    args
		want    want
	}{
		{
			name: "normal case",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: 1, End: 84}},
			want: want{
				returned: &rpcfb.RangeT{Epoch: 2, Index: 1, Start: 42, End: 84},
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: 84},
				},
			},
		},
		{
			name: "nil range",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: nil},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "nil range",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid stream id",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{StreamId: -1, Epoch: 2, Index: 1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid stream id",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid range index",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: -1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid index",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid epoch",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: -1, Index: 1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid epoch",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid end",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: 1, End: -1}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid end",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "stream not found (deleted)",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, StreamId: 1, Index: 1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream 1 deleted: stream not found",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "stream not found (not exist)",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, StreamId: 2, Index: 1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream 2: stream not found",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name:    "empty stream",
			prepare: []preRange{},
			args:    args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 0, Index: 0, End: 42}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeRANGE_NOT_FOUND,
				errMsg:  "range 0-0: range not found",
				after:   []*rpcfb.RangeT{},
			},
		},
		{
			name: "range not found",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: 2, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeRANGE_NOT_FOUND,
				errMsg:  "range 0-2: range not found",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "seal range twice",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: 84},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: 1, End: 84}},
			want: want{
				returned: &rpcfb.RangeT{Epoch: 2, Index: 1, Start: 42, End: 84},
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: 84},
				},
			},
		},
		{
			name: "range already sealed",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: 0, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "range 0-0: range already sealed",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid end offset",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 2, Index: 1, End: 21}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "range 0-1 end 21 < start 42: invalid range end offset",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid epoch (less)",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 1, Index: 1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "range 0-1 epoch 1 != 2: invalid stream epoch",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid epoch (greater)",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{kind: rpcfb.SealKindPLACEMENT_DRIVER, r: &rpcfb.RangeT{Epoch: 3, Index: 1, End: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "range 0-1 epoch 3 != 2: invalid stream epoch",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			h, closeFunc := startSbpHandler(t, nil, true)
			defer closeFunc()

			// prepare
			preHeartbeats(t, h, 0, 1, 2)
			streamIDs := preCreateStreams(t, h, 3, 2)
			re.Equal([]int64{0, 1}, streamIDs)
			preDeleteStream(t, h, 1)
			prepareRanges(t, h, 0, tt.prepare)

			// seal range
			req := &protocol.SealRangeRequest{SealRangeRequestT: rpcfb.SealRangeRequestT{
				Kind:  tt.args.kind,
				Range: tt.args.r,
			}}
			resp := &protocol.SealRangeResponse{}
			h.SealRange(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
				fmtRangeServers(resp.Range)
				fillRangeInfo(tt.want.returned)
				re.Equal(tt.want.returned, resp.Range)
			}

			// list ranges
			lReq := &protocol.ListRangeRequest{ListRangeRequestT: rpcfb.ListRangeRequestT{
				Criteria: &rpcfb.ListRangeCriteriaT{StreamId: 0, ServerId: -1},
			}}
			lResp := &protocol.ListRangeResponse{}
			h.ListRange(lReq, lResp)
			re.Equal(rpcfb.ErrorCodeOK, lResp.Status.Code, lResp.Status.Message)

			// check list range response
			for _, r := range lResp.Ranges {
				fmtRangeServers(r)
			}
			for _, r := range tt.want.after {
				fillRangeInfo(r)
			}
			re.Equal(tt.want.after, lResp.Ranges)
		})
	}
}

func TestHandler_CreateRange(t *testing.T) {
	type args struct {
		r *rpcfb.RangeT
	}
	type want struct {
		returned *rpcfb.RangeT
		wantErr  bool
		errCode  rpcfb.ErrorCode
		errMsg   string
		after    []*rpcfb.RangeT
	}
	tests := []struct {
		name    string
		prepare []preRange
		args    args
		want    want
	}{
		{
			name: "normal case",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 1, Index: 1, Start: 42}},
			want: want{
				returned: &rpcfb.RangeT{Epoch: 1, Index: 1, Start: 42, End: -1},
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 1, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name:    "create the first range",
			prepare: []preRange{},
			args:    args{&rpcfb.RangeT{}},
			want: want{
				returned: &rpcfb.RangeT{End: -1},
				after: []*rpcfb.RangeT{
					{End: -1},
				},
			},
		},
		{
			name: "stream not found (deleted)",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 1, StreamId: 1, Index: 1, Start: 42}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream 1 deleted: stream not found",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
				},
			},
		},
		{
			name: "stream not found (not exist)",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 1, StreamId: 2, Index: 1, Start: 42}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream 2: stream not found",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
				},
			},
		},
		{
			name: "invalid epoch (less)",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 0, Index: 1, Start: 42}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "range 0-1 epoch 0 != 1: invalid stream epoch",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
				},
			},
		},
		{
			name: "invalid epoch (greater)",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 2, Index: 1, Start: 42}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "range 0-1 epoch 2 != 1: invalid stream epoch",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
				},
			},
		},
		{
			name: "create range twice",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{&rpcfb.RangeT{Epoch: 2, Index: 1, Start: 42}},
			want: want{
				returned: &rpcfb.RangeT{Epoch: 2, Index: 1, Start: 42, End: -1},
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "range already exist (different start)",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{&rpcfb.RangeT{Epoch: 2, Index: 1, Start: 420}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "range 0-1: range already created",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid range index",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 1, Index: 2, Start: 42}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "previous range 0-1 not found: invalid range index",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
				},
			},
		},
		{
			name: "create before seal",
			prepare: []preRange{
				{end: 42},
				{index: 1, start: 42, end: -1},
			},
			args: args{&rpcfb.RangeT{Epoch: 2, Index: 2, Start: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeCREATE_RANGE_BEFORE_SEAL,
				errMsg:  "create range 0-2 before sealing the previous range 0-1: create range before sealing the previous one",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
					{Epoch: 2, Index: 1, Start: 42, End: -1},
				},
			},
		},
		{
			name: "invalid start offset",
			prepare: []preRange{
				{end: 42},
			},
			args: args{&rpcfb.RangeT{Epoch: 1, Index: 1, Start: 84}},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "range 0-1 start 84 != 42: invalid range start offset",
				after: []*rpcfb.RangeT{
					{Epoch: 1, End: 42},
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			h, closeFunc := startSbpHandler(t, nil, true)
			defer closeFunc()

			// prepare
			preHeartbeats(t, h, 0, 1, 2)
			streamIDs := preCreateStreams(t, h, 3, 2)
			re.Equal([]int64{0, 1}, streamIDs)
			preDeleteStream(t, h, 1)
			prepareRanges(t, h, 0, tt.prepare)

			// create range
			req := &protocol.CreateRangeRequest{CreateRangeRequestT: rpcfb.CreateRangeRequestT{
				Range: tt.args.r,
			}}
			resp := &protocol.CreateRangeResponse{}
			h.CreateRange(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
				fmtRangeServers(resp.Range)
				fillRangeInfo(tt.want.returned)
				re.Equal(tt.want.returned, resp.Range)
			}

			// list ranges
			lReq := &protocol.ListRangeRequest{ListRangeRequestT: rpcfb.ListRangeRequestT{
				Criteria: &rpcfb.ListRangeCriteriaT{StreamId: 0, ServerId: -1},
			}}
			lResp := &protocol.ListRangeResponse{}
			h.ListRange(lReq, lResp)
			re.Equal(rpcfb.ErrorCodeOK, lResp.Status.Code, lResp.Status.Message)

			// check list range response
			for _, r := range lResp.Ranges {
				fmtRangeServers(r)
			}
			for _, r := range tt.want.after {
				fillRangeInfo(r)
			}
			re.Equal(tt.want.after, lResp.Ranges)
		})
	}
}

// NOT thread safe
func preNewRange(tb testing.TB, h *Handler, streamID int64, sealed bool, length ...int64) (r *rpcfb.RangeT) {
	s := getStream(tb, h, streamID)
	updateStreamEpoch(tb, h, streamID, s.Epoch+1)

	r = createRange(tb, h, streamID)
	if sealed {
		r = sealRange(tb, h, streamID, length[0])
	}

	return
}

func createRange(tb testing.TB, h *Handler, streamID int64) *rpcfb.RangeT {
	re := require.New(tb)

	s := getStream(tb, h, streamID)
	r := getLastRange(tb, h, streamID)
	req := &protocol.CreateRangeRequest{CreateRangeRequestT: rpcfb.CreateRangeRequestT{
		Range: &rpcfb.RangeT{
			StreamId: streamID,
			Epoch:    s.Epoch,
			Index:    r.Index + 1,
			Start:    r.End,
		},
	}}
	resp := &protocol.CreateRangeResponse{}

	h.CreateRange(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)

	return resp.Range
}

func sealRange(tb testing.TB, h *Handler, streamID int64, length int64) *rpcfb.RangeT {
	re := require.New(tb)

	s := getStream(tb, h, streamID)
	r := getLastRange(tb, h, streamID)
	req := &protocol.SealRangeRequest{SealRangeRequestT: rpcfb.SealRangeRequestT{
		Kind: rpcfb.SealKindPLACEMENT_DRIVER,
		Range: &rpcfb.RangeT{
			StreamId: streamID,
			Epoch:    s.Epoch,
			Index:    r.Index,
			Start:    r.Start,
			End:      r.Start + length,
		},
	}}
	resp := &protocol.SealRangeResponse{}

	h.SealRange(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)

	return resp.Range
}

func getLastRange(tb testing.TB, h *Handler, streamID int64) *rpcfb.RangeT {
	re := require.New(tb)

	req := &protocol.ListRangeRequest{ListRangeRequestT: rpcfb.ListRangeRequestT{
		Criteria: &rpcfb.ListRangeCriteriaT{StreamId: streamID, ServerId: -1},
	}}
	resp := &protocol.ListRangeResponse{}
	h.ListRange(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)

	r := &rpcfb.RangeT{StreamId: streamID, Index: -1}
	for _, rr := range resp.Ranges {
		if rr.Index > r.Index {
			r = rr
		}
	}

	return r
}

func getRanges(tb testing.TB, h *Handler, streamID int64) []*rpcfb.RangeT {
	re := require.New(tb)

	// If the stream has been deleted, we can not get ranges by `ListRange`, so we use `ListResource` instead.
	req := &protocol.ListResourceRequest{ListResourceRequestT: rpcfb.ListResourceRequestT{
		ResourceType: []rpcfb.ResourceType{rpcfb.ResourceTypeRESOURCE_RANGE},
	}}
	resp := &protocol.ListResourceResponse{}
	h.ListResource(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)

	ranges := make([]*rpcfb.RangeT, 0, len(resp.Resources))
	for _, r := range resp.Resources {
		if r.Range.StreamId == streamID {
			re.Equal(rpcfb.ResourceTypeRESOURCE_RANGE, r.Type)
			ranges = append(ranges, r.Range)
		}
	}

	return ranges
}

func fmtRangeServers(r *rpcfb.RangeT) {
	// erase offload owner
	r.OffloadOwner = nil

	// erase advertise addr
	for _, s := range r.Servers {
		s.AdvertiseAddr = ""
	}
	// sort by server id
	sort.Slice(r.Servers, func(i, j int) bool {
		return r.Servers[i].ServerId < r.Servers[j].ServerId
	})
}

func fillRangeInfo(r *rpcfb.RangeT) {
	r.Servers = []*rpcfb.RangeServerT{
		{ServerId: 0, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE},
		{ServerId: 1, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE},
		{ServerId: 2, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE},
	}
	r.ReplicaCount = 3
	r.AckCount = 3
}

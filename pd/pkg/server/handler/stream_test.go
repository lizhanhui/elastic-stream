package handler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
)

func TestHandler_CreateStream(t *testing.T) {
	type args struct {
		stream *rpcfb.StreamT
	}
	type want struct {
		stream rpcfb.StreamT

		wantErr bool
		errCode rpcfb.ErrorCode
		errMsg  string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "normal case",
			args: args{
				stream: &rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
			want: want{
				stream: rpcfb.StreamT{StreamId: 1, Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
		},
		{
			name: "nil stream",
			args: args{
				stream: nil,
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "nil stream",
			},
		},
		{
			name: "invalid replica",
			args: args{
				stream: &rpcfb.StreamT{Replica: 0, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid replica",
			},
		},
		{
			name: "invalid ack count",
			args: args{
				stream: &rpcfb.StreamT{Replica: 2, AckCount: 0, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid ack count",
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
			streamIDs := preCreateStreams(t, h, 3, 1)
			re.Equal([]int64{0}, streamIDs)

			// create stream
			req := &protocol.CreateStreamRequest{CreateStreamRequestT: rpcfb.CreateStreamRequestT{
				Stream: tt.args.stream,
			}}
			resp := &protocol.CreateStreamResponse{}
			h.CreateStream(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code)
				re.Equal(tt.want.stream, *resp.Stream)
			}
		})
	}
}

func TestHandler_DeleteStream(t *testing.T) {
	const (
		_streamEpoch = 16
	)
	type args struct {
		streamID int64
		epoch    int64
	}
	type want struct {
		stream rpcfb.StreamT

		wantErr bool
		errCode rpcfb.ErrorCode
		errMsg  string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "normal case",
			args: args{streamID: 1, epoch: _streamEpoch},
			want: want{
				stream: rpcfb.StreamT{StreamId: 1, Replica: 3, AckCount: 3, Epoch: _streamEpoch, Deleted: true},
			},
		},
		{
			name: "delete stream twice",
			args: args{streamID: 0},
			want: want{
				stream: rpcfb.StreamT{StreamId: 0, Replica: 3, AckCount: 3, Deleted: true},
			},
		},
		{
			name: "invalid stream id",
			args: args{streamID: -1},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid stream id",
			},
		},
		{
			name: "stream not found",
			args: args{streamID: 2},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
			},
		},
		{
			name: "invalid epoch (less)",
			args: args{streamID: 1, epoch: _streamEpoch - 1},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "invalid stream epoch",
			},
		},
		{
			name: "invalid epoch (greater)",
			args: args{streamID: 1, epoch: _streamEpoch + 1},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "invalid stream epoch",
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
			preDeleteStream(t, h, 0) // delete stream 0
			prepareRanges(t, h, 1, []preRange{{0, 0, 1}, {1, 1, 2}, {2, 2, -1}})
			updateStreamEpoch(t, h, 1, _streamEpoch)

			// create stream
			req := &protocol.DeleteStreamRequest{DeleteStreamRequestT: rpcfb.DeleteStreamRequestT{
				StreamId: tt.args.streamID,
				Epoch:    tt.args.epoch,
			}}
			resp := &protocol.DeleteStreamResponse{}
			h.DeleteStream(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code)
				re.Equal(tt.want.stream, *resp.Stream)
				re.Empty(getRanges(t, h, tt.args.streamID)) // no ranges in the stream
			}
		})
	}
}

func TestHandler_UpdateStream(t *testing.T) {
	type args struct {
		stream *rpcfb.StreamT
	}
	type want struct {
		stream rpcfb.StreamT
		after  rpcfb.StreamT

		wantErr bool
		errCode rpcfb.ErrorCode
		errMsg  string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "normal case",
			args: args{
				stream: &rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds(), Epoch: 10, StartOffset: -1},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds(), Epoch: 10},
				after:  rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds(), Epoch: 10},
			},
		},
		{
			name: "update replica only",
			args: args{
				stream: &rpcfb.StreamT{Replica: 2, AckCount: -1, RetentionPeriodMs: -1, Epoch: -1, StartOffset: -1},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 2, AckCount: 3},
				after:  rpcfb.StreamT{Replica: 2, AckCount: 3},
			},
		},
		{
			name: "update ack count only",
			args: args{
				stream: &rpcfb.StreamT{Replica: -1, AckCount: 2, RetentionPeriodMs: -1, Epoch: -1, StartOffset: -1},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 3, AckCount: 2},
				after:  rpcfb.StreamT{Replica: 3, AckCount: 2},
			},
		},
		{
			name: "update retention period only",
			args: args{
				stream: &rpcfb.StreamT{Replica: -1, AckCount: -1, RetentionPeriodMs: time.Hour.Milliseconds(), Epoch: -1, StartOffset: -1},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 3, AckCount: 3, RetentionPeriodMs: time.Hour.Milliseconds()},
				after:  rpcfb.StreamT{Replica: 3, AckCount: 3, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
		},
		{
			name: "update epoch only",
			args: args{
				stream: &rpcfb.StreamT{Replica: -1, AckCount: -1, RetentionPeriodMs: -1, Epoch: 10, StartOffset: -1},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 3, AckCount: 3, Epoch: 10},
				after:  rpcfb.StreamT{Replica: 3, AckCount: 3, Epoch: 10},
			},
		},
		{
			name: "nil stream",
			args: args{},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "nil stream",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "invalid stream id",
			args: args{
				stream: &rpcfb.StreamT{StreamId: -1, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid stream id",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "invalid replica",
			args: args{
				stream: &rpcfb.StreamT{Replica: 0, AckCount: 3, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid replica",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "invalid ack count",
			args: args{
				stream: &rpcfb.StreamT{Replica: 3, AckCount: 0, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid ack count",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "do not support update start offset",
			args: args{
				stream: &rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds(), Epoch: 10, StartOffset: 10},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "do not support update start offset",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "no change",
			args: args{
				stream: &rpcfb.StreamT{Replica: -1, AckCount: -1, RetentionPeriodMs: -1, Epoch: -1, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "no change",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "replica < ack count",
			args: args{
				stream: &rpcfb.StreamT{Replica: 1, AckCount: 2, RetentionPeriodMs: -1, Epoch: -1, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid replica 1 < ack count 2",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "stream not found (deleted)",
			args: args{
				stream: &rpcfb.StreamT{StreamId: 1, Replica: 3, AckCount: 3, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "stream not found (not exist)",
			args: args{
				stream: &rpcfb.StreamT{StreamId: 2, Replica: 3, AckCount: 3, StartOffset: -1},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
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

			// update stream
			req := &protocol.UpdateStreamRequest{UpdateStreamRequestT: rpcfb.UpdateStreamRequestT{
				Stream: tt.args.stream,
			}}
			resp := &protocol.UpdateStreamResponse{}
			h.UpdateStream(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code)
				re.Equal(tt.want.stream, *resp.Stream)
			}

			// describe stream and check
			req2 := &protocol.DescribeStreamRequest{DescribeStreamRequestT: rpcfb.DescribeStreamRequestT{
				StreamId: tt.want.after.StreamId,
			}}
			resp2 := &protocol.DescribeStreamResponse{}
			h.DescribeStream(req2, resp2)

			re.Equal(rpcfb.ErrorCodeOK, resp2.Status.Code)
			re.Equal(tt.want.after, *resp2.Stream)
		})
	}
}

func TestHandler_DescribeStream(t *testing.T) {
	type args struct {
		streamID int64
	}
	type want struct {
		stream rpcfb.StreamT

		wantErr bool
		errCode rpcfb.ErrorCode
		errMsg  string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "normal case",
			args: args{
				streamID: 0,
			},
			want: want{
				stream: rpcfb.StreamT{StreamId: 0, Replica: 3, AckCount: 3},
			},
		},
		{
			name: "invalid stream id",
			args: args{
				streamID: -1,
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid stream id",
			},
		},
		{
			name: "stream not found (deleted)",
			args: args{
				streamID: 1,
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
			},
		},
		{
			name: "stream not found (not exist)",
			args: args{
				streamID: 2,
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
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

			// create stream
			req := &protocol.DescribeStreamRequest{DescribeStreamRequestT: rpcfb.DescribeStreamRequestT{
				StreamId: tt.args.streamID,
			}}
			resp := &protocol.DescribeStreamResponse{}
			h.DescribeStream(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code)
				re.Equal(tt.want.stream, *resp.Stream)
			}
		})
	}
}

func TestHandler_TrimStream(t *testing.T) {
	const (
		_streamEpoch  = 16
		_replica      = 3
		_ack          = _replica
		_streamOffset = 10
	)

	type args struct {
		streamID int64
		epoch    int64
		offset   int64
	}
	type want struct {
		s *rpcfb.StreamT
		r *rpcfb.RangeT

		wantErr bool
		errCode rpcfb.ErrorCode
		errMsg  string
	}
	tests := []struct {
		name    string
		prepare []preRange
		args    args
		want    want
		// TODO check after trim
	}{
		{
			name: "trim at a non-sealed range",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 0, epoch: _streamEpoch, offset: 84},
			want: want{
				s: &rpcfb.StreamT{StartOffset: 84},
				r: &rpcfb.RangeT{Epoch: 2, Index: 1, Start: 84, End: -1},
			},
		},
		{
			name: "trim at the middle of a sealed range",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 0, epoch: _streamEpoch, offset: 21},
			want: want{
				s: &rpcfb.StreamT{StartOffset: 21},
				r: &rpcfb.RangeT{Epoch: 1, Index: 0, Start: 21, End: 42},
			},
		},
		{
			name: "trim at the end of a sealed range",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 0, epoch: _streamEpoch, offset: 42},
			want: want{
				s: &rpcfb.StreamT{StartOffset: 42},
				r: &rpcfb.RangeT{Epoch: 2, Index: 1, Start: 42, End: -1},
			},
		},
		{
			name: "trim at the end of a sealed range, and it's the last range",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, 84},
			},
			args: args{streamID: 0, epoch: _streamEpoch, offset: 84},
			want: want{
				s: &rpcfb.StreamT{StartOffset: 84},
				r: &rpcfb.RangeT{Epoch: 2, Index: 1, Start: 84, End: 84},
			},
		},
		{
			name: "stream not found (deleted)",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 1, epoch: _streamEpoch, offset: 84},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
			},
		},
		{
			name: "stream not found (not exist)",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 2, epoch: _streamEpoch, offset: 84},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeNOT_FOUND,
				errMsg:  "stream not found",
			},
		},
		{
			name: "invalid epoch (less)",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 0, epoch: _streamEpoch - 1, offset: 84},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "invalid stream epoch",
			},
		},
		{
			name: "invalid epoch (greater)",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, -1},
			},
			args: args{streamID: 0, epoch: _streamEpoch + 1, offset: 84},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeEXPIRED_STREAM_EPOCH,
				errMsg:  "invalid stream epoch",
			},
		},
		{
			name: "offset too small",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, 84},
			},
			args: args{streamID: 0, epoch: _streamEpoch, offset: 5},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid stream offset",
			},
		},
		{
			name: "offset too large",
			prepare: []preRange{
				{0, 0, 42},
				{1, 42, 84},
			},
			args: args{streamID: 0, epoch: _streamEpoch, offset: 168},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid offset",
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
			streamIDs := preCreateStreams(t, h, _replica, 2)
			re.Equal([]int64{0, 1}, streamIDs)
			preDeleteStream(t, h, 1)
			prepareRanges(t, h, 0, tt.prepare)
			trimStream(t, h, 0, _streamOffset)
			updateStreamEpoch(t, h, 0, _streamEpoch)

			// common want
			if tt.want.s != nil {
				tt.want.s.Epoch = _streamEpoch
				tt.want.s.Replica = _replica
				tt.want.s.AckCount = _ack
			}
			if tt.want.r != nil {
				tt.want.r.ReplicaCount = _replica
				tt.want.r.AckCount = _ack
			}

			// trim stream
			req := &protocol.TrimStreamRequest{TrimStreamRequestT: rpcfb.TrimStreamRequestT{
				StreamId:  tt.args.streamID,
				Epoch:     tt.args.epoch,
				MinOffset: tt.args.offset,
			}}
			resp := &protocol.TrimStreamResponse{}
			h.TrimStream(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code)
				re.Equal(tt.want.s, resp.Stream)
				fmtRangeServers(resp.Range)
				fillRangeInfo(tt.want.r)
				re.Equal(tt.want.r, resp.Range)
			}
		})
	}
}

func getStream(tb testing.TB, h *Handler, streamID int64) *rpcfb.StreamT {
	re := require.New(tb)

	req := &protocol.DescribeStreamRequest{DescribeStreamRequestT: rpcfb.DescribeStreamRequestT{
		StreamId: streamID,
	}}
	resp := &protocol.DescribeStreamResponse{}
	h.DescribeStream(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)

	return resp.Stream
}

func updateStreamEpoch(tb testing.TB, h *Handler, streamID int64, epoch int64) {
	re := require.New(tb)

	req := &protocol.UpdateStreamRequest{UpdateStreamRequestT: rpcfb.UpdateStreamRequestT{
		Stream: &rpcfb.StreamT{
			StreamId:          streamID,
			Replica:           -1,
			AckCount:          -1,
			RetentionPeriodMs: -1,
			StartOffset:       -1,
			Epoch:             epoch,
		},
	}}
	resp := &protocol.UpdateStreamResponse{}
	h.UpdateStream(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
	re.Equal(epoch, resp.Stream.Epoch)
}

func trimStream(tb testing.TB, h *Handler, streamID int64, offset int64) {
	re := require.New(tb)

	s := getStream(tb, h, streamID)
	req := &protocol.TrimStreamRequest{TrimStreamRequestT: rpcfb.TrimStreamRequestT{
		StreamId:  streamID,
		Epoch:     s.Epoch,
		MinOffset: offset,
	}}
	resp := &protocol.TrimStreamResponse{}
	h.TrimStream(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
	re.Equal(offset, resp.Stream.StartOffset)
}

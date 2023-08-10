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
				stream: &rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds()},
				after:  rpcfb.StreamT{Replica: 2, AckCount: 2, RetentionPeriodMs: time.Hour.Milliseconds()},
			},
		},
		{
			name: "do not update start offset or epoch",
			args: args{
				stream: &rpcfb.StreamT{Replica: 3, AckCount: 3, StartOffset: 10, Epoch: 20},
			},
			want: want{
				stream: rpcfb.StreamT{Replica: 3, AckCount: 3},
				after:  rpcfb.StreamT{Replica: 3, AckCount: 3},
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
				stream: &rpcfb.StreamT{StreamId: -1},
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
				stream: &rpcfb.StreamT{Replica: 0, AckCount: 3},
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
				stream: &rpcfb.StreamT{Replica: 3, AckCount: 0},
			},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeBAD_REQUEST,
				errMsg:  "invalid ack count",
				after:   rpcfb.StreamT{Replica: 3, AckCount: 3},
			},
		},
		{
			name: "stream not found",
			args: args{
				stream: &rpcfb.StreamT{StreamId: 1, Replica: 3, AckCount: 3},
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
			streamIDs := preCreateStreams(t, h, 3, 1)
			re.Equal([]int64{0}, streamIDs)

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
			name: "stream not found",
			args: args{
				streamID: 1,
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
			streamIDs := preCreateStreams(t, h, 3, 1)
			re.Equal([]int64{0}, streamIDs)

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

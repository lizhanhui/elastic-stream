package handler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
)

func TestHandler_CommitObject(t *testing.T) {
	type args struct {
		streamID   int64
		rangeIndex int32
	}
	type want struct {
		returned endpoint.Object
		wantErr  bool
		errCode  rpcfb.ErrorCode
		errMsg   string
		after    []endpoint.Object
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
			args: args{},
			want: want{
				returned: endpoint.Object{ObjT: &rpcfb.ObjT{}},
				after:    []endpoint.Object{{ObjT: &rpcfb.ObjT{}}},
			},
		},
		{
			name: "range not found",
			prepare: []preRange{
				{end: 42},
			},
			args: args{streamID: 0, rangeIndex: 1},
			want: want{
				wantErr: true,
				errCode: rpcfb.ErrorCodeRANGE_NOT_FOUND,
				errMsg:  "range not found",
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
			prepareRanges(t, h, 0, tt.prepare)

			// commit object
			req := &protocol.CommitObjectRequest{CommitObjectRequestT: rpcfb.CommitObjectRequestT{
				Object: &rpcfb.ObjT{StreamId: tt.args.streamID, RangeIndex: tt.args.rangeIndex},
			}}
			resp := &protocol.CommitObjectResponse{}
			h.CommitObject(req, resp)

			// check response
			if tt.want.wantErr {
				re.Equal(tt.want.errCode, resp.Status.Code)
				re.Contains(resp.Status.Message, tt.want.errMsg)
			} else {
				re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code)
			}

			// list objects and check
			// TODO: there's no list object rpc yet, check it later
		})
	}
}

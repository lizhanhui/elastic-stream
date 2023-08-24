package protocol

import (
	"reflect"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/codec"
	fbutil "github.com/AutoMQ/pd/pkg/util/flatbuffer"
)

type packableResponse interface {
	OutResponse
	fbutil.Packable
}

var _outResponses = []packableResponse{
	&CommitObjectResponse{},
	&CreateRangeResponse{},
	&CreateStreamResponse{},
	&DeleteStreamResponse{},
	&DescribePDClusterResponse{},
	&DescribeStreamResponse{},
	&TrimStreamResponse{},
	&HeartbeatResponse{},
	&IDAllocationResponse{},
	&ListRangeResponse{},
	&ListResourceResponse{},
	&ReportMetricsResponse{},
	&SealRangeResponse{},
	&SystemErrorResponse{},
	&UpdateStreamResponse{},
	&WatchResourceResponse{},
}

func TestListRangeResponse_Marshal(t *testing.T) {
	var mockListRangeResponse ListRangeResponse
	_ = gofakeit.Struct(&mockListRangeResponse)
	mockData := fbutil.Marshal(&mockListRangeResponse.ListRangeResponseT)
	tests := []struct {
		name    string
		resp    ListRangeResponse
		fmt     codec.Format
		want    []byte
		wantErr bool
		errMsg  string
	}{
		{
			name: "FlatBuffer",
			resp: mockListRangeResponse,
			fmt:  codec.FormatFlatBuffer,
			want: mockData,
		},
		{
			name:    "ProtoBuffer",
			resp:    mockListRangeResponse,
			fmt:     codec.FormatProtoBuffer,
			wantErr: true,
			errMsg:  "unsupported format",
		},
		{
			name:    "JSON",
			resp:    mockListRangeResponse,
			fmt:     codec.FormatJSON,
			wantErr: true,
			errMsg:  "unsupported format",
		},
		{
			name:    "Unknown",
			resp:    mockListRangeResponse,
			fmt:     codec.Format(0),
			wantErr: true,
			errMsg:  "unsupported format",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			got, err := tt.resp.Marshal(tt.fmt)
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, got)
			}
		})
	}
}

func TestOutResponse(t *testing.T) {
	for _, resp := range _outResponses {
		resp := resp
		t.Run(reflect.TypeOf(resp).String(), func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			// mock
			mockResp := reflect.New(reflect.TypeOf(resp).Elem()).Interface().(packableResponse)
			err := gofakeit.Struct(mockResp)
			re.NoError(err)
			mockData := fbutil.Marshal(mockResp)

			// check Marshal
			data, err := mockResp.Marshal(codec.FormatFlatBuffer)
			re.NoError(err)
			re.Equal(mockData, data)

			// check Error
			errStatus := rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: "test error message"}
			mockResp.Error(&errStatus)
			status := reflect.ValueOf(mockResp).Elem().FieldByName("Status").Interface().(*rpcfb.StatusT)
			re.Equal(errStatus, *status)

			// check OK
			mockResp.OK()
			status = reflect.ValueOf(mockResp).Elem().FieldByName("Status").Interface().(*rpcfb.StatusT)
			re.Equal(rpcfb.StatusT{Code: rpcfb.ErrorCodeOK}, *status)

			// check IsEnd
			// currently, all responses are single response
			re.True(mockResp.IsEnd())
		})
	}
}

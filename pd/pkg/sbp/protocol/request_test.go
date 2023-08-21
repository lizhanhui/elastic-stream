package protocol

import (
	"reflect"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/pkg/sbp/codec"
	"github.com/AutoMQ/pd/pkg/util/fbutil"
)

type packableRequest interface {
	InRequest
	fbutil.Packable
}

var _inRequests = []packableRequest{
	&CommitObjectRequest{},
	&CreateRangeRequest{},
	&CreateStreamRequest{},
	&DeleteStreamRequest{},
	&DescribePDClusterRequest{},
	&DescribeStreamRequest{},
	&TrimStreamRequest{},
	&HeartbeatRequest{},
	&IDAllocationRequest{},
	&ListRangeRequest{},
	&ListResourceRequest{},
	&ReportMetricsRequest{},
	&SealRangeRequest{},
	&UpdateStreamRequest{},
	&WatchResourceRequest{},
}

func TestListRangeRequest_Unmarshal(t *testing.T) {
	var mockListRangeRequest ListRangeRequest
	_ = gofakeit.Struct(&mockListRangeRequest)
	mockData := fbutil.Marshal(&mockListRangeRequest)

	type args struct {
		fmt  codec.Format
		data []byte
	}
	tests := []struct {
		name    string
		args    args
		want    ListRangeRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "FlatBuffer",
			args: args{
				fmt:  codec.FormatFlatBuffer,
				data: mockData,
			},
			want: mockListRangeRequest,
		},
		{
			name: "FlatBuffer in wrong format",
			args: args{
				fmt:  codec.FormatFlatBuffer,
				data: []byte{'a', 'b', 'c'},
			},
			wantErr: true,
			errMsg:  "unmarshal FlatBuffer:",
		},
		{
			name: "ProtoBuffer",
			args: args{
				fmt:  codec.FormatProtoBuffer,
				data: []byte{},
			},
			wantErr: true,
			errMsg:  "unsupported format",
		},
		{
			name: "JSON",
			args: args{
				fmt:  codec.FormatJSON,
				data: []byte{},
			},
			wantErr: true,
			errMsg:  "unsupported format",
		},
		{
			name: "Unknown",
			args: args{
				fmt:  codec.Format(0),
				data: []byte{},
			},
			wantErr: true,
			errMsg:  "unsupported format",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			l := ListRangeRequest{}
			err := l.Unmarshal(tt.args.fmt, tt.args.data)
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want, l)
			}
		})
	}
}

func TestAll_Unmarshal(t *testing.T) {
	for _, req := range _inRequests {
		req := req
		t.Run(reflect.TypeOf(req).String(), func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			// mock
			mockReq := reflect.New(reflect.TypeOf(req).Elem()).Interface().(packableRequest)
			err := gofakeit.Struct(mockReq)
			re.NoError(err)
			data := fbutil.Marshal(mockReq)

			newReq := reflect.New(reflect.TypeOf(req).Elem()).Interface().(packableRequest)
			err = newReq.Unmarshal(codec.FormatFlatBuffer, data)
			re.NoError(err)
			re.Equal(mockReq, newReq)
		})
	}
}

func TestAll_Timeout(t *testing.T) {
	noTimeoutReqs := mapset.NewThreadUnsafeSet(
		reflect.TypeOf(&ReportMetricsRequest{}).String(),
		reflect.TypeOf(&HeartbeatRequest{}).String(),
	)

	for _, req := range _inRequests {
		req := req
		t.Run(reflect.TypeOf(req).String(), func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			// mock
			err := gofakeit.Struct(req)
			re.NoError(err)
			if noTimeoutReqs.Contains(reflect.TypeOf(req).String()) {
				re.Zero(req.Timeout())
			} else {
				re.NotZero(req.Timeout())
			}
		})
	}
}

func TestAll_LongPoll(t *testing.T) {
	longPollReqs := mapset.NewThreadUnsafeSet(
		reflect.TypeOf(&WatchResourceRequest{}).String(),
	)

	for _, req := range _inRequests {
		req := req
		t.Run(reflect.TypeOf(req).String(), func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			// mock
			err := gofakeit.Struct(req)
			re.NoError(err)
			if longPollReqs.Contains(reflect.TypeOf(req).String()) {
				re.True(req.LongPoll())
			} else {
				re.False(req.LongPoll())
			}
		})
	}
}

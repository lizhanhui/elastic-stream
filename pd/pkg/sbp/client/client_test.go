package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	"github.com/AutoMQ/pd/pkg/sbp/server"
	"github.com/AutoMQ/pd/pkg/server/config"
	tempurl "github.com/AutoMQ/pd/pkg/util/test/url"
)

type timeoutHandler struct {
	mockHandler
	UsedTime time.Duration
}

func (th *timeoutHandler) Heartbeat(_ *protocol.HeartbeatRequest, resp *protocol.HeartbeatResponse) {
	time.Sleep(th.UsedTime)
	resp.OK()
}

type timeoutRequest struct {
	protocol.HeartbeatRequest
	RequestTimeout time.Duration
}

func (tr *timeoutRequest) Timeout() int32 {
	return int32(tr.RequestTimeout / time.Millisecond)
}

func TestClientTimeout(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	re := require.New(t)

	var used = 40 * time.Millisecond
	var timeout = 20 * time.Millisecond

	addr, shutdown := startServer(t, &timeoutHandler{UsedTime: used}, logger)
	defer shutdown()

	client := NewClient(&config.SbpClient{}, logger)
	defer client.Shutdown(context.Background())

	req := &timeoutRequest{RequestTimeout: timeout}
	now := time.Now()
	_, err := client.Do(context.Background(), req, addr)
	cost := time.Since(now)
	re.True(errors.Is(err, context.DeadlineExceeded))
	re.Greater(cost, timeout)
	re.Less(cost, used)
}

var (
	_okRespFunc = func(_ protocol.InRequest, resp protocol.OutResponse) {
		resp.OK()
	}
	_timeoutRespFunc = func(_ protocol.InRequest, resp protocol.OutResponse) {
		time.Sleep(40 * time.Millisecond)
		resp.OK()
	}
	_panicRespFunc = func(_ protocol.InRequest, _ protocol.OutResponse) {
		panic("test system error")
	}
)

func TestSbpClient_SealRange(t *testing.T) {
	type fields struct {
		handler server.Handler
	}
	type args struct {
		req *protocol.SealRangeRequest
	}
	type want struct {
		resp *protocol.SealRangeResponse

		wantErr bool
		errMsg  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name:   "normal case",
			fields: fields{&mockHandler{_okRespFunc}},
			args:   args{&protocol.SealRangeRequest{SealRangeRequestT: rpcfb.SealRangeRequestT{}}},
			want: want{resp: &protocol.SealRangeResponse{SealRangeResponseT: rpcfb.SealRangeResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
			}}},
		},
		{
			name:   "timeout",
			fields: fields{&mockHandler{_timeoutRespFunc}},
			args: args{&protocol.SealRangeRequest{SealRangeRequestT: rpcfb.SealRangeRequestT{
				TimeoutMs: 20,
			}}},
			want: want{wantErr: true, errMsg: "context deadline exceeded"},
		},
		{
			name:   "handler panic",
			fields: fields{&mockHandler{_panicRespFunc}},
			args:   args{&protocol.SealRangeRequest{SealRangeRequestT: rpcfb.SealRangeRequestT{}}},
			want: want{resp: &protocol.SealRangeResponse{SealRangeResponseT: rpcfb.SealRangeResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: "handler panic"},
			}}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			addr, shutdown := startServer(t, tt.fields.handler, zap.NewNop())
			defer shutdown()

			client := NewClient(&config.SbpClient{}, zap.NewNop())
			defer client.Shutdown(context.Background())

			resp, err := client.SealRange(context.Background(), tt.args.req, addr)
			if tt.want.wantErr {
				re.Error(err)
				re.Contains(err.Error(), tt.want.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want.resp, resp)
			}
		})
	}
}

func TestSbpClient_CreateRange(t *testing.T) {
	type fields struct {
		handler server.Handler
	}
	type args struct {
		req *protocol.CreateRangeRequest
	}
	type want struct {
		resp *protocol.CreateRangeResponse

		wantErr bool
		errMsg  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name:   "normal case",
			fields: fields{&mockHandler{_okRespFunc}},
			args:   args{&protocol.CreateRangeRequest{CreateRangeRequestT: rpcfb.CreateRangeRequestT{}}},
			want: want{resp: &protocol.CreateRangeResponse{CreateRangeResponseT: rpcfb.CreateRangeResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
			}}},
		},
		{
			name:   "timeout",
			fields: fields{&mockHandler{_timeoutRespFunc}},
			args: args{&protocol.CreateRangeRequest{CreateRangeRequestT: rpcfb.CreateRangeRequestT{
				TimeoutMs: 20,
			}}},
			want: want{wantErr: true, errMsg: "context deadline exceeded"},
		},
		{
			name:   "handler panic",
			fields: fields{&mockHandler{_panicRespFunc}},
			args:   args{&protocol.CreateRangeRequest{CreateRangeRequestT: rpcfb.CreateRangeRequestT{}}},
			want: want{resp: &protocol.CreateRangeResponse{CreateRangeResponseT: rpcfb.CreateRangeResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: "handler panic"},
			}}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			addr, shutdown := startServer(t, tt.fields.handler, zap.NewNop())
			defer shutdown()

			client := NewClient(&config.SbpClient{}, zap.NewNop())
			defer client.Shutdown(context.Background())

			resp, err := client.CreateRange(context.Background(), tt.args.req, addr)
			if tt.want.wantErr {
				re.Error(err)
				re.Contains(err.Error(), tt.want.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want.resp, resp)
			}
		})
	}
}

func TestSbpClient_CreateStream(t *testing.T) {
	type fields struct {
		handler server.Handler
	}
	type args struct {
		req *protocol.CreateStreamRequest
	}
	type want struct {
		resp *protocol.CreateStreamResponse

		wantErr bool
		errMsg  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name:   "normal case",
			fields: fields{&mockHandler{_okRespFunc}},
			args:   args{&protocol.CreateStreamRequest{CreateStreamRequestT: rpcfb.CreateStreamRequestT{}}},
			want: want{resp: &protocol.CreateStreamResponse{CreateStreamResponseT: rpcfb.CreateStreamResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
			}}},
		},
		{
			name:   "timeout",
			fields: fields{&mockHandler{_timeoutRespFunc}},
			args: args{&protocol.CreateStreamRequest{CreateStreamRequestT: rpcfb.CreateStreamRequestT{
				TimeoutMs: 20,
			}}},
			want: want{wantErr: true, errMsg: "context deadline exceeded"},
		},
		{
			name:   "handler panic",
			fields: fields{&mockHandler{_panicRespFunc}},
			args:   args{&protocol.CreateStreamRequest{CreateStreamRequestT: rpcfb.CreateStreamRequestT{}}},
			want: want{resp: &protocol.CreateStreamResponse{CreateStreamResponseT: rpcfb.CreateStreamResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: "handler panic"},
			}}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			addr, shutdown := startServer(t, tt.fields.handler, zap.NewNop())
			defer shutdown()

			client := NewClient(&config.SbpClient{}, zap.NewNop())
			defer client.Shutdown(context.Background())

			resp, err := client.CreateStream(context.Background(), tt.args.req, addr)
			if tt.want.wantErr {
				re.Error(err)
				re.Contains(err.Error(), tt.want.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want.resp, resp)
			}
		})
	}
}

func TestSbpClient_ReportMetrics(t *testing.T) {
	type fields struct {
		handler server.Handler
	}
	type args struct {
		req *protocol.ReportMetricsRequest
	}
	type want struct {
		resp *protocol.ReportMetricsResponse

		wantErr bool
		errMsg  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name:   "normal case",
			fields: fields{&mockHandler{_okRespFunc}},
			args:   args{&protocol.ReportMetricsRequest{ReportMetricsRequestT: rpcfb.ReportMetricsRequestT{}}},
			want: want{resp: &protocol.ReportMetricsResponse{ReportMetricsResponseT: rpcfb.ReportMetricsResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
			}}},
		},
		{
			name:   "handler panic",
			fields: fields{&mockHandler{_panicRespFunc}},
			args:   args{&protocol.ReportMetricsRequest{ReportMetricsRequestT: rpcfb.ReportMetricsRequestT{}}},
			want: want{resp: &protocol.ReportMetricsResponse{ReportMetricsResponseT: rpcfb.ReportMetricsResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: "handler panic"},
			}}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			addr, shutdown := startServer(t, tt.fields.handler, zap.NewNop())
			defer shutdown()

			client := NewClient(&config.SbpClient{}, zap.NewNop())
			defer client.Shutdown(context.Background())

			resp, err := client.ReportMetrics(context.Background(), tt.args.req, addr)
			if tt.want.wantErr {
				re.Error(err)
				re.Contains(err.Error(), tt.want.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want.resp, resp)
			}
		})
	}
}

func TestSbpClient_CommitObject(t *testing.T) {
	type fields struct {
		handler server.Handler
	}
	type args struct {
		req *protocol.CommitObjectRequest
	}
	type want struct {
		resp *protocol.CommitObjectResponse

		wantErr bool
		errMsg  string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   want
	}{
		{
			name:   "normal case",
			fields: fields{&mockHandler{_okRespFunc}},
			args:   args{&protocol.CommitObjectRequest{CommitObjectRequestT: rpcfb.CommitObjectRequestT{}}},
			want: want{resp: &protocol.CommitObjectResponse{CommitObjectResponseT: rpcfb.CommitObjectResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
			}}},
		},
		{
			name:   "handler panic",
			fields: fields{&mockHandler{_panicRespFunc}},
			args:   args{&protocol.CommitObjectRequest{CommitObjectRequestT: rpcfb.CommitObjectRequestT{}}},
			want: want{resp: &protocol.CommitObjectResponse{CommitObjectResponseT: rpcfb.CommitObjectResponseT{
				Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodePD_INTERNAL_SERVER_ERROR, Message: "handler panic"},
			}}},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			addr, shutdown := startServer(t, tt.fields.handler, zap.NewNop())
			defer shutdown()

			client := NewClient(&config.SbpClient{}, zap.NewNop())
			defer client.Shutdown(context.Background())

			resp, err := client.CommitObject(context.Background(), tt.args.req, addr)
			if tt.want.wantErr {
				re.Error(err)
				re.Contains(err.Error(), tt.want.errMsg)
			} else {
				re.NoError(err)
				re.Equal(tt.want.resp, resp)
			}
		})
	}
}

func startServer(tb testing.TB, handler server.Handler, lg *zap.Logger) (addr string, shutdown func()) {
	re := require.New(tb)

	addr = tempurl.AllocAddr(tb)
	listener, err := net.Listen("tcp", addr)
	re.NoError(err)

	s := server.NewServer(context.Background(), &config.SbpServer{}, handler, lg)
	go func() {
		_ = s.Serve(listener)
	}()

	shutdown = func() {
		_ = s.Shutdown(context.Background())
	}
	return
}

type mockHandler struct {
	f func(req protocol.InRequest, resp protocol.OutResponse)
}

func (b *mockHandler) Heartbeat(req *protocol.HeartbeatRequest, resp *protocol.HeartbeatResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) AllocateID(req *protocol.IDAllocationRequest, resp *protocol.IDAllocationResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) SealRange(req *protocol.SealRangeRequest, resp *protocol.SealRangeResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) CreateRange(req *protocol.CreateRangeRequest, resp *protocol.CreateRangeResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) ListRange(req *protocol.ListRangeRequest, resp *protocol.ListRangeResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) CreateStream(req *protocol.CreateStreamRequest, resp *protocol.CreateStreamResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) DeleteStream(req *protocol.DeleteStreamRequest, resp *protocol.DeleteStreamResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) UpdateStream(req *protocol.UpdateStreamRequest, resp *protocol.UpdateStreamResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) DescribeStream(req *protocol.DescribeStreamRequest, resp *protocol.DescribeStreamResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) TrimStream(req *protocol.TrimStreamRequest, resp *protocol.TrimStreamResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) ReportMetrics(req *protocol.ReportMetricsRequest, resp *protocol.ReportMetricsResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) DescribePDCluster(req *protocol.DescribePDClusterRequest, resp *protocol.DescribePDClusterResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) CommitObject(req *protocol.CommitObjectRequest, resp *protocol.CommitObjectResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) ListResource(req *protocol.ListResourceRequest, resp *protocol.ListResourceResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

func (b *mockHandler) WatchResource(req *protocol.WatchResourceRequest, resp *protocol.WatchResourceResponse) {
	if b.f != nil {
		b.f(req, resp)
	}
}

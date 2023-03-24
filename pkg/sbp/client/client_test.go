package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/sbp/protocol"
	"github.com/AutoMQ/placement-manager/pkg/sbp/server"
	tempurl "github.com/AutoMQ/placement-manager/pkg/util/testutil/url"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestClient_Do(t *testing.T) {
	t.Parallel()
	var logger *zap.Logger
	logger, _ = zap.NewDevelopment()
	//logger = zap.NewNop()
	re := require.New(t)

	addr, shutdown := startServer(t, &mockHandler{}, logger)
	defer shutdown()

	client := NewClient("test", logger)
	defer client.CloseIdleConnections()
	client.IdleConnTimeout = time.Second

	req := &protocol.SealRangesRequest{
		SealRangesRequestT: rpcfb.SealRangesRequestT{
			Ranges: []*rpcfb.RangeIdT{
				{
					StreamId:   1,
					RangeIndex: 2,
				},
			},
		},
	}
	resp, err := client.Do(req, addr)
	re.NoError(err)
	_ = resp
}

func startServer(tb testing.TB, handler server.Handler, lg *zap.Logger) (addr string, shutdown func()) {
	re := require.New(tb)

	addr = tempurl.AllocAddr(tb)
	listener, err := net.Listen("tcp", addr)
	re.NoError(err)

	s := server.NewServer(context.Background(), handler, lg)
	go func() {
		_ = s.Serve(listener)
	}()

	shutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}
	return
}

type mockHandler struct {
	baseHandler
}

func (m *mockHandler) SealRanges(req *protocol.SealRangesRequest, resp *protocol.SealRangesResponse) {
	results := make([]*rpcfb.SealRangesResultT, 0, len(req.Ranges))
	for _, r := range req.Ranges {
		results = append(results, &rpcfb.SealRangesResultT{
			StreamId: r.StreamId,
			Range: &rpcfb.RangeT{
				StreamId:    r.StreamId,
				RangeIndex:  r.RangeIndex,
				StartOffset: 1024,
				EndOffset:   2048,
			},
			Status: &rpcfb.StatusT{Code: rpcfb.ErrorCodeOK},
		})
	}
	resp.SealRangesResponseT = rpcfb.SealRangesResponseT{
		SealResponses: results,
	}
	resp.OK()
}

type baseHandler struct{}

func (b *baseHandler) Heartbeat(_ *protocol.HeartbeatRequest, resp *protocol.HeartbeatResponse) {
	resp.OK()
}

func (b *baseHandler) SealRanges(_ *protocol.SealRangesRequest, resp *protocol.SealRangesResponse) {
	resp.OK()
}

func (b *baseHandler) ListRanges(_ *protocol.ListRangesRequest, resp *protocol.ListRangesResponse) {
	resp.OK()
}

func (b *baseHandler) CreateStreams(_ *protocol.CreateStreamsRequest, resp *protocol.CreateStreamsResponse) {
	resp.OK()
}

func (b *baseHandler) DeleteStreams(_ *protocol.DeleteStreamsRequest, resp *protocol.DeleteStreamsResponse) {
	resp.OK()
}

func (b *baseHandler) UpdateStreams(_ *protocol.UpdateStreamsRequest, resp *protocol.UpdateStreamsResponse) {
	resp.OK()
}

func (b *baseHandler) DescribeStreams(_ *protocol.DescribeStreamsRequest, resp *protocol.DescribeStreamsResponse) {
	resp.OK()
}

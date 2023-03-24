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
	re := require.New(t)

	addr, shutdown := startServer(t, &mockHandler{})
	defer shutdown()

	client := NewClient("test", zap.NewNop())
	defer client.CloseIdleConnections()

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

func startServer(tb testing.TB, handler server.Handler) (addr string, shutdown func()) {
	re := require.New(tb)

	addr = tempurl.AllocAddr(tb)
	listener, err := net.Listen("tcp", addr)
	re.NoError(err)

	s := server.NewServer(context.Background(), handler, zap.NewNop())
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

type baseHandler struct{}

func (b *baseHandler) Heartbeat(_ *protocol.HeartbeatRequest, _ *protocol.HeartbeatResponse) {
}
func (b *baseHandler) SealRanges(_ *protocol.SealRangesRequest, _ *protocol.SealRangesResponse) {
}
func (b *baseHandler) ListRanges(_ *protocol.ListRangesRequest, _ *protocol.ListRangesResponse) {
}
func (b *baseHandler) CreateStreams(_ *protocol.CreateStreamsRequest, _ *protocol.CreateStreamsResponse) {
}
func (b *baseHandler) DeleteStreams(_ *protocol.DeleteStreamsRequest, _ *protocol.DeleteStreamsResponse) {
}
func (b *baseHandler) UpdateStreams(_ *protocol.UpdateStreamsRequest, _ *protocol.UpdateStreamsResponse) {
}
func (b *baseHandler) DescribeStreams(_ *protocol.DescribeStreamsRequest, _ *protocol.DescribeStreamsResponse) {
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

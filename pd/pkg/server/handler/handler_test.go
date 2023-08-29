package handler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	sbpClient "github.com/AutoMQ/pd/pkg/sbp/client"
	"github.com/AutoMQ/pd/pkg/sbp/protocol"
	sbpServer "github.com/AutoMQ/pd/pkg/sbp/server"
	"github.com/AutoMQ/pd/pkg/server/cluster"
	"github.com/AutoMQ/pd/pkg/server/config"
	"github.com/AutoMQ/pd/pkg/server/id"
	"github.com/AutoMQ/pd/pkg/server/member"
	"github.com/AutoMQ/pd/pkg/server/storage"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	testutil "github.com/AutoMQ/pd/pkg/util/test"
)

type mockServer struct {
	client    kv.Client
	sbpClient sbpClient.Client
}

func (m *mockServer) Storage() storage.Storage {
	etcdKV := kv.NewEtcd(kv.EtcdParam{
		Client:   m.client,
		RootPath: "/test-server",
		CmpFunc:  func() clientv3.Cmp { return clientv3.Compare(clientv3.CreateRevision("not-exist-key"), "=", 0) },
	}, zap.NewNop())
	return endpoint.NewEndpoint(etcdKV, zap.NewNop())
}

func (m *mockServer) IDAllocator(key string, start, step uint64) id.Allocator {
	return id.NewEtcdAllocator(&id.EtcdAllocatorParam{
		KV:       m.client,
		CmpFunc:  func() clientv3.Cmp { return clientv3.Compare(clientv3.CreateRevision("not-exist-key"), "=", 0) },
		RootPath: "/test-server",
		Key:      key,
		Start:    start,
		Step:     step,
	}, zap.NewNop())
}

func (m *mockServer) Member() cluster.MemberService {
	return m
}

func (m *mockServer) IsLeader() bool {
	return true
}

func (m *mockServer) ClusterInfo(_ context.Context) ([]*member.Info, error) {
	return []*member.Info{{
		Name:            "test-member-name",
		MemberID:        1,
		PeerUrls:        []string{"test-member-peer-urls"},
		ClientUrls:      []string{"test-member-client-urls"},
		AdvertisePDAddr: "test-member-sbp-addr",
		IsLeader:        true,
	}}, nil
}

func (m *mockServer) SbpClient() sbpClient.Client {
	return m.sbpClient
}

type mockServerNotLeader struct {
	cluster.Server
}

func (m *mockServerNotLeader) IsLeader() bool {
	return false
}

func startSbpHandler(tb testing.TB, sc sbpClient.Client, clusterCfg *config.Cluster, isLeader bool) (sbpServer.Handler, func()) {
	re := require.New(tb)

	if sc == nil {
		mockCtrl := gomock.NewController(tb)
		sc = sbpClient.NewMockClient(mockCtrl)
	}
	if clusterCfg == nil {
		clusterCfg = config.DefaultCluster()
	}

	_, client, closeFunc := testutil.StartEtcd(tb, nil)

	var server cluster.Server
	server = &mockServer{client: client, sbpClient: sc}
	if !isLeader {
		server = &mockServerNotLeader{server}
	}

	c := cluster.NewRaftCluster(context.Background(), clusterCfg, server.Member(), zap.NewNop())
	err := c.Start(server)
	re.NoError(err)

	h := timeoutHandler{
		handler: NewHandler(c, zap.NewNop()),
		timeout: time.Second,
	}

	return h, func() { _ = c.Stop(); closeFunc() }
}

func preHeartbeats(tb testing.TB, h sbpServer.Handler, serverIDs ...int32) {
	for _, serverID := range serverIDs {
		preHeartbeat(tb, h, serverID)
	}
}

func preHeartbeat(tb testing.TB, h sbpServer.Handler, serverID int32) {
	re := require.New(tb)

	req := &protocol.HeartbeatRequest{HeartbeatRequestT: rpcfb.HeartbeatRequestT{
		ClientRole: rpcfb.ClientRoleCLIENT_ROLE_RANGE_SERVER,
		RangeServer: &rpcfb.RangeServerT{
			ServerId:      serverID,
			AdvertiseAddr: fmt.Sprintf("addr-%d", serverID),
			State:         rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE,
		}}}
	resp := &protocol.HeartbeatResponse{}

	h.Heartbeat(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
}

func preCreateStreams(tb testing.TB, h sbpServer.Handler, replica int8, cnt int) (streamIDs []int64) {
	streamIDs = make([]int64, 0, cnt)
	for i := 0; i < cnt; i++ {
		stream := preCreateStream(tb, h, replica)
		streamIDs = append(streamIDs, stream.StreamId)
	}
	return
}

func preCreateStream(tb testing.TB, h sbpServer.Handler, replica int8) *rpcfb.StreamT {
	re := require.New(tb)

	req := &protocol.CreateStreamRequest{CreateStreamRequestT: rpcfb.CreateStreamRequestT{
		Stream: &rpcfb.StreamT{
			Replica:  replica,
			AckCount: replica,
		},
	}}
	resp := &protocol.CreateStreamResponse{}

	h.CreateStream(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)

	return resp.Stream
}

func preDeleteStream(tb testing.TB, h sbpServer.Handler, streamID int64) {
	re := require.New(tb)

	req := &protocol.DeleteStreamRequest{DeleteStreamRequestT: rpcfb.DeleteStreamRequestT{
		StreamId: streamID,
	}}
	resp := &protocol.DeleteStreamResponse{}

	h.DeleteStream(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
}

type preObject struct {
	streamID    int64
	rangeIndex  int32
	epoch       int16
	startOffset int64
	endOffset   int64
	dataLen     int32
}

func preNewObject(tb testing.TB, h sbpServer.Handler, object preObject) {
	re := require.New(tb)

	req := &protocol.CommitObjectRequest{CommitObjectRequestT: rpcfb.CommitObjectRequestT{Object: &rpcfb.ObjT{
		StreamId:       object.streamID,
		RangeIndex:     object.rangeIndex,
		Epoch:          object.epoch,
		StartOffset:    object.startOffset,
		EndOffsetDelta: int32(object.endOffset - object.startOffset),
		DataLen:        object.dataLen,
	}}}
	resp := &protocol.CommitObjectResponse{}

	h.CommitObject(req, resp)
	re.Equal(rpcfb.ErrorCodeOK, resp.Status.Code, resp.Status.Message)
}

func timeoutReq(req protocol.InRequest, timeout time.Duration) context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req.SetContext(ctx)
	return cancel
}

type timeoutHandler struct {
	handler sbpServer.Handler
	timeout time.Duration
}

func (th timeoutHandler) Heartbeat(req *protocol.HeartbeatRequest, resp *protocol.HeartbeatResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.Heartbeat(req, resp)
}

func (th timeoutHandler) AllocateID(req *protocol.IDAllocationRequest, resp *protocol.IDAllocationResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.AllocateID(req, resp)
}

func (th timeoutHandler) ListRange(req *protocol.ListRangeRequest, resp *protocol.ListRangeResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.ListRange(req, resp)
}

func (th timeoutHandler) SealRange(req *protocol.SealRangeRequest, resp *protocol.SealRangeResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.SealRange(req, resp)
}

func (th timeoutHandler) CreateRange(req *protocol.CreateRangeRequest, resp *protocol.CreateRangeResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.CreateRange(req, resp)
}

func (th timeoutHandler) CreateStream(req *protocol.CreateStreamRequest, resp *protocol.CreateStreamResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.CreateStream(req, resp)
}

func (th timeoutHandler) DeleteStream(req *protocol.DeleteStreamRequest, resp *protocol.DeleteStreamResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.DeleteStream(req, resp)
}

func (th timeoutHandler) UpdateStream(req *protocol.UpdateStreamRequest, resp *protocol.UpdateStreamResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.UpdateStream(req, resp)
}

func (th timeoutHandler) DescribeStream(req *protocol.DescribeStreamRequest, resp *protocol.DescribeStreamResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.DescribeStream(req, resp)
}

func (th timeoutHandler) TrimStream(req *protocol.TrimStreamRequest, resp *protocol.TrimStreamResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.TrimStream(req, resp)
}

func (th timeoutHandler) ReportMetrics(req *protocol.ReportMetricsRequest, resp *protocol.ReportMetricsResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.ReportMetrics(req, resp)
}

func (th timeoutHandler) DescribePDCluster(req *protocol.DescribePDClusterRequest, resp *protocol.DescribePDClusterResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.DescribePDCluster(req, resp)
}

func (th timeoutHandler) CommitObject(req *protocol.CommitObjectRequest, resp *protocol.CommitObjectResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.CommitObject(req, resp)
}

func (th timeoutHandler) ListResource(req *protocol.ListResourceRequest, resp *protocol.ListResourceResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.ListResource(req, resp)
}

func (th timeoutHandler) WatchResource(req *protocol.WatchResourceRequest, resp *protocol.WatchResourceResponse) {
	cancel := timeoutReq(req, th.timeout)
	defer cancel()
	th.handler.WatchResource(req, resp)
}

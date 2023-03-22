package handler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/server/cluster"
	"github.com/AutoMQ/placement-manager/pkg/server/id"
	"github.com/AutoMQ/placement-manager/pkg/server/member"
	"github.com/AutoMQ/placement-manager/pkg/server/storage"
	"github.com/AutoMQ/placement-manager/pkg/util/testutil"
)

type mockServer struct {
	c *clientv3.Client
}

func (m *mockServer) Storage() storage.Storage {
	return storage.NewEtcd(
		m.c,
		"/test-server",
		zap.NewNop(),
		func() clientv3.Cmp { return clientv3.Compare(clientv3.CreateRevision("not-exist-key"), "=", 0) },
	)
}

func (m *mockServer) IDAllocator(key string, start, step uint64) id.Allocator {
	return id.NewEtcdAllocator(&id.EtcdAllocatorParam{
		Client:   m.c,
		CmpFunc:  func() clientv3.Cmp { return clientv3.Compare(clientv3.CreateRevision("not-exist-key"), "=", 0) },
		RootPath: "/test-server",
		Key:      key,
		Start:    start,
		Step:     step,
	}, zap.NewNop())
}

func (m *mockServer) Member() cluster.Member {
	return m
}

func (m *mockServer) IsLeader() bool {
	return true
}

func (m *mockServer) Leader() *member.Info {
	return &member.Info{
		Name:       "test-member-name",
		MemberID:   1,
		PeerUrls:   []string{"test-member-peer-urls"},
		ClientUrls: []string{"test-member-client-urls"},
		SbpAddr:    "test-member-sbp-addr",
	}
}

func startSbp(tb testing.TB) (*Sbp, func()) {
	re := require.New(tb)

	_, client, closeFunc := testutil.StartEtcd(tb)

	c := cluster.NewRaftCluster(context.Background(), zap.NewNop())
	err := c.Start(&mockServer{c: client})
	re.NoError(err)

	sbp := NewSbp(c, zap.NewNop())

	return sbp, func() { _ = c.Stop(); closeFunc() }
}

package member

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/pkg/server/config"
	testutil "github.com/AutoMQ/pd/pkg/util/test"
)

func TestMember_ClusterInfo(t *testing.T) {
	re := require.New(t)

	etcd, client, closeFunc := testutil.StartEtcd(t, nil)
	defer closeFunc()

	member := NewMember(etcd, client, zap.NewNop())
	cfg := &config.Config{AdvertisePDAddr: "test-sbp-addr"}
	err := member.Init(context.Background(), cfg, "test-member", "/test-member")
	re.NoError(err)

	members, err := member.ClusterInfo(context.Background())
	re.NoError(err)
	re.Len(members, 1)
	re.Equal("test-sbp-addr", members[0].AdvertisePDAddr)
}

func TestMember_ClusterInfo_MemberNotInit(t *testing.T) {
	re := require.New(t)

	etcd, client, closeFunc := testutil.StartEtcd(t, nil)
	defer closeFunc()

	member := NewMember(etcd, client, zap.NewNop())

	members, err := member.ClusterInfo(context.Background())
	re.NoError(err)
	re.Len(members, 0)
}

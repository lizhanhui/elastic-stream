package cluster

import (
	"context"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/util/fbutil"
)

// TestRaftCluster_eraseDataNodeInfo will fail if there are new fields in rpcfb.DataNodeT
func TestRaftCluster_eraseDataNodeInfo(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	var node rpcfb.DataNodeT
	_ = gofakeit.New(1).Struct(&node)

	cluster := NewRaftCluster(context.Background(), nil, zap.NewNop())
	cluster.eraseDataNodeInfo(&node)

	bytes := fbutil.Marshal(&node)
	re.Equal(20, len(bytes))
}

// TestRaftCluster_fillDataNodeInfo will fail if there are new fields in rpcfb.DataNodeT
func TestRaftCluster_fillDataNodeInfo(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	var node rpcfb.DataNodeT
	_ = gofakeit.New(1).Struct(&node)
	cluster := NewRaftCluster(context.Background(), nil, zap.NewNop())
	cluster.cache.SaveDataNode(&node)

	node2 := rpcfb.DataNodeT{
		NodeId: node.NodeId,
	}
	cluster.fillDataNodeInfo(&node2)

	re.Equal(node, node2)
}

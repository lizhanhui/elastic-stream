package cluster

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

var (
	// ErrNotEnoughDataNodes is returned when there are not enough data nodes to allocate a range.
	ErrNotEnoughDataNodes = errors.New("not enough data nodes")
)

// Heartbeat updates DataNode's last active time, and save it to storage if its info changed.
func (c *RaftCluster) Heartbeat(ctx context.Context, node *rpcfb.DataNodeT) error {
	logger := c.lg.With(traceutil.TraceLogField(ctx))

	updated := c.cache.SaveDataNode(node)
	if updated {
		logger.Info("data node updated, start to save it", zap.Int32("node-id", node.NodeId), zap.String("advertise-addr", node.AdvertiseAddr))
		_, err := c.storage.SaveDataNode(ctx, node)
		logger.Info("finish saving data node", zap.Int32("node-id", node.NodeId), zap.Error(err))
		if err != nil {
			return err
		}
	}
	return nil
}

// chooseDataNodes selects `cnt` number of data nodes from the available data nodes for a range.
// It returns ErrNotEnoughDataNodes if there are not enough data nodes to allocate.
func (c *RaftCluster) chooseDataNodes(cnt int8) ([]*rpcfb.ReplicaNodeT, error) {
	if int(cnt) > c.cache.DataNodeCount() {
		return nil, errors.Wrapf(ErrNotEnoughDataNodes, "required %d, available %d", cnt, c.cache.DataNodeCount())
	}

	nodes := c.cache.DataNodes()
	// TODO more intelligent selection
	sort.Slice(nodes, func(i, j int) bool {
		return !nodes[i].LastActiveTime.Before(nodes[j].LastActiveTime)
	})

	replicaNodes := make([]*rpcfb.ReplicaNodeT, 0, cnt)
	for i := 0; i < int(cnt); i++ {
		replicaNodes = append(replicaNodes, &rpcfb.ReplicaNodeT{
			DataNode: nodes[i].DataNodeT,
		})
	}
	replicaNodes[0].IsPrimary = true

	return replicaNodes, nil
}

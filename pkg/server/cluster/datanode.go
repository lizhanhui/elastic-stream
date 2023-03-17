package cluster

import (
	"sort"

	"github.com/pkg/errors"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

var (
	// ErrNotEnoughDataNodes is returned when there are not enough data nodes to allocate a range.
	ErrNotEnoughDataNodes = errors.New("not enough data nodes")
)

// Heartbeat updates DataNode's last active time, and save it to storage if its info changed.
func (c *RaftCluster) Heartbeat(node *rpcfb.DataNodeT) error {
	updated := c.cache.SaveDataNode(node)
	if updated {
		_, err := c.storage.SaveDataNode(node)
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
		return nil, ErrNotEnoughDataNodes
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

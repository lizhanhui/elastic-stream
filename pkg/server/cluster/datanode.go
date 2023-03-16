package cluster

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

func (c *RaftCluster) Heartbeat(node *rpcfb.DataNodeT) error {
	updated := c.cache.SaveDataNode(node)
	if updated {
		// TODO
		// err := c.storage.SaveDataNode(node)
	}
	return nil
}

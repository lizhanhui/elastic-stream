package cluster

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

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

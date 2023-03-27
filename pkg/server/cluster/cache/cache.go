package cache

import (
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// Cache is the cache for all metadata.
type Cache struct {
	dataNodes cmap.ConcurrentMap[int32, *DataNode]
	// TODO: add more cache
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		dataNodes: cmap.NewWithCustomShardingFunction[int32, *DataNode](func(key int32) uint32 { return uint32(key) }),
	}
}

// Reset resets the cache.
func (c *Cache) Reset() {
	c.dataNodes.Clear()
}

// DataNode is the cache for DataNodeT and its status.
type DataNode struct {
	*rpcfb.DataNodeT
	LastActiveTime time.Time
}

func NewDataNode(dataNode *rpcfb.DataNodeT) *DataNode {
	return &DataNode{
		DataNodeT:      dataNode,
		LastActiveTime: time.Now(),
	}
}

// SaveDataNode saves a data node to the cache.
// It returns true if the data node is updated.
func (c *Cache) SaveDataNode(nodeT *rpcfb.DataNodeT) (updated bool) {
	_ = c.dataNodes.Upsert(nodeT.NodeId, NewDataNode(nodeT), func(exist bool, valueInMap, newValue *DataNode) *DataNode {
		if exist {
			if !isDataNodeEqual(valueInMap.DataNodeT, newValue.DataNodeT) {
				updated = true
				valueInMap.DataNodeT = newValue.DataNodeT
			}
			valueInMap.LastActiveTime = newValue.LastActiveTime
			return valueInMap
		}
		updated = true
		return newValue
	})
	return updated
}

// DataNodes returns all data nodes in the cache.
func (c *Cache) DataNodes() []*DataNode {
	nodes := make([]*DataNode, 0, c.dataNodes.Count())
	c.dataNodes.IterCb(func(_ int32, node *DataNode) {
		nodes = append(nodes, node)
	})
	return nodes
}

// DataNodeCount returns the count of data nodes in the cache.
func (c *Cache) DataNodeCount() int {
	return c.dataNodes.Count()
}

func isDataNodeEqual(a, b *rpcfb.DataNodeT) bool {
	return a.NodeId == b.NodeId &&
		a.AdvertiseAddr == b.AdvertiseAddr
}

package cache

import (
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// Cache is the cache for all metadata.
type Cache struct {
	streams   cmap.ConcurrentMap[int64, *Stream]
	dataNodes cmap.ConcurrentMap[int32, *DataNode]
	// TODO: add more cache
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		streams:   cmap.NewWithCustomShardingFunction[int64, *Stream](func(key int64) uint32 { return uint32(key) }),
		dataNodes: cmap.NewWithCustomShardingFunction[int32, *DataNode](func(key int32) uint32 { return uint32(key) }),
	}
}

// Reset resets the cache.
func (c *Cache) Reset() {
	c.streams.Clear()
	c.dataNodes.Clear()
}

// Stream is the cache for StreamT and its ranges.
type Stream struct {
	*rpcfb.StreamT
	ranges cmap.ConcurrentMap[int32, *rpcfb.RangeT]
}

func NewStream(stream *rpcfb.StreamT) *Stream {
	return &Stream{
		StreamT: stream,
		ranges:  cmap.NewWithCustomShardingFunction[int32, *rpcfb.RangeT](func(key int32) uint32 { return uint32(key) }),
	}
}

// SaveStream saves a stream to the cache.
func (c *Cache) SaveStream(streamT *rpcfb.StreamT) {
	stream, ok := c.streams.Get(streamT.StreamId)
	if ok {
		stream.StreamT = streamT
	} else {
		stream = NewStream(streamT)
	}
	c.streams.Set(stream.StreamId, stream)
}

// StreamCount returns the count of streams in the cache.
func (c *Cache) StreamCount() int {
	return c.streams.Count()
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
	node, ok := c.dataNodes.Get(nodeT.NodeId)
	if ok {
		if !isDataNodeEqual(node.DataNodeT, nodeT) {
			updated = true
			node.DataNodeT = nodeT
		}
		node.LastActiveTime = time.Now()
	} else {
		node = NewDataNode(nodeT)
		updated = true
	}
	c.dataNodes.Set(node.NodeId, node)
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

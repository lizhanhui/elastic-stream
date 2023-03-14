package cache

import (
	cmap "github.com/orcaman/concurrent-map/v2"

	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

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

// Cache is the cache for all metadata.
type Cache struct {
	streams cmap.ConcurrentMap[int64, *Stream]
	// TODO: add more cache
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		streams: cmap.NewWithCustomShardingFunction[int64, *Stream](func(key int64) uint32 { return uint32(key) }),
	}
}

// Reset resets the cache.
func (c *Cache) Reset() {
	c.streams.Clear()
}

// SaveStream saves a stream to the cache.
func (c *Cache) SaveStream(stream *rpcfb.StreamT) {
	c.streams.Set(stream.StreamId, NewStream(stream))
}

// StreamCount returns the count of streams in the cache.
func (c *Cache) StreamCount() int {
	return c.streams.Count()
}

package cache

import (
	"github.com/AutoMQ/placement-manager/api/rpcfb/rpcfb"
)

// Stream is the cache for StreamT and its ranges.
type Stream struct {
	*rpcfb.StreamT
	ranges map[int32]*rpcfb.RangeT
}

// Cache is the cache for all metadata.
type Cache struct {
	streams map[int64]*Stream
	// TODO: add more cache
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		// TODO init other fields
	}
}

// Reset resets the cache.
func (c *Cache) Reset() {
	c.streams = make(map[int64]*Stream)
}

// SaveStream saves a stream to the cache.
func (c *Cache) SaveStream(stream *rpcfb.StreamT) {
	// TODO lock it
	c.streams[stream.StreamId] = &Stream{
		StreamT: stream,
		ranges:  make(map[int32]*rpcfb.RangeT),
	}
}

// StreamCount returns the count of streams in the cache.
func (c *Cache) StreamCount() int {
	// TODO lock it
	return len(c.streams)
}

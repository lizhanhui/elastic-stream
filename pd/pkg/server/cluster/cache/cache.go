package cache

import (
	"fmt"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	cmap "github.com/orcaman/concurrent-map/v2"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/model"
)

type serverID int32

// RangeServer is the cache for RangeServerT and its status.
type RangeServer struct {
	rpcfb.RangeServerT
	LastActiveTime *time.Time                 // nil means never active
	Metrics        *rpcfb.RangeServerMetricsT // nil means no metrics
}

// Score returns the score of the range server.
func (rs *RangeServer) Score() (score int) {
	// TODO more intelligent score
	if rs.Metrics == nil {
		return
	}

	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	score += 10000
	score -= int(10 * rs.Metrics.DiskInRate / (100 * MB))
	score -= int(10 * rs.Metrics.DiskOutRate / (100 * MB))
	score += int(10 * rs.Metrics.DiskFreeSpace / (50 * GB))
	score -= int(10 * rs.Metrics.DiskUnindexedDataSize / (100 * MB))
	score -= int(10 * rs.Metrics.MemoryUsed / (1 * GB))
	score -= int(10 * rs.Metrics.UringTaskRate / 1024)
	score -= int(10 * rs.Metrics.UringInflightTaskCnt / 128)
	score -= int(10 * rs.Metrics.UringPendingTaskCnt / 1024)
	score -= int(10 * rs.Metrics.UringTaskAvgLatency / 10)
	score -= int(10 * rs.Metrics.NetworkAppendRate / 256)
	score -= int(10 * rs.Metrics.NetworkFetchRate / 256)
	score -= int(10 * rs.Metrics.NetworkFailedAppendRate / 1)
	score -= int(10 * rs.Metrics.NetworkFailedFetchRate / 1)
	score -= int(10 * rs.Metrics.NetworkAppendAvgLatency / 1)
	score -= int(10 * rs.Metrics.NetworkAppendAvgLatency / 1)
	score -= int(10 * rs.Metrics.RangeMissingReplicaCnt / 2)
	score -= int(10 * rs.Metrics.RangeActiveCnt / 10)

	return
}

type rangeIDSet mapset.Set[model.RangeID]

// Cache is the cache for all metadata.
type Cache struct {
	rangeServers cmap.ConcurrentMap[int32, *RangeServer]
	rangeIndex   cmap.ConcurrentMap[serverID, rangeIDSet]
}

// NewCache creates a new Cache.
func NewCache() *Cache {
	return &Cache{
		rangeServers: cmap.NewWithCustomShardingFunction[int32, *RangeServer](func(key int32) uint32 { return uint32(key) }),
		rangeIndex:   cmap.NewWithCustomShardingFunction[serverID, rangeIDSet](func(key serverID) uint32 { return uint32(key) }),
	}
}

// Reset resets the cache.
func (c *Cache) Reset() {
	// No need to reset range servers, as they will be updated by heartbeat.
	// c.rangeServers.Clear()
	c.ResetRangeIndex()
}

// ResetRangeIndex resets the range index.
func (c *Cache) ResetRangeIndex() {
	c.rangeIndex.Clear()
}

// SaveRangeServer saves a range server to the cache.
// It returns true if the range server is new or its info is updated.
// If its info is updated, the old value is returned.
func (c *Cache) SaveRangeServer(rangeServer *RangeServer) (updated bool, old *rpcfb.RangeServerT) {
	_ = c.rangeServers.Upsert(rangeServer.ServerId, rangeServer, func(exist bool, valueInMap, newValue *RangeServer) *RangeServer {
		if exist {
			if !isRangeServerEqual(valueInMap.RangeServerT, newValue.RangeServerT) {
				updated = true
				valueInMap.RangeServerT = newValue.RangeServerT
				old = &valueInMap.RangeServerT
			}
			valueInMap.LastActiveTime = newValue.LastActiveTime
			if newValue.Metrics != nil {
				valueInMap.Metrics = newValue.Metrics
			}
			return valueInMap
		}
		updated = true
		return newValue
	})
	return
}

// RangeServer returns the range server by server ID.
// The returned value is nil if the range server is not found.
// The returned value should not be modified.
func (c *Cache) RangeServer(serverID int32) *RangeServer {
	rangeServer, ok := c.rangeServers.Get(serverID)
	if !ok {
		return nil
	}
	return rangeServer
}

// ActiveRangeServers returns all active range servers.
// It returns two lists, the first one is the white list, the second one is the gray list.
func (c *Cache) ActiveRangeServers(timeout time.Duration, grayServerIDs mapset.Set[int32]) (white []*RangeServer, gray []*RangeServer) {
	white = make([]*RangeServer, 0)
	gray = make([]*RangeServer, 0)
	c.rangeServers.IterCb(func(_ int32, rangeServer *RangeServer) {
		if rangeServer.LastActiveTime == nil || time.Since(*rangeServer.LastActiveTime) > timeout {
			return
		}
		if rangeServer.RangeServerT.State != rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE {
			return
		}
		if rangeServer.Metrics != nil && rangeServer.Metrics.DiskFreeSpace == 0 {
			return
		}
		if grayServerIDs.Contains(rangeServer.ServerId) {
			gray = append(gray, rangeServer)
		} else {
			white = append(white, rangeServer)
		}
	})
	return
}

// RangeServerCount returns the count of range servers in the cache.
func (c *Cache) RangeServerCount() int {
	return c.rangeServers.Count()
}

// RangeServerInfo returns the range server info for logging.
func (c *Cache) RangeServerInfo() (fields []zap.Field) {
	c.rangeServers.IterCb(func(id int32, rangeServer *RangeServer) {
		fields = append(fields,
			zap.String(fmt.Sprintf("range-server-%d-addr", id), rangeServer.AdvertiseAddr),
			zap.Stringer(fmt.Sprintf("range-server-%d-state", id), rangeServer.State),
			zap.Timep(fmt.Sprintf("range-server-%d-last-active-time", id), rangeServer.LastActiveTime),
		)
	})
	return
}

// OnRangeCreated is called when a new range is created.
func (c *Cache) OnRangeCreated(r *rpcfb.RangeT) {
	rID := model.RangeID{
		StreamID: r.StreamId,
		Index:    r.Index,
	}
	for _, s := range r.Servers {
		c.addRangeToServer(rID, s.ServerId)
	}
}

// OnRangeModified is called when a range is modified.
func (c *Cache) OnRangeModified(r *rpcfb.RangeT) {
	_ = r
	// TODO: Currently, range servers are not updated when a range is modified.
}

// OnRangeDeleted is called when a range is deleted.
func (c *Cache) OnRangeDeleted(r *rpcfb.RangeT) {
	rID := model.RangeID{
		StreamID: r.StreamId,
		Index:    r.Index,
	}
	for _, s := range r.Servers {
		c.removeRangeFromServer(rID, s.ServerId)
	}
}

func (c *Cache) addRangeToServer(rID model.RangeID, sID int32) {
	c.rangeIndex.Upsert(serverID(sID), nil, func(exist bool, valueInMap rangeIDSet, _ rangeIDSet) rangeIDSet {
		if exist {
			valueInMap.Add(rID)
			return valueInMap
		}
		return mapset.NewThreadUnsafeSet(rID)
	})
}

func (c *Cache) removeRangeFromServer(rID model.RangeID, sID int32) {
	c.rangeIndex.Upsert(serverID(sID), nil, func(exist bool, valueInMap rangeIDSet, _ rangeIDSet) rangeIDSet {
		if exist {
			valueInMap.Remove(rID)
			return valueInMap
		}
		return mapset.NewThreadUnsafeSet[model.RangeID]()
	})
}

// RangesOnServer returns all ranges on a range server.
func (c *Cache) RangesOnServer(sID int32) (rangeIDs mapset.Set[model.RangeID]) {
	rangeIDs, _ = c.rangeIndex.Get(serverID(sID))
	if rangeIDs == nil {
		rangeIDs = mapset.NewThreadUnsafeSet[model.RangeID]()
	}
	return
}

func isRangeServerEqual(a, b rpcfb.RangeServerT) bool {
	return a.ServerId == b.ServerId &&
		a.AdvertiseAddr == b.AdvertiseAddr &&
		a.State == b.State
}

package cluster

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/cluster/cache"
	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

type RangeServerService interface {
	// Heartbeat updates RangeServer's last active time, and save it to storage if its info changed.
	// It returns model.ErrPDNotLeader if the current PD node is not the leader.
	Heartbeat(ctx context.Context, rangeServer *rpcfb.RangeServerT) error
	// AllocateID allocates a range server id from the id allocator.
	// It returns model.ErrPDNotLeader if the current PD node is not the leader.
	AllocateID(ctx context.Context) (int32, error)
	// Metrics receives metrics from range servers.
	// It returns model.ErrPDNotLeader if the current PD node is not the leader.
	Metrics(ctx context.Context, rangeServer *rpcfb.RangeServerT, metrics *rpcfb.RangeServerMetricsT) error
}

func (c *RaftCluster) Heartbeat(ctx context.Context, rangeServer *rpcfb.RangeServerT) error {
	logger := c.lg.With(traceutil.TraceLogField(ctx))

	t := time.Now()
	updated, old := c.cache.SaveRangeServer(&cache.RangeServer{
		RangeServerT:   *rangeServer,
		LastActiveTime: &t,
	})
	if updated && c.IsLeader() {
		if old == nil {
			logger.Info("range server added, start to save it", zap.Any("new", rangeServer))
		} else {
			logger.Info("range server updated, start to save it", zap.Any("new", rangeServer), zap.Any("old", old))
		}
		_, err := c.storage.SaveRangeServer(ctx, rangeServer)
		logger.Info("finish saving range server", zap.Int32("server-id", rangeServer.ServerId), zap.Error(err))
		if err != nil {
			if errors.Is(err, model.ErrKVTxnFailed) {
				return model.ErrPDNotLeader
			}
			return err
		}
	}
	return nil
}

func (c *RaftCluster) AllocateID(ctx context.Context) (int32, error) {
	logger := c.lg.With(traceutil.TraceLogField(ctx))

	id, err := c.rsAlloc.Alloc(ctx)
	if err != nil {
		logger.Error("failed to allocate range server id", zap.Error(err))
		if errors.Is(err, model.ErrKVTxnFailed) {
			err = model.ErrPDNotLeader
		}
		return -1, err
	}

	return int32(id), nil
}

func (c *RaftCluster) Metrics(ctx context.Context, rangeServer *rpcfb.RangeServerT, metrics *rpcfb.RangeServerMetricsT) error {
	logger := c.lg.With(traceutil.TraceLogField(ctx))

	t := time.Now()
	updated, old := c.cache.SaveRangeServer(&cache.RangeServer{
		RangeServerT:   *rangeServer,
		LastActiveTime: &t,
		Metrics:        metrics,
	})
	if updated && c.IsLeader() {
		if old == nil {
			logger.Info("range server added when reporting metrics, start to save it", zap.Any("new", rangeServer))
		} else {
			logger.Info("range server updated when reporting metrics, start to save it", zap.Any("new", rangeServer), zap.Any("old", old))
		}
		_, err := c.storage.SaveRangeServer(ctx, rangeServer)
		logger.Info("finish saving range server", zap.Int32("server-id", rangeServer.ServerId), zap.Error(err))
		if err != nil {
			if errors.Is(err, model.ErrKVTxnFailed) {
				return model.ErrPDNotLeader
			}
			return err
		}
	}

	return nil
}

// chooseRangeServers selects `cnt` number of range servers from the available range servers for a range.
// If `grayServerIDs` is not nil, the range servers with the ids in `grayServerIDs` will not be selected, unless there are no other available range servers.
// Only RangeServerT.ServerId is filled in the returned RangeServerT.
// It returns model.ErrNotEnoughRangeServers if there are not enough range servers to allocate.
func (c *RaftCluster) chooseRangeServers(cnt int, grayServerIDs map[int32]struct{}, logger *zap.Logger) ([]*rpcfb.RangeServerT, error) {
	if cnt <= 0 {
		return nil, nil
	}

	chose := make([]*rpcfb.RangeServerT, 0, cnt)
	wRangeServers, gRangeServers := c.cache.ActiveRangeServers(c.cfg.RangeServerTimeout, grayServerIDs)

	if cnt > len(wRangeServers)+len(gRangeServers) {
		logger.With(zap.Namespace("range-servers")).With(c.cache.RangeServerInfo()...).
			Error("not enough range servers", zap.Int("required", cnt), zap.Int("available", len(wRangeServers)+len(gRangeServers)))
		return nil, errors.WithMessagef(model.ErrNotEnoughRangeServers, "required %d, available %d", cnt, len(wRangeServers)+len(gRangeServers))
	}

	// If there are not enough range servers, use the gray list.
	for _, rs := range gRangeServers {
		if cnt > len(wRangeServers) {
			chose = append(chose, &rpcfb.RangeServerT{
				ServerId: rs.ServerId,
			})
			cnt--
		} else {
			break
		}
	}

	// TODO: use a better algorithm to choose range servers
	// perm := rand.Perm(len(rangeServers))
	// for i := 0; i < cnt; i++ {
	// 	// select two random range servers and choose the one with higher score
	// 	rangeServer1 := rangeServers[perm[i]]
	// 	id := rangeServer1.ServerId
	// 	if cnt+i < len(perm) {
	// 		rangeServer2 := rangeServers[perm[cnt+i]]
	// 		if rangeServer2.Score() > rangeServer1.Score() {
	// 			id = rangeServer2.ServerId
	// 		}
	// 	}
	// 	chose[i] = &rpcfb.RangeServerT{
	// 		ServerId: id,
	// 	}
	// }

	// round-robin
	idx := c.rangeServerIdx.Add(uint64(cnt))
	for i := 0; i < cnt; i++ {
		serverIdx := (idx - uint64(i)) % uint64(len(wRangeServers))
		chose = append(chose, &rpcfb.RangeServerT{
			ServerId: wRangeServers[serverIdx].ServerId,
		})
	}

	return chose, nil
}

func (c *RaftCluster) fillRangeServersInfo(r *rpcfb.RangeT) {
	if r == nil {
		return
	}
	for _, rangeServer := range r.Servers {
		c.fillRangeServerInfo(rangeServer)
	}
}

func (c *RaftCluster) fillRangeServerInfo(rangeServer *rpcfb.RangeServerT) {
	if rangeServer == nil {
		return
	}
	rs := c.cache.RangeServer(rangeServer.ServerId)
	if rs == nil {
		c.lg.Warn("range server not found", zap.Int32("server-id", rangeServer.ServerId))
		return
	}
	rangeServer.AdvertiseAddr = rs.AdvertiseAddr
	rangeServer.State = rs.State
}

func eraseRangeServersInfo(in []*rpcfb.RangeServerT) (out []*rpcfb.RangeServerT) {
	out = make([]*rpcfb.RangeServerT, len(in))
	for i, rs := range in {
		out[i] = &rpcfb.RangeServerT{
			ServerId: rs.ServerId,
		}
	}
	return
}

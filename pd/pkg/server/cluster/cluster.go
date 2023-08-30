// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	sbpClient "github.com/AutoMQ/pd/pkg/sbp/client"
	"github.com/AutoMQ/pd/pkg/server/cluster/cache"
	"github.com/AutoMQ/pd/pkg/server/config"
	"github.com/AutoMQ/pd/pkg/server/id"
	"github.com/AutoMQ/pd/pkg/server/member"
	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/server/storage"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
)

const (
	_streamIDAllocKey = "stream"
	_streamIDStep     = 100

	_rangeServerIDAllocKey = "range-server"
	_rangeServerIDStep     = 1

	_objectIDAllocKey = "object"
	_objectIDStep     = 100

	_loadRangeLimit      = 1024
	_loadRangeRetryDelay = 1 * time.Second
)

// RaftCluster is used for metadata management.
type RaftCluster struct {
	ctx context.Context

	// set when creating the RaftCluster
	cfg            *config.Cluster
	member         MemberService
	cache          *cache.Cache
	rangeServerIdx atomic.Uint64
	mu             sync.RWMutex  // lock for all variables below
	rangeLoadedCh  chan struct{} // Closed when all ranges are loaded.

	running       atomic.Bool
	runningCtx    context.Context
	runningCancel context.CancelFunc

	// set when starting the RaftCluster
	storage storage.Storage
	sAlloc  id.Allocator // stream id allocator
	rsAlloc id.Allocator // range server id allocator
	oAlloc  id.Allocator // object id allocator
	client  sbpClient.Client

	lg *zap.Logger
}

type MemberService interface {
	IsLeader() bool
	// ClusterInfo returns all members in the cluster.
	ClusterInfo(ctx context.Context) ([]*member.Info, error)
}

// Server is the interface for starting a RaftCluster.
type Server interface {
	Storage() storage.Storage
	IDAllocator(key string, start, step uint64) id.Allocator
	Member() MemberService
	SbpClient() sbpClient.Client
}

// NewRaftCluster creates a new RaftCluster.
func NewRaftCluster(ctx context.Context, cfg *config.Cluster, member MemberService, logger *zap.Logger) *RaftCluster {
	return &RaftCluster{
		ctx:           ctx,
		cfg:           cfg,
		member:        member,
		cache:         cache.NewCache(),
		rangeLoadedCh: make(chan struct{}),
		lg:            logger,
	}
}

// Start starts the RaftCluster.
func (c *RaftCluster) Start(s Server) error {
	logger := c.lg
	if c.IsRunning() {
		logger.Warn("raft cluster is already running")
		return nil
	}

	logger.Info("starting raft cluster")

	c.storage = s.Storage()
	c.runningCtx, c.runningCancel = context.WithCancel(c.ctx)
	c.sAlloc = s.IDAllocator(_streamIDAllocKey, uint64(model.MinStreamID), _streamIDStep)
	c.rsAlloc = s.IDAllocator(_rangeServerIDAllocKey, uint64(model.MinRangeServerID), _rangeServerIDStep)
	c.oAlloc = s.IDAllocator(_objectIDAllocKey, uint64(model.MinObjectID), _objectIDStep)
	c.client = s.SbpClient()

	err := c.loadInfo(c.runningCtx)
	if err != nil {
		logger.Error("load cluster info failed", zap.Error(err))
		return errors.WithMessage(err, "load cluster info")
	}

	// start other background goroutines

	if c.running.Swap(true) {
		logger.Warn("raft cluster is already running")
		return nil
	}
	return nil
}

// loadInfo loads all info from storage into cache.
func (c *RaftCluster) loadInfo(ctx context.Context) error {
	err := c.loadRangeServers(ctx)
	if err != nil {
		return err
	}

	go c.loadRangesLoop(ctx)

	return nil
}

func (c *RaftCluster) loadRangeServers(ctx context.Context) error {
	logger := c.lg

	start := time.Now()
	err := c.storage.ForEachRangeServer(ctx, func(serverT *rpcfb.RangeServerT) error {
		updated, old := c.cache.SaveRangeServer(&cache.RangeServer{
			RangeServerT: *serverT,
		})
		if updated && old != nil {
			logger.Warn("different range server in storage and cache", zap.Any("range-server-in-storage", serverT), zap.Any("range-server-in-cache", old))
		}
		return nil
	})
	if err != nil {
		return errors.WithMessage(err, "load range servers")
	}
	logger.Info("load range servers", zap.Int("count", c.cache.RangeServerCount()), zap.Duration("cost", time.Since(start)))

	return nil
}

func (c *RaftCluster) loadRangesLoop(ctx context.Context) {
	for {
		if ok := c.loadRanges(ctx); !ok {
			return
		}
		c.cache.ResetRangeIndex()
		time.Sleep(_loadRangeRetryDelay)
	}
}

// loadRanges returns whether a retry should be performed.
func (c *RaftCluster) loadRanges(ctx context.Context) bool {
	logger := c.lg

	var rv int64
	// list ranges
	token := endpoint.ContinueToken{
		ResourceType: rpcfb.ResourceTypeRESOURCE_RANGE,
		More:         true,
	}
	for token.More {
		var resources []*rpcfb.ResourceT
		var err error
		logger := logger.With(zap.Int64("rv", rv)).With(token.ZapFields()...)
		resources, rv, token, err = c.storage.ListResource(ctx, rv, token, _loadRangeLimit)
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			logger.Warn("list ranges failed", zap.Error(err))
			//nolint:gosimple
			if errors.Is(err, model.ErrKVTxnFailed) {
				// Not leader anymore, fail fast
				return false
			}
			return true
		}
		for _, resource := range resources {
			c.cache.OnRangeCreated(resource.Range)
		}
	}

	// all ranges are loaded, ready to serve
	c.rangeLoaded()
	defer c.resetRangeLoaded()

	// watch ranges
	for {
		var events []*rpcfb.ResourceEventT
		var err error
		logger := logger.With(zap.Int64("rv", rv))
		events, rv, err = c.WatchResource(ctx, rv, []rpcfb.ResourceType{rpcfb.ResourceTypeRESOURCE_RANGE})
		if err != nil {
			if ctx.Err() != nil {
				return false
			}
			logger.Warn("watch ranges failed", zap.Error(err))
			return true
		}
		for _, event := range events {
			switch event.Type {
			case rpcfb.EventTypeEVENT_ADDED:
				c.cache.OnRangeCreated(event.Resource.Range)
			case rpcfb.EventTypeEVENT_MODIFIED:
				c.cache.OnRangeModified(event.Resource.Range)
			case rpcfb.EventTypeEVENT_DELETED:
				c.cache.OnRangeDeleted(event.Resource.Range)
			}
		}
	}
}

// Stop stops the RaftCluster.
func (c *RaftCluster) Stop() error {
	logger := c.lg
	if !c.running.Swap(false) {
		logger.Warn("raft cluster has already been stopped")
		return nil
	}

	logger.Info("stopping cluster")
	c.runningCancel()
	c.cache.Reset()

	logger.Info("cluster stopped")
	return nil
}

// IsRunning returns true if the RaftCluster is running.
func (c *RaftCluster) IsRunning() bool {
	return c.running.Load()
}

func (c *RaftCluster) IsLeader() bool {
	return c.member.IsLeader()
}

func (c *RaftCluster) ClusterInfo(ctx context.Context) ([]*member.Info, error) {
	return c.member.ClusterInfo(ctx)
}

func (c *RaftCluster) rangeLoaded() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.rangeLoadedCh:
	default:
		close(c.rangeLoadedCh)
	}
}

func (c *RaftCluster) resetRangeLoaded() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.rangeLoadedCh:
		c.rangeLoadedCh = make(chan struct{})
	default:
	}
}

func (c *RaftCluster) rangeLoadedNotify() <-chan struct{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.rangeLoadedCh
}

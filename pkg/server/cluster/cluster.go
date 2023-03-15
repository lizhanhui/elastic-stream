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
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/server/cluster/cache"
	"github.com/AutoMQ/placement-manager/pkg/server/member"
	"github.com/AutoMQ/placement-manager/pkg/server/storage"
)

// RaftCluster is used for metadata management.
type RaftCluster struct {
	ctx       context.Context
	clusterID uint64

	starting      atomic.Bool
	running       atomic.Bool
	runningCtx    context.Context
	runningCancel context.CancelFunc

	storage storage.Storage
	cache   *cache.Cache
	member  *member.Member

	lg *zap.Logger
}

// Server is the interface for starting a RaftCluster.
type Server interface {
	Storage() storage.Storage
	Member() *member.Member
}

// NewRaftCluster creates a new RaftCluster.
func NewRaftCluster(ctx context.Context, clusterID uint64, logger *zap.Logger) *RaftCluster {
	return &RaftCluster{
		ctx:       ctx,
		clusterID: clusterID,
		cache:     cache.NewCache(),
		lg:        logger.With(zap.Uint64("cluster-id", clusterID)),
	}
}

// Start starts the RaftCluster.
func (c *RaftCluster) Start(s Server) error {
	logger := c.lg
	if c.IsRunning() {
		logger.Warn("raft cluster is already running")
		return nil
	}
	if c.starting.Swap(true) {
		logger.Warn("raft cluster is starting")
		return nil
	}
	defer c.starting.Store(false)

	logger.Info("starting raft cluster")

	c.storage = s.Storage()
	c.member = s.Member()
	c.runningCtx, c.runningCancel = context.WithCancel(c.ctx)

	err := c.loadInfo()
	if err != nil {
		logger.Error("load cluster info failed", zap.Error(err))
		return errors.Wrap(err, "load cluster info")
	}

	// TODO: start raft cluster

	if c.running.Swap(true) {
		logger.Warn("raft cluster is already running")
		return nil
	}
	return nil
}

// loadInfo loads all info from storage into cache.
func (c *RaftCluster) loadInfo() error {
	// TODO use cache later
	if c != nil {
		return nil
	}
	logger := c.lg

	c.cache.Reset()

	start := time.Now()
	err := c.storage.ForEachStream(c.cache.SaveStream)
	if err != nil {
		return errors.Wrap(err, "load streams")
	}
	logger.Info("load streams", zap.Int("count", c.cache.StreamCount()), zap.Duration("cost", time.Since(start)))

	// TODO load other info
	return nil
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
	// TODO: stop raft cluster
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

func (c *RaftCluster) Leader() *member.Info {
	return c.member.Leader()
}

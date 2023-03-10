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

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/server/storage"
)

type RaftCluster struct {
	ctx context.Context

	starting      atomic.Bool
	running       atomic.Bool
	runningCtx    context.Context
	runningCancel context.CancelFunc

	clusterID uint64
	storage   storage.Storage

	lg *zap.Logger
}

func NewRaftCluster(ctx context.Context, clusterID uint64, storage storage.Storage, logger *zap.Logger) *RaftCluster {
	return &RaftCluster{
		ctx:       ctx,
		clusterID: clusterID,
		storage:   storage,
		lg:        logger.With(zap.Uint64("cluster-id", clusterID)),
	}
}

func (c *RaftCluster) Start() error {
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

	logger.Info("start raft cluster")

	c.runningCtx, c.runningCancel = context.WithCancel(c.ctx)

	err := c.LoadInfo()
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

func (c *RaftCluster) LoadInfo() error {
	// TODO
	return nil
}

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

func (c *RaftCluster) IsRunning() bool {
	return c.running.Load()
}

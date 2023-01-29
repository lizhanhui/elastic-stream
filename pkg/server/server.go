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

package server

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	etcdTimeout    = time.Second * 3      // etcd DialTimeout
	rootPathPrefix = "/placement-manager" // prefix of Server.rootPath
)

// Server ensures redundancy by using the Raft consensus algorithm provided by etcd
type Server struct {
	started int64 // 0 for closed, 1 for started

	cfg     *Config       // Server configuration
	etcdCfg *embed.Config // etcd configuration

	ctx        context.Context // main context
	loopCtx    context.Context // loop context
	loopCancel func()          // loop cancel
	loopWg     sync.WaitGroup  // loop wait group

	member   *Member          // for leader election
	client   *clientv3.Client // etcd client
	id       uint64           // server id
	rootPath string           // root path in etcd
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *Config) (*Server, error) {
	s := &Server{
		cfg:    cfg,
		ctx:    ctx,
		member: &Member{},
	}

	// etcd Config
	etcdCfg, err := s.cfg.GenEmbedEtcdConfig()
	if err != nil {
		return nil, err
	}
	s.etcdCfg = etcdCfg

	return s, nil
}

// Start starts the server
func (s *Server) Start() error {
	if err := s.startEtcd(s.ctx); err != nil {
		return err
	}
	if err := s.startServer(s.ctx); err != nil {
		return err
	}
	s.startLoop(s.ctx)

	return nil
}

func (s *Server) startEtcd(ctx context.Context) error {
	// TODO
	return nil
}

func (s *Server) startServer(ctx context.Context) error {
	// TODO
	return nil
}

func (s *Server) startLoop(ctx context.Context) {
	// TODO
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.started, 1, 0) {
		// server is already closed
		return
	}
	// TODO stop loop, close etcd, etc.
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.started) == 0
}

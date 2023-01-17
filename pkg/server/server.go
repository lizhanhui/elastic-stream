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
	closed int64 // 0 for closed, 1 for started

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

func CreateServer(ctx context.Context, cfg *Config) (*Server, error) {
	// TODO
	return nil, nil
}

func (s *Server) Start() error {
	// TODO
	return nil
}

func (s *Server) Close() {
	// TODO
}

func (s *Server) IsClosed() bool {
	// TODO
	return false
}

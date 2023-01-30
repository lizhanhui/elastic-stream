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
	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
	"github.com/AutoMQ/placement-manager/pkg/util/typeutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"math/rand"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const (
	serverStatusClosed  = iota
	serverStatusStarted = iota
)

const (
	etcdTimeout      = time.Second * 3 // etcd DialTimeout
	etcdStartTimeout = time.Minute * 5 // timeout when start etcd

	rootPathPrefix = "/placement-manager"           // prefix of Server.rootPath
	serverIDPath   = "/placement-manager/server_id" // path of Server.id
)

// Server ensures redundancy by using the Raft consensus algorithm provided by etcd
type Server struct {
	status int64 // serverStatusClosed or serverStatusStarted

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

	lg *zap.Logger // logger
}

// CreateServer creates the UNINITIALIZED pd server with given configuration.
func CreateServer(ctx context.Context, cfg *Config) (*Server, error) {
	rand.Seed(time.Now().UnixNano())

	s := &Server{
		status: serverStatusClosed,
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
	startTimeoutCtx, cancel := context.WithTimeout(ctx, etcdStartTimeout)
	defer cancel()

	etcd, err := embed.StartEtcd(s.etcdCfg)
	if err != nil {
		return err
	}

	// wait until etcd is ready or timeout
	select {
	case <-etcd.Server.ReadyNotify():
	case <-startTimeoutCtx.Done():
		return errors.New("failed to start etcd: timeout.")
	}

	// init client
	// TODO whether to use ACUrls or ACUrls[0] ?
	endpoints := []string{s.etcdCfg.ACUrls[0].String()}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: etcdTimeout,
		Logger:      s.lg,
	})
	if err != nil {
		return err
	}
	s.client = client

	// init member
	s.member = NewMember(etcd, client, uint64(etcd.Server.ID()))

	return nil
}

func (s *Server) startServer(ctx context.Context) error {
	// init server id
	if err := s.initID(); err != nil {
		return err
	}

	s.rootPath = path.Join(rootPathPrefix, strconv.FormatUint(s.id, 10))
	s.member.Init(s.cfg, s.Name(), s.rootPath)
	// TODO set member prop

	atomic.StoreInt64(&s.status, serverStatusStarted)
	return nil
}

func (s *Server) initID() error {
	// query any existing ID in etcd
	resp, err := etcdutil.GetValue(s.client, serverIDPath)
	if err != nil {
		return err
	}

	// use an existed ID
	if len(resp.Kvs) != 0 {
		s.id, err = typeutil.BytesToUint64(resp.Kvs[0].Value)
		return err
	}

	// new an ID
	s.id, err = InitOrGetServerID(s.client, serverIDPath)
	return err
}

func (s *Server) startLoop(ctx context.Context) {
	// TODO
}

// Close closes the server.
func (s *Server) Close() {
	if !atomic.CompareAndSwapInt64(&s.status, serverStatusStarted, serverStatusClosed) {
		// server is already closed
		return
	}
	// TODO stop loop, close etcd, etc.
}

// IsClosed checks whether server is closed or not.
func (s *Server) IsClosed() bool {
	return atomic.LoadInt64(&s.status) == serverStatusClosed
}

func (s *Server) Name() string {
	// TODO
	return ""
}

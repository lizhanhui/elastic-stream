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

package member

import (
	"context"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/election"
	"github.com/AutoMQ/placement-manager/pkg/server/config"
	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
)

const (
	_memberPathPrefix = "member"

	_leaderPathPrefix   = "leader"
	_priorityPathPrefix = "priority"

	_leaderElectionPurpose = "PM leader election"

	_moveLeaderTimeout = 5 * time.Second // The timeout to wait transfer etcd leader to complete
)

// Member is used for the election related logic.
type Member struct {
	leadership *election.Leadership
	leader     atomic.Pointer[Info] // current leader's info

	etcd            *embed.Etcd
	client          *clientv3.Client
	id              uint64 // etcd server id.
	clusterRootPath string // cluster root path in etcd
	rootPath        string // root path in etcd

	// info is current PM's info.
	// It will be serialized and saved in etcd leader key when the PM node
	// is successfully elected as the PM leader of the cluster.
	// Every write will use it to check PM leadership.
	info *Info

	lg *zap.Logger // logger
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64, logger *zap.Logger) *Member {
	return &Member{
		etcd:   etcd,
		client: client,
		id:     id,
		lg:     logger,
	}
}

// Init initializes the member info.
func (m *Member) Init(cfg *config.Config, name string, clusterRootPath string) {
	info := &Info{
		Name:       name,
		MemberID:   m.id,
		ClientUrls: strings.Split(cfg.AdvertiseClientUrls, config.URLSeparator),
		PeerUrls:   strings.Split(cfg.AdvertisePeerUrls, config.URLSeparator),
	}

	m.info = info
	m.clusterRootPath = clusterRootPath
	m.rootPath = path.Join(clusterRootPath, _memberPathPrefix, strconv.FormatUint(m.id, 10))
	m.leadership = election.NewLeadership(m.client, path.Join(m.clusterRootPath, _leaderPathPrefix), _leaderElectionPurpose)
}

// CheckPriorityAndMoveLeader checks whether the etcd leader should be moved according to the priority, and moves if so
func (m *Member) CheckPriorityAndMoveLeader(ctx context.Context) error {
	etcdLeaderID := m.EtcdLeaderID()
	if etcdLeaderID == m.id || etcdLeaderID == 0 {
		return nil
	}
	logger := m.lg

	myPriority, err := m.GetMemberPriority(m.id)
	if err != nil {
		return errors.Wrap(err, "load current member priority")
	}
	leaderPriority, err := m.GetMemberPriority(etcdLeaderID)
	if err != nil {
		return errors.Wrap(err, "load etcd leader member priority")
	}

	if myPriority > leaderPriority {
		err := m.MoveEtcdLeader(ctx, etcdLeaderID, m.id)
		if err != nil {
			return errors.Wrap(err, "transfer etcd leader")
		} else {
			logger.Info("transfer etcd leader", zap.Uint64("from", etcdLeaderID), zap.Uint64("to", m.id))
		}
	}
	return nil
}

func (m *Member) EtcdLeaderID() uint64 {
	return m.etcd.Server.Lead()
}

// GetMemberPriority loads a member's priority to be elected as the etcd leader.
func (m *Member) GetMemberPriority(id uint64) (int, error) {
	key := m.getPriorityPath(id)
	res, err := etcdutil.GetValue(m.client, key)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to get member's leader priority by key %s", key)
	}
	if len(res.Kvs) == 0 {
		return 0, nil
	}

	priority, err := strconv.Atoi(string(res.Kvs[0].Value))
	if err != nil {
		return 0, errors.Wrap(err, "parse priority")
	}
	return priority, nil
}

func (m *Member) getPriorityPath(id uint64) string {
	return path.Join(m.clusterRootPath, _memberPathPrefix, strconv.FormatUint(id, 10), _priorityPathPrefix)
}

// MoveEtcdLeader tries to transfer etcd leader.
func (m *Member) MoveEtcdLeader(ctx context.Context, old, new uint64) error {
	moveCtx, cancel := context.WithTimeout(ctx, _moveLeaderTimeout)
	defer cancel()

	err := m.etcd.Server.MoveLeader(moveCtx, old, new)
	if err != nil {
		return errors.Wrap(err, "move leader")
	}
	return nil
}

func (m *Member) IsLeader() bool {
	return m.leadership.Check() && m.Leader().MemberID == m.info.MemberID
}

// Leader returns current PM leader of PM cluster.
func (m *Member) Leader() *Info {
	leader := m.leader.Load()
	if leader == nil {
		return nil
	}
	if leader.MemberID == 0 {
		return nil
	}
	return leader
}

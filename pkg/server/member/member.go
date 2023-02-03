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
	"path"
	"strings"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/AutoMQ/placement-manager/pkg/election"
	"github.com/AutoMQ/placement-manager/pkg/server/config"
)

const (
	_leaderPathPrefix      = "leader"
	_leaderElectionPurpose = "PM leader election"
)

// Member is used for the election related logic.
type Member struct {
	leadership *election.Leadership
	leader     atomic.Pointer[Info] // current leader's info

	etcd     *embed.Etcd
	client   *clientv3.Client
	id       uint64 // etcd server id.
	rootPath string // root path in etcd

	// info is current PM's info.
	// It will be serialized and saved in etcd leader key when the PM node
	// is successfully elected as the PM leader of the cluster.
	// Every write will use it to check PM leadership.
	info *Info
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64) *Member {
	return &Member{
		etcd:   etcd,
		client: client,
		id:     id,
	}
}

// Init initializes the member info.
func (m *Member) Init(cfg *config.Config, name string, rootPath string) {
	info := &Info{
		Name:       name,
		MemberID:   m.id,
		ClientUrls: strings.Split(cfg.AdvertiseClientUrls, config.URLSeparator),
		PeerUrls:   strings.Split(cfg.AdvertisePeerUrls, config.URLSeparator),
	}

	m.info = info
	m.rootPath = rootPath
	m.leadership = election.NewLeadership(m.client, path.Join(m.rootPath, _leaderPathPrefix), _leaderElectionPurpose)
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

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
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"

	"github.com/AutoMQ/placement-manager/pkg/server/config"
)

// Member is used for the election related logic.
type Member struct {
	// TODO
}

// NewMember create a new Member.
func NewMember(etcd *embed.Etcd, client *clientv3.Client, id uint64) *Member {
	// TODO
	return &Member{}
}

// Init initializes the member info.
func (m *Member) Init(cfg *config.Config, name string, rootPath string) {
	// TODO
}

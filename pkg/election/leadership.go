// Copyright 2020 TiKV Project Authors.
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

package election

import (
	"context"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// Leadership is used to manage the leadership campaigning.
type Leadership struct {
	purpose string                // purpose is used to show what this election for
	lease   atomic.Pointer[lease] // The lease which is used to get this leadership
	client  *clientv3.Client

	// leaderKey and leaderValue are key-value pair in etcd
	leaderKey   string
	leaderValue string

	keepAliveCtx        context.Context
	keepAliveCancelFunc context.CancelFunc
}

// NewLeadership creates a new Leadership.
func NewLeadership(client *clientv3.Client, leaderKey, purpose string) *Leadership {
	leadership := &Leadership{
		purpose:   purpose,
		client:    client,
		leaderKey: leaderKey,
	}
	return leadership
}

// Check returns whether the leadership is still available.
func (ls *Leadership) Check() bool {
	return ls.getLease() != nil && !ls.getLease().IsExpired()
}

// getLease gets the lease of leadership, only if leadership is valid,
// i.e. the owner is a true leader, the lease is not nil.
func (ls *Leadership) getLease() *lease {
	l := ls.lease.Load()
	if l == nil {
		return nil
	}
	return l
}

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

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
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

	lg *zap.Logger
}

// NewLeadership creates a new Leadership.
func NewLeadership(client *clientv3.Client, leaderKey, purpose string, logger *zap.Logger) *Leadership {
	leadership := &Leadership{
		purpose:   purpose,
		client:    client,
		leaderKey: leaderKey,
		lg:        logger,
	}
	return leadership
}

// DeleteLeaderKey deletes the corresponding leader from etcd by the leaderPath as the key.
func (ls *Leadership) DeleteLeaderKey() error {
	logger := ls.lg

	resp, err := etcdutil.NewTxn(ls.client).Then(clientv3.OpDelete(ls.leaderKey)).Commit()
	if err != nil {
		return errors.Wrap(err, "delete etcd key")
	}
	if !resp.Succeeded {
		return errors.New("failed to delete etcd key: transaction failed")
	}

	// Reset the lease as soon as possible.
	ls.Reset()
	logger.Info("delete the leader key ok", zap.String("leaderPath", ls.leaderKey), zap.String("purpose", ls.purpose))
	return nil
}

// Reset does some defer jobs such as closing lease, resetting lease etc.
func (ls *Leadership) Reset() {
	if ls.getLease() == nil {
		return
	}
	if ls.keepAliveCancelFunc != nil {
		ls.keepAliveCancelFunc()
	}
	if l := ls.getLease(); l != nil {
		l.Close()
	}
}

// Check returns whether the leadership is still available.
func (ls *Leadership) Check() bool {
	l := ls.getLease()
	return l != nil && !l.IsExpired()
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

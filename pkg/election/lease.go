// Copyright 2019 TiKV Project Authors.
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
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// lease is used as the low-level mechanism for campaigning and renewing elected leadership.
// The way to gain and maintain leadership is to update and keep the lease alive continuously.
type lease struct {
	Purpose string // purpose is used to show what this election for

	// etcd client and lease
	client *clientv3.Client
	lease  clientv3.Lease
	ID     clientv3.LeaseID

	// leaseTimeout and expireTime are used to control the lease's lifetime
	leaseTimeout time.Duration
	expireTime   atomic.Pointer[time.Time]
}

// IsExpired checks if the lease is expired. If it returns true,
// current leader should step down and try to re-elect again.
func (l *lease) IsExpired() bool {
	if l.expireTime.Load() == nil {
		return true
	}
	return time.Now().After(*l.expireTime.Load())
}

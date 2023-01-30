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
	"math/rand"
	"time"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
	"github.com/AutoMQ/placement-manager/pkg/util/typeutil"
)

// InitOrGetServerID tries to save a new ID to etcd. If failed (due to another PM
// save an ID at the same time), InitOrGetServerID get the saved ID.
func InitOrGetServerID(c *clientv3.Client, key string) (uint64, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), etcdutil.DefaultRequestTimeout)
	defer cancel()

	// Generate a random server ID.
	ts := uint64(time.Now().Unix())
	ID := (ts << 32) + uint64(rand.Uint32())
	value := typeutil.Uint64ToBytes(ID)

	// Multiple PDs may try to init the server ID at the same time.
	// Only one PD can commit this transaction, then other PDs can get
	// the committed server ID.
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(value))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return 0, err
	}

	// Txn commits ok, return the generated server ID.
	if resp.Succeeded {
		return ID, nil
	}

	// Otherwise, parse the committed server ID.
	if len(resp.Responses) == 0 {
		return 0, errors.New("etcd transaction failed, conflicted and rolled back")
	}
	response := resp.Responses[0].GetResponseRange()
	if response == nil || len(response.Kvs) != 1 {
		return 0, errors.New("etcd transaction failed, conflicted and rolled back")
	}
	return typeutil.BytesToUint64(response.Kvs[0].Value)
}

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

package etcdutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/goleak"

	"github.com/AutoMQ/placement-manager/pkg/util/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestGetValue(t *testing.T) {
	t.Parallel()
	re := require.New(t)
	etcd, client := prepare(re, t)

	tests := []struct {
		inKeys        []string
		inValues      []string
		key           string
		opts          []clientv3.OpOption
		outKeys       []string
		outValues     []string
		expectedError bool
	}{
		{
			inKeys:        []string{"key1"},
			inValues:      []string{"val1"},
			key:           "key1",
			opts:          []clientv3.OpOption{},
			outKeys:       []string{"key1"},
			outValues:     []string{"val1"},
			expectedError: false,
		},
	}
	t.Log("start test")

	for _, rt := range tests {
		t.Run("name", func(t *testing.T) {
			// prepare
			kv := clientv3.NewKV(client)
			for i := range rt.inKeys {
				t.Log(i)
				_, err := kv.Put(context.TODO(), rt.inKeys[i], rt.inValues[i])
				re.NoError(err)
			}

			// run
			resp, err := GetValue(client, rt.key, rt.opts...)

			// check
			if rt.expectedError {
				re.Error(err)
			} else {
				re.NoError(err)
				for i := range resp.Kvs {
					re.Equal(rt.outKeys[i], string(resp.Kvs[i].Key))
					re.Equal(rt.outValues[i], string(resp.Kvs[i].Value))
				}
			}
		})
	}

	etcd.Close()

}

func prepare(re *require.Assertions, t *testing.T) (*embed.Etcd, *clientv3.Client) {
	// start etcd
	cfg := testutil.NewEtcdConfig(t)
	etcd, err := embed.StartEtcd(cfg)
	defer func() {
		etcd.Close()
	}()
	re.NoError(err)

	// new client
	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()
	t.Log("ready")

	return etcd, client
}

// Copyright 2022 TiKV Project Authors.
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

package storage

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/server/storage/endpoint"
	"github.com/AutoMQ/placement-manager/pkg/server/storage/kv"
)

// Etcd is a storage based on etcd.
type Etcd struct {
	*endpoint.Endpoint
}

// NewEtcd creates a new etcd storage.
func NewEtcd(client *clientv3.Client, rootPath string, lg *zap.Logger, cmpFunc func() clientv3.Cmp) *Etcd {
	storageLg := lg.With(zap.String("etcd-storage-root-path", rootPath))
	kvLg := lg.With(zap.String("etcd-kv-root-path", rootPath))
	return &Etcd{
		endpoint.NewEndpoint(kv.NewEtcd(client, rootPath, kvLg, cmpFunc), storageLg),
	}
}

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

package kv

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Etcd is a kv based on etcd.
type Etcd struct {
	client   *clientv3.Client
	rootPath string
}

// NewEtcd creates a new etcd kv.
func NewEtcd(client *clientv3.Client, rootPath string) *Etcd {
	return &Etcd{
		client:   client,
		rootPath: rootPath,
	}
}

func (e *Etcd) Get(key string) (string, error) {
	//TODO implement me
	panic("implement me")
}

func (e *Etcd) GetByRange(r Range, limit int) (kvs []KeyValue, err error) {
	//TODO implement me
	panic("implement me")
}

func (e *Etcd) Put(key, value string) error {
	//TODO implement me
	panic("implement me")
}

func (e *Etcd) Delete(key string) error {
	//TODO implement me
	panic("implement me")
}

func (e *Etcd) GetPrefixRangeEnd(prefix string) string {
	//TODO implement me
	panic("implement me")
}

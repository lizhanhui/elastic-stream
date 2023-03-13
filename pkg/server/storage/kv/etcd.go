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
	"bytes"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
)

// Etcd is a kv based on etcd.
type Etcd struct {
	client     *clientv3.Client
	rootPath   []byte
	newTxnFunc func() clientv3.Txn
}

// NewEtcd creates a new etcd kv.
// If newTxnFunc is nil, it will use etcdutil.NewTxn.
func NewEtcd(client *clientv3.Client, rootPath string, newTxnFunc func() clientv3.Txn) *Etcd {
	e := &Etcd{
		client:     client,
		rootPath:   []byte(rootPath),
		newTxnFunc: newTxnFunc,
	}
	if e.newTxnFunc == nil {
		e.newTxnFunc = func() clientv3.Txn { return etcdutil.NewTxn(e.client) }
	}
	return e
}

func (e *Etcd) Get(k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}
	key := e.addPrefix(k)

	kv, err := etcdutil.GetOne(e.client, key)
	if err != nil {
		return nil, errors.Wrap(err, "kv get")
	}

	return kv.Value, nil
}

func (e *Etcd) GetByRange(r Range, limit int64) ([]KeyValue, error) {
	if len(r.StartKey) == 0 {
		return nil, nil
	}

	startKey := e.addPrefix(r.StartKey)
	endKey := e.addPrefix(r.EndKey)

	resp, err := etcdutil.Get(e.client, startKey, clientv3.WithRange(string(endKey)), clientv3.WithLimit(limit))
	if err != nil {
		return nil, errors.Wrap(err, "kv get by range")
	}

	kvs := make([]KeyValue, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		kvs = append(kvs, KeyValue{
			Key:   e.trimPrefix(kv.Key),
			Value: kv.Value,
		})
	}
	return kvs, nil
}

func (e *Etcd) Put(k, v []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}
	key := e.addPrefix(k)

	resp, err := etcdutil.Put(e.client, key, v, clientv3.WithPrevKV())
	if err != nil {
		return nil, errors.Wrap(err, "kv put")
	}

	var prevValue []byte
	if resp.PrevKv != nil {
		prevValue = resp.PrevKv.Value
	}
	return prevValue, nil
}

func (e *Etcd) Delete(k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}
	key := e.addPrefix(k)

	resp, err := etcdutil.Delete(e.client, key, clientv3.WithPrevKV())
	if err != nil {
		return nil, errors.Wrap(err, "kv delete")
	}

	var prevValue []byte
	for _, kv := range resp.PrevKvs {
		if bytes.Equal(kv.Key, key) {
			prevValue = kv.Value
			break
		}
	}
	return prevValue, nil
}

func (e *Etcd) GetPrefixRangeEnd(p []byte) []byte {
	prefix := e.addPrefix(p)
	return []byte(clientv3.GetPrefixRangeEnd(string(prefix)))
}

func (e *Etcd) addPrefix(k []byte) []byte {
	return bytes.Join([][]byte{e.rootPath, k}, []byte(KeySeparator))
}

func (e *Etcd) trimPrefix(k []byte) []byte {
	return k[len(e.rootPath)+len(KeySeparator):]
}

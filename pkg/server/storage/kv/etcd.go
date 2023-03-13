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
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
)

var (
	// ErrTxnFailed is the error when etcd transaction failed.
	ErrTxnFailed = errors.New("etcd transaction failed")
)

// Etcd is a kv based on etcd.
type Etcd struct {
	client     *clientv3.Client
	rootPath   []byte
	newTxnFunc func() clientv3.Txn

	lg *zap.Logger
}

// NewEtcd creates a new etcd kv.
// If newTxnFunc is nil, it will use etcdutil.NewTxn.
func NewEtcd(client *clientv3.Client, rootPath string, newTxnFunc func() clientv3.Txn, lg *zap.Logger) *Etcd {
	e := &Etcd{
		client:     client,
		rootPath:   []byte(rootPath),
		newTxnFunc: newTxnFunc,
		lg:         lg.With(zap.String("etcd-kv-root-path", rootPath)),
	}
	if e.newTxnFunc == nil {
		e.newTxnFunc = func() clientv3.Txn { return etcdutil.NewTxn(e.client, lg) }
	}
	return e
}

func (e *Etcd) Get(k []byte) ([]byte, error) {
	if len(k) == 0 {
		return nil, nil
	}
	logger := e.lg

	key := e.addPrefix(k)

	kv, err := etcdutil.GetOne(e.client, key, logger)
	if err != nil {
		return nil, errors.Wrap(err, "kv get")
	}

	return kv.Value, nil
}

func (e *Etcd) GetByRange(r Range, limit int64) ([]KeyValue, error) {
	if len(r.StartKey) == 0 {
		return nil, nil
	}
	logger := e.lg

	startKey := e.addPrefix(r.StartKey)
	endKey := e.addPrefix(r.EndKey)

	resp, err := etcdutil.Get(e.client, startKey, logger, clientv3.WithRange(string(endKey)), clientv3.WithLimit(limit))
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

func (e *Etcd) Put(k, v []byte, prevKV bool) ([]byte, error) {
	prevKvs, err := e.BatchPut([]KeyValue{{Key: k, Value: v}}, prevKV)
	if err != nil {
		return nil, errors.Wrap(err, "kv put")
	}

	if !prevKV {
		return nil, nil
	}
	return prevKvs[0].Value, nil
}

func (e *Etcd) BatchPut(kvs []KeyValue, prevKV bool) ([]KeyValue, error) {
	if len(kvs) == 0 {
		return nil, nil
	}

	ops := make([]clientv3.Op, 0, len(kvs))
	var opts []clientv3.OpOption
	if prevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	for _, kv := range kvs {
		key := e.addPrefix(kv.Key)
		ops = append(ops, clientv3.OpPut(string(key), string(kv.Value), opts...))
	}

	txn := e.newTxnFunc().Then(ops...)
	resp, err := txn.Commit()
	if err != nil {
		return nil, errors.Wrap(err, "kv batch put")
	}
	if !resp.Succeeded {
		return nil, ErrTxnFailed
	}

	if !prevKV {
		return nil, nil
	}
	prevKvs := make([]KeyValue, 0, len(resp.Responses))
	for _, resp := range resp.Responses {
		putResp := resp.GetResponsePut()
		prevKvs = append(prevKvs, KeyValue{
			Key:   e.trimPrefix(putResp.PrevKv.Key),
			Value: putResp.PrevKv.Value,
		})
	}
	return prevKvs, nil
}

func (e *Etcd) Delete(k []byte, prevKV bool) ([]byte, error) {
	prevKvs, err := e.BatchDelete([]KeyValue{{Key: k}}, prevKV)
	if err != nil {
		return nil, errors.Wrap(err, "kv delete")
	}

	if !prevKV {
		return nil, nil
	}
	return prevKvs[0].Value, nil
}

func (e *Etcd) BatchDelete(kvs []KeyValue, prevKV bool) ([]KeyValue, error) {
	if len(kvs) == 0 {
		return nil, nil
	}

	ops := make([]clientv3.Op, 0, len(kvs))
	var opts []clientv3.OpOption
	if prevKV {
		opts = append(opts, clientv3.WithPrevKV())
	}
	for _, kv := range kvs {
		key := e.addPrefix(kv.Key)
		ops = append(ops, clientv3.OpDelete(string(key), opts...))
	}

	txn := e.newTxnFunc().Then(ops...)
	resp, err := txn.Commit()
	if err != nil {
		return nil, errors.Wrap(err, "kv batch delete")
	}
	if !resp.Succeeded {
		return nil, ErrTxnFailed
	}

	if !prevKV {
		return nil, nil
	}
	prevKvs := make([]KeyValue, 0, len(resp.Responses))
	for _, resp := range resp.Responses {
		deleteResp := resp.GetResponseDeleteRange()
		for _, kv := range deleteResp.PrevKvs {
			prevKvs = append(prevKvs, KeyValue{
				Key:   e.trimPrefix(kv.Key),
				Value: kv.Value,
			})
		}
	}
	return prevKvs, nil
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

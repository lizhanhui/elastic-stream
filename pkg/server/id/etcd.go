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

package id

import (
	"context"
	"strings"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
	"github.com/AutoMQ/placement-manager/pkg/util/traceutil"
)

const (
	_keySeparator = "/"
)

// EtcdAllocator is an allocator based on etcd.
type EtcdAllocator struct {
	mu   sync.Mutex
	base uint64
	end  uint64
	step uint64

	client     *clientv3.Client
	path       []byte
	newTxnFunc func(ctx context.Context) clientv3.Txn

	lg *zap.Logger
}

// EtcdAllocatorParam is the parameter for creating a new etcd allocator.
type EtcdAllocatorParam struct {
	Client   *clientv3.Client
	CmpFunc  func() clientv3.Cmp // CmpFunc is used to create a transaction. If CmpFunc is nil, the transaction will not have any additional condition.
	RootPath string              // RootPath is the prefix of all keys in etcd.
	Key      string              // Key is the unique key to identify the allocator.
	Step     uint64              // Step is the step to allocate the next ID. If Step is 0, it will be set to _defaultStep.
}

// NewEtcdAllocator creates a new etcd allocator.
func NewEtcdAllocator(param *EtcdAllocatorParam, lg *zap.Logger) *EtcdAllocator {
	e := &EtcdAllocator{
		client: param.Client,
		path:   []byte(strings.Join([]string{param.RootPath, param.Key}, _keySeparator)),
		lg:     lg,
		step:   param.Step,
	}

	if e.step == 0 {
		e.step = _defaultStep
	}

	if param.CmpFunc != nil {
		e.newTxnFunc = func(ctx context.Context) clientv3.Txn {
			// cmpFunc should be evaluated lazily.
			return etcdutil.NewTxn(ctx, param.Client, lg.With(traceutil.TraceLogField(ctx))).If(param.CmpFunc())
		}
	} else {
		e.newTxnFunc = func(ctx context.Context) clientv3.Txn {
			return etcdutil.NewTxn(ctx, param.Client, lg.With(traceutil.TraceLogField(ctx)))
		}
	}
	return e
}

func (e *EtcdAllocator) Alloc() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (e *EtcdAllocator) AllocN(n uint) ([]uint64, error) {
	//TODO implement me
	panic("implement me")
}

func (e *EtcdAllocator) Rebase(base uint64, force bool) error {
	//TODO implement me
	panic("implement me")
}

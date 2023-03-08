// Copyright 2017 TiKV Project Authors.
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
	"fmt"
	"path"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/AutoMQ/placement-manager/api/kvpb"
	"github.com/AutoMQ/placement-manager/pkg/util/etcdutil"
)

// GrpcServer wraps Server to provide grpc service.
type GrpcServer struct {
	*Server
}

const _globalConfigPath = "/global/config/"

// StoreGlobalConfig store global config into etcd by transaction
func (s *GrpcServer) StoreGlobalConfig(_ context.Context, request *kvpb.StoreGlobalConfigRequest) (*kvpb.StoreGlobalConfigResponse, error) {
	configPath := request.GetConfigPath()
	if configPath == "" {
		configPath = _globalConfigPath
	}
	ops := make([]clientv3.Op, len(request.Changes))
	for i, item := range request.Changes {
		name := path.Join(configPath, item.GetName())
		switch item.GetKind() {
		case kvpb.EventType_PUT:
			value := string(item.GetPayload())
			ops[i] = clientv3.OpPut(name, value)
		case kvpb.EventType_DELETE:
			ops[i] = clientv3.OpDelete(name)
		}
	}
	res, err :=
		etcdutil.NewTxn(s.client).Then(ops...).Commit()
	if err != nil {
		return &kvpb.StoreGlobalConfigResponse{}, err
	}
	if !res.Succeeded {
		return &kvpb.StoreGlobalConfigResponse{}, errors.New("failed to execute StoreGlobalConfig transaction")
	}
	return &kvpb.StoreGlobalConfigResponse{}, nil
}

// LoadGlobalConfig support 2 ways to load global config from etcd
// - `Names` iteratively get value from `ConfigPath/Name` but not care about revision
// - `ConfigPath` if `Names` is nil can get all values and revision of current path
func (s *GrpcServer) LoadGlobalConfig(ctx context.Context, request *kvpb.LoadGlobalConfigRequest) (*kvpb.LoadGlobalConfigResponse, error) {
	configPath := request.GetConfigPath()
	if configPath == "" {
		configPath = _globalConfigPath
	}
	if request.Names != nil {
		res := make([]*kvpb.GlobalConfigItem, len(request.Names))
		for i, name := range request.Names {
			r, err := s.client.Get(ctx, path.Join(configPath, name))
			switch {
			case err != nil:
				res[i] = &kvpb.GlobalConfigItem{Name: name, Error: &kvpb.Error{Type: kvpb.ErrorType_UNKNOWN, Message: err.Error()}}
			case len(r.Kvs) == 0:
				res[i] = &kvpb.GlobalConfigItem{Name: name, Error: &kvpb.Error{Type: kvpb.ErrorType_NOT_FOUND, Message: fmt.Sprintf("key %s not found", name)}}
			default:
				res[i] = &kvpb.GlobalConfigItem{Name: name, Payload: r.Kvs[0].Value, Kind: kvpb.EventType_PUT}
			}
		}
		return &kvpb.LoadGlobalConfigResponse{Items: res}, nil
	}
	r, err := s.client.Get(ctx, configPath, clientv3.WithPrefix())
	if err != nil {
		return &kvpb.LoadGlobalConfigResponse{}, err
	}
	res := make([]*kvpb.GlobalConfigItem, len(r.Kvs))
	for i, value := range r.Kvs {
		res[i] = &kvpb.GlobalConfigItem{Kind: kvpb.EventType_PUT, Name: string(value.Key), Payload: value.Value}
	}
	return &kvpb.LoadGlobalConfigResponse{Items: res, Revision: r.Header.GetRevision()}, nil
}

// WatchGlobalConfig if the connection of WatchGlobalConfig is end
// or stopped by whatever reason, just reconnect to it.
// Watch on revision which greater than or equal to the required revision.
func (s *GrpcServer) WatchGlobalConfig(req *kvpb.WatchGlobalConfigRequest, server kvpb.KV_WatchGlobalConfigServer) error {
	ctx, cancel := context.WithCancel(s.Context())
	defer cancel()
	configPath := req.GetConfigPath()
	if configPath == "" {
		configPath = _globalConfigPath
	}
	revision := req.GetRevision()
	// If the revision is compacted, will meet required revision has been compacted error.
	// - If required revision < CompactRevision, we need to reload all configs to avoid losing data.
	// - If required revision >= CompactRevision, just keep watching.
	watchChan := s.client.Watch(ctx, configPath, clientv3.WithPrefix(), clientv3.WithRev(revision))
	for {
		select {
		case <-ctx.Done():
			return nil
		case res := <-watchChan:
			if revision < res.CompactRevision {
				if err := server.Send(&kvpb.WatchGlobalConfigResponse{
					Revision: res.CompactRevision,
					Error: &kvpb.Error{
						Type:    kvpb.ErrorType_DATA_COMPACTED,
						Message: fmt.Sprintf("required watch revision: %d is smaller than current compact/min revision. %d", revision, res.CompactRevision),
					},
				}); err != nil {
					return err
				}
			}
			revision = res.Header.GetRevision()

			cfgs := make([]*kvpb.GlobalConfigItem, 0, len(res.Events))
			for _, e := range res.Events {
				cfgs = append(cfgs, &kvpb.GlobalConfigItem{Name: string(e.Kv.Key), Payload: e.Kv.Value, Kind: kvpb.EventType(e.Type)})
			}
			if len(cfgs) > 0 {
				if err := server.Send(&kvpb.WatchGlobalConfigResponse{Changes: cfgs, Revision: res.Header.GetRevision()}); err != nil {
					return err
				}
			}
		}
	}
}

//go:build testing

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

package testutil

import (
	"fmt"
	"net/url"
	"testing"

	"go.etcd.io/etcd/server/v3/embed"

	tempurl "github.com/AutoMQ/pd/pkg/util/test/url"
)

// NewEtcdConfig is used to create an etcd config for the unit test purpose.
func NewEtcdConfig(tb testing.TB) *embed.Config {
	cfg := embed.NewConfig()
	cfg.Name = "test_etcd"
	cfg.Dir = tb.TempDir()
	cfg.WalDir = ""
	cfg.Logger = "zap"
	cfg.LogOutputs = []string{"stdout"}

	pu, _ := url.Parse(tempurl.Alloc(tb))
	cfg.ListenPeerUrls = []url.URL{*pu}
	cfg.AdvertisePeerUrls = cfg.ListenPeerUrls
	cu, _ := url.Parse(tempurl.Alloc(tb))
	cfg.ListenClientUrls = []url.URL{*cu}
	cfg.AdvertiseClientUrls = cfg.ListenClientUrls

	cfg.StrictReconfigCheck = false
	cfg.InitialCluster = fmt.Sprintf("%s=%s", cfg.Name, &cfg.ListenPeerUrls[0])
	cfg.ClusterState = embed.ClusterStateFlagNew

	cfg.LogLevel = "error"
	return cfg
}

// NewEtcdConfigs is used to create multiple etcd configs for the unit test purpose.
func NewEtcdConfigs(tb testing.TB, cnt int) (cs []*embed.Config) {
	var initialCluster string
	for i := 0; i < cnt; i++ {
		cfg := NewEtcdConfig(tb)
		cfg.Name = fmt.Sprintf("test_etcd_%d", i)
		initialCluster += fmt.Sprintf(",%s=%s", cfg.Name, &cfg.ListenPeerUrls[0])
		cs = append(cs, cfg)
	}
	for _, cfg := range cs {
		cfg.InitialCluster = initialCluster[1:]
	}
	return
}

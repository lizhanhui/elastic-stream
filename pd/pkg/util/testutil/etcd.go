//go:build testing

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func StartEtcd(tb testing.TB, cfg *embed.Config) (*embed.Etcd, *clientv3.Client, func()) {
	re := require.New(tb)

	// start etcd
	if cfg == nil {
		cfg = NewEtcdConfig(tb)
	}
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)

	// new client
	ep := cfg.ListenClientUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
		Logger:    zap.NewNop(),
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	return etcd, client, func() { _ = client.Close(); etcd.Close() }
}

// StartEtcdCluster starts a etcd cluster with cnt nodes.
func StartEtcdCluster(tb testing.TB, cnt int) ([]*embed.Etcd, *clientv3.Client, func()) {
	re := require.New(tb)

	// start etcd cluster
	etcds := make([]*embed.Etcd, cnt)
	cs := NewEtcdConfigs(tb, cnt)
	for i, cfg := range cs {
		etcd, err := embed.StartEtcd(cfg)
		re.NoError(err)
		etcds[i] = etcd
	}

	// new client
	eps := make([]string, cnt)
	for i := 0; i < cnt; i++ {
		eps[i] = cs[i].ListenClientUrls[0].String()
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints: eps,
		Logger:    zap.NewNop(),
	})
	re.NoError(err)

	// wait for all nodes ready
	for _, etcd := range etcds {
		<-etcd.Server.ReadyNotify()
	}

	free := func() {
		_ = client.Close()
		for _, etcd := range etcds {
			etcd.Close()
		}
	}
	return etcds, client, free
}

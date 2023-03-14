package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func StartEtcd(re *require.Assertions, tb testing.TB) (*embed.Etcd, *clientv3.Client, func()) {
	// start etcd
	cfg := NewEtcdConfig(tb)
	etcd, err := embed.StartEtcd(cfg)
	re.NoError(err)

	// new client
	ep := cfg.LCUrls[0].String()
	client, err := clientv3.New(clientv3.Config{
		Endpoints: []string{ep},
	})
	re.NoError(err)

	<-etcd.Server.ReadyNotify()

	return etcd, client, func() { _ = client.Close(); etcd.Close() }
}

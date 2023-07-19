package server

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/pkg/server/config"
	tempurl "github.com/AutoMQ/pd/pkg/util/testutil/url"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestStartSingleServer(t *testing.T) {
	re := require.New(t)

	svr := startServer(t, "test-pd", "", tempurl.Alloc(t))
	defer svr.Close()

	re.True(pool(svr.Member().IsLeader, 10, time.Millisecond*100))
}

func TestStartMultiServer(t *testing.T) {
	re := require.New(t)

	peerUrls := make([]string, 3)
	names := make([]string, 3)
	var initialCluster string
	for i := 0; i < 3; i++ {
		peerUrls[i] = tempurl.Alloc(t)
		names[i] = fmt.Sprintf("test-pd-%d", i)
		initialCluster += fmt.Sprintf("%s=%s,", names[i], peerUrls[i])
	}

	svrs := make([]*Server, 3)
	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			svrs[i] = startServer(t, names[i], initialCluster, peerUrls[i])
			wg.Done()
		}(i)
	}

	defer func() {
		for i := 0; i < 3; i++ {
			svrs[i].Close()
		}
	}()

	wg.Wait()
	anyLeader := func() bool {
		for i := 0; i < 3; i++ {
			if svrs[i].Member().IsLeader() {
				return true
			}
		}
		return false
	}
	re.True(pool(anyLeader, 10, time.Millisecond*100))
}

func startServer(tb testing.TB, name string, initialCluster string, peerURL string) *Server {
	re := require.New(tb)

	args := []string{
		"--name=" + name,
		"--data-dir=" + tb.TempDir(),
		"--peer-urls=" + peerURL,
		"--client-urls=" + tempurl.Alloc(tb),
		"--pd-addr=" + tempurl.AllocAddr(tb),
		"--etcd-log-level=error",
	}
	if initialCluster != "" {
		args = append(args, "--initial-cluster="+initialCluster)
	}

	cfg, err := config.NewConfig(args, io.Discard)
	re.NoError(err)
	err = cfg.Adjust()
	re.NoError(err)
	err = cfg.Validate()
	re.NoError(err)

	svr, err := NewServer(context.Background(), cfg, zap.NewNop())
	re.NoError(err)
	err = svr.Start()
	re.NoError(err)

	return svr
}

func pool(f func() bool, maxAttempts int, interval time.Duration) bool {
	for i := 0; i < maxAttempts; i++ {
		if f() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}

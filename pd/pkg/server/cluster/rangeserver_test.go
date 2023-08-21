package cluster

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/cluster/cache"
	"github.com/AutoMQ/pd/pkg/server/config"
)

func TestRaftCluster_chooseRangeServers(t *testing.T) {
	type args struct {
		cnt           int
		grayServerIDs []int32
	}
	type want struct {
		serverIDs []int32
	}
	tests := []struct {
		name    string
		args    args
		want    want
		wantErr bool
		errMsg  string
	}{
		{
			name: "normal case",
			args: args{
				cnt: 2,
			},
			want: want{
				serverIDs: []int32{1, 2},
			},
		},
		{
			name: "skip gray server",
			args: args{
				cnt:           1,
				grayServerIDs: []int32{1},
			},
			want: want{
				serverIDs: []int32{2},
			},
		},
		{
			name: "use gray server if no other server",
			args: args{
				cnt:           2,
				grayServerIDs: []int32{1},
			},
			want: want{
				serverIDs: []int32{1, 2},
			},
		},
		{
			name: "not enough server",
			args: args{
				cnt: 3,
			},
			wantErr: true,
			errMsg:  "not enough range servers",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			cfg := config.NewCluster()
			cfg.RangeServerTimeout = 1 * time.Minute
			cluster := NewRaftCluster(context.Background(), cfg, nil, zap.NewNop())
			tActive := time.Now()
			tInactive := tActive.Add(-2 * time.Minute)
			// Two active range server
			cluster.cache.SaveRangeServer(&cache.RangeServer{
				RangeServerT:   rpcfb.RangeServerT{ServerId: 1, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE},
				LastActiveTime: &tActive,
			})
			cluster.cache.SaveRangeServer(&cache.RangeServer{
				RangeServerT:   rpcfb.RangeServerT{ServerId: 2, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE},
				LastActiveTime: &tActive,
			})
			// An offline range server
			cluster.cache.SaveRangeServer(&cache.RangeServer{
				RangeServerT:   rpcfb.RangeServerT{ServerId: 3, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_OFFLINE},
				LastActiveTime: &tActive,
			})
			// An inactive range server
			cluster.cache.SaveRangeServer(&cache.RangeServer{
				RangeServerT:   rpcfb.RangeServerT{ServerId: 4, State: rpcfb.RangeServerStateRANGE_SERVER_STATE_READ_WRITE},
				LastActiveTime: &tInactive,
			})

			// tt.args.grayServerIDs into set
			grayServerIDs := mapset.NewThreadUnsafeSet(tt.args.grayServerIDs...)
			servers, err := cluster.chooseRangeServers(tt.args.cnt, grayServerIDs, zap.NewNop())
			if tt.wantErr {
				re.ErrorContains(err, tt.errMsg)
				return
			}
			re.NoError(err)
			got := make([]int32, 0, len(servers))
			for _, s := range servers {
				got = append(got, s.ServerId)
			}
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
			re.Equal(tt.want.serverIDs, got)
		})
	}
}

// TestRaftCluster_fillRangeServersInfo will fail if there are new fields in rpcfb.RangeServerT
func TestRaftCluster_fillRangeServersInfo(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	var rangeServer rpcfb.RangeServerT
	_ = gofakeit.New(1).Struct(&rangeServer)
	cluster := NewRaftCluster(context.Background(), nil, nil, zap.NewNop())
	cluster.cache.SaveRangeServer(&cache.RangeServer{
		RangeServerT: rangeServer,
	})

	rangeServer2 := rpcfb.RangeServerT{
		ServerId: rangeServer.ServerId,
	}
	cluster.fillRangeServerInfo(&rangeServer2)

	re.Equal(rangeServer, rangeServer2)
}

// Test_eraseRangeServersInfo will fail if there are new fields in rpcfb.RangeServerT
func Test_eraseRangeServersInfo(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	var rangeServer rpcfb.RangeServerT
	_ = gofakeit.New(1).Struct(&rangeServer)

	servers := eraseRangeServersInfo([]*rpcfb.RangeServerT{&rangeServer})

	// `AdvertiseAddr` should not be copied
	rangeServer.AdvertiseAddr = ""
	rangeServer.State = 0
	re.Equal(rangeServer, *servers[0])

	// returned servers should be a copy
	rangeServer.AdvertiseAddr = "modified"
	re.Equal("", servers[0].AdvertiseAddr)
}

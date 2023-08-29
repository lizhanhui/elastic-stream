package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/storage"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
)

func TestRaftCluster_listRangeOnRangeServer_Timeout(t *testing.T) {
	t.Parallel()
	// FIXME: #1049
	t.SkipNow()
	re := require.New(t)

	mockCtrl := gomock.NewController(t)
	s := storage.NewMockStorage(mockCtrl)
	s.EXPECT().ForEachRangeServer(gomock.Any(), gomock.Any())
	s.EXPECT().ListResource(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ int64, _ endpoint.ContinueToken, _ int32) (_ []*rpcfb.ResourceT, _ int64, _ endpoint.ContinueToken, _ error) {
			time.Sleep(100 * time.Millisecond)
			return
		},
	)
	c := NewRaftCluster(context.Background(), nil, nil, zap.NewNop())
	c.storage = s
	err := c.loadInfo(context.Background())
	re.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	ranges, err := c.listRangeOnRangeServer(ctx, 0)

	re.Empty(ranges)
	re.ErrorIs(err, context.DeadlineExceeded)
}

func TestRaftCluster_listRangeOnRangeServerInStream_Timeout(t *testing.T) {
	t.Parallel()
	// FIXME: #1049
	t.SkipNow()
	re := require.New(t)

	mockCtrl := gomock.NewController(t)
	s := storage.NewMockStorage(mockCtrl)
	s.EXPECT().ForEachRangeServer(gomock.Any(), gomock.Any())
	s.EXPECT().ListResource(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ int64, _ endpoint.ContinueToken, _ int32) (_ []*rpcfb.ResourceT, _ int64, _ endpoint.ContinueToken, _ error) {
			time.Sleep(100 * time.Millisecond)
			return
		},
	)
	c := NewRaftCluster(context.Background(), nil, nil, zap.NewNop())
	c.storage = s
	err := c.loadInfo(context.Background())
	re.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	ranges, err := c.listRangeOnRangeServerInStream(ctx, 0, 0)

	re.Empty(ranges)
	re.ErrorIs(err, context.DeadlineExceeded)
}

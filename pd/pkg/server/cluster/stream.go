package cluster

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/id"
	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

var (
	ErrStreamNotFound = errors.New("stream not found")
)

type StreamService interface {
	CreateStream(ctx context.Context, param *model.CreateStreamParam) (*rpcfb.StreamT, error)
	DeleteStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error)
	UpdateStream(ctx context.Context, param *model.UpdateStreamParam) (*rpcfb.StreamT, error)
	DescribeStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error)
}

// CreateStream creates a stream.
// It returns ErrNotLeader if the current PD node is not the leader.
func (c *RaftCluster) CreateStream(ctx context.Context, param *model.CreateStreamParam) (*rpcfb.StreamT, error) {
	logger := c.lg.With(traceutil.TraceLogField(ctx))

	sid, err := c.sAlloc.Alloc(ctx)
	if err != nil {
		logger.Error("failed to allocate a stream id", zap.Error(err))
		if errors.Is(err, id.ErrTxnFailed) {
			return nil, ErrNotLeader
		}
		return nil, err
	}

	stream := &rpcfb.StreamT{
		StreamId:          int64(sid),
		Replica:           param.Replica,
		AckCount:          param.AckCount,
		RetentionPeriodMs: param.RetentionPeriodMs,
		StartOffset:       0,
		Epoch:             0,
	}
	logger = logger.With(zap.Int64("stream-id", stream.StreamId))

	logger.Info("start to create stream")
	stream, err = c.storage.CreateStream(ctx, stream)
	logger.Info("finish creating stream", zap.Error(err))
	if err != nil {
		if errors.Is(err, kv.ErrTxnFailed) {
			return nil, ErrNotLeader
		}
		return nil, err
	}

	return stream, nil
}

// DeleteStream deletes the stream.
// It returns ErrNotLeader if the current PD node is not the leader.
// It returns ErrStreamNotFound if the stream is not found.
func (c *RaftCluster) DeleteStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error) {
	logger := c.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	logger.Info("start to delete stream")
	stream, err := c.storage.DeleteStream(ctx, streamID)
	logger.Info("finish deleting stream", zap.Error(err))
	if err != nil {
		if errors.Is(err, kv.ErrTxnFailed) {
			return nil, ErrNotLeader
		}
		return nil, err
	}
	if stream == nil {
		return nil, errors.Wrapf(ErrStreamNotFound, "stream id %d", streamID)
	}

	return stream, nil
}

// UpdateStream updates the stream.
// It returns ErrNotLeader if the current PD node is not the leader.
// It returns ErrStreamNotFound if the stream is not found.
func (c *RaftCluster) UpdateStream(ctx context.Context, param *model.UpdateStreamParam) (*rpcfb.StreamT, error) {
	logger := c.lg.With(zap.Int64("stream-id", param.StreamID), traceutil.TraceLogField(ctx))

	logger.Info("start to update stream")
	upStream, err := c.storage.UpdateStream(ctx, param)
	logger.Info("finish updating stream", zap.Error(err))
	if err != nil {
		if errors.Is(err, kv.ErrTxnFailed) {
			return nil, ErrNotLeader
		}
		if errors.Is(err, endpoint.ErrStreamNotFound) {
			return nil, errors.Wrapf(ErrStreamNotFound, "stream id %d", param.StreamID)
		}
		return nil, err
	}

	return upStream, nil
}

// DescribeStream describes the stream.
// It returns ErrNotLeader if the current PD node is not the leader.
// It returns ErrStreamNotFound if the stream is not found.
func (c *RaftCluster) DescribeStream(ctx context.Context, streamID int64) (*rpcfb.StreamT, error) {
	logger := c.lg.With(zap.Int64("stream-id", streamID), traceutil.TraceLogField(ctx))

	logger.Debug("start to describe stream")
	stream, err := c.storage.GetStream(ctx, streamID)
	logger.Debug("finish describing stream", zap.Error(err))
	if err != nil {
		if errors.Is(err, kv.ErrTxnFailed) {
			return nil, ErrNotLeader
		}
		return nil, err
	}
	if stream == nil {
		return nil, errors.Wrapf(ErrStreamNotFound, "stream id %d", streamID)
	}

	return stream, nil
}

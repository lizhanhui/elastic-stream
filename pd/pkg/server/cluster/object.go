package cluster

import (
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/id"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

type ObjectService interface {
	// CommitObject commits an object and returns the committed object.
	// It returns ErrNotLeader if the current PD node is not the leader.
	// It returns ErrRangeNotFound if the range does not exist.
	CommitObject(ctx context.Context, object *rpcfb.ObjT) (endpoint.Object, error)
	// ListObjectInRange returns all objects in the range.
	// It returns ErrNotLeader if the current PD node is not the leader.
	ListObjectInRange(ctx context.Context, rangeID endpoint.RangeID) ([]endpoint.Object, error)
}

func (c *RaftCluster) CommitObject(ctx context.Context, obj *rpcfb.ObjT) (endpoint.Object, error) {
	logger := c.lg.With(zap.Int64("stream-id", obj.StreamId), zap.Int32("range-index", obj.RangeIndex), traceutil.TraceLogField(ctx))

	r, err := c.storage.GetRange(ctx, &endpoint.RangeID{StreamID: obj.StreamId, Index: obj.RangeIndex})
	if err != nil {
		if errors.Is(err, kv.ErrTxnFailed) {
			return endpoint.Object{}, ErrNotLeader
		}
		return endpoint.Object{}, err
	}
	if r == nil {
		return endpoint.Object{}, errors.Wrapf(ErrRangeNotFound, "stream-id %d, range-index %d", obj.StreamId, obj.RangeIndex)
	}

	oid, err := c.oAlloc.Alloc(ctx)
	if err != nil {
		logger.Error("failed to allocate an object id", zap.Error(err))
		if errors.Is(err, id.ErrTxnFailed) {
			return endpoint.Object{}, ErrNotLeader
		}
		return endpoint.Object{}, err
	}
	object := endpoint.Object{
		ObjT:     obj,
		ObjectID: int64(oid),
	}
	logger = logger.With(zap.Int64("object-id", int64(oid)))

	logger.Info("start to commit object")
	err = c.storage.CreateObject(ctx, object)
	logger.Info("finish committing object", zap.Error(err))
	if err != nil {
		if errors.Is(err, kv.ErrTxnFailed) {
			return endpoint.Object{}, ErrNotLeader
		}
		return endpoint.Object{}, err
	}

	return object, nil
}

func (c *RaftCluster) ListObjectInRange(ctx context.Context, rangeID endpoint.RangeID) ([]endpoint.Object, error) {
	logger := c.lg.With(zap.Int64("stream-id", rangeID.StreamID), zap.Int32("range-index", rangeID.Index), traceutil.TraceLogField(ctx))

	logger.Debug("start to list objects in range")
	objects, err := c.storage.GetObjectsByRange(ctx, rangeID)
	logger.Debug("finish listing objects in range", zap.Error(err))
	if errors.Is(err, kv.ErrTxnFailed) {
		return nil, ErrNotLeader
	}

	return objects, err
}

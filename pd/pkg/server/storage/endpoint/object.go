package endpoint

import (
	"context"
	"fmt"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/model"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/fbutil"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

const (
	_objectIDFormat = _int64Format
	_objectIDLen    = _int64Len

	// objects in range
	_objectPath                = "objects"
	_objectPrefix              = _objectPath + kv.KeySeparator
	_objectInRangePrefixFormat = _objectPath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeIDFormat + kv.KeySeparator
	_objectFormat              = _objectPath + kv.KeySeparator + _streamIDFormat + kv.KeySeparator + _rangeIDFormat + kv.KeySeparator + _objectIDFormat
	_objectKeyLen              = len(_objectPath) + len(kv.KeySeparator) + _streamIDLen + len(kv.KeySeparator) + _rangeIDLen + len(kv.KeySeparator) + _objectIDLen

	_objectByRangeLimit = 1e4
)

type Object struct {
	*rpcfb.ObjT
	ObjectID int64
}

type ObjectEndpoint interface {
	CreateObject(ctx context.Context, object Object) error
	// UpdateObject updates the object and returns the previous object.
	UpdateObject(ctx context.Context, object Object) (Object, error)
	// GetObjectsByRange returns all objects in the range.
	GetObjectsByRange(ctx context.Context, rangeID RangeID) ([]Object, error)
	// ForEachObjectInRange calls f for each object in the range.
	// If f returns an error, stop traversing and return the error.
	ForEachObjectInRange(ctx context.Context, rangeID RangeID, f func(object Object) error) error
}

func (e *Endpoint) CreateObject(ctx context.Context, object Object) error {
	logger := e.lg.With(zap.Int64("stream-id", object.StreamId), zap.Int32("range-index", object.RangeIndex), zap.Int64("object-id", object.ObjectID), traceutil.TraceLogField(ctx))

	key := objectPath(object.StreamId, object.RangeIndex, object.ObjectID)
	value := fbutil.Marshal(object)
	prevValue, err := e.KV.Put(ctx, key, value, true)
	mcache.Free(value)

	if err != nil {
		logger.Error("failed to create object", zap.Error(err))
		return errors.Wrap(err, "create object")
	}
	if prevValue != nil {
		logger.Warn("object already exist, will override it")
	}

	return nil
}

func (e *Endpoint) UpdateObject(ctx context.Context, object Object) (Object, error) {
	logger := e.lg.With(zap.Int64("stream-id", object.StreamId), zap.Int32("range-index", object.RangeIndex), zap.Int64("object-id", object.ObjectID), traceutil.TraceLogField(ctx))

	if object.ObjectID < model.MinObjectID {
		logger.Error("invalid object ID")
		return Object{}, errors.Errorf("invalid object ID %d < %d", object.ObjectID, model.MinObjectID)
	}

	key := objectPath(object.StreamId, object.RangeIndex, object.ObjectID)
	value := fbutil.Marshal(object)

	prevValue, err := e.KV.Put(ctx, key, value, false)
	mcache.Free(value)
	if err != nil {
		logger.Error("failed to update object", zap.Error(err))
		return Object{}, errors.Wrap(err, "update object")
	}
	if prevValue == nil {
		logger.Warn("object not found when updating")
		return Object{}, nil
	}

	prevObject := Object{}
	prevObject.ObjT = rpcfb.GetRootAsObj(prevValue, 0).UnPack()
	prevObject.ObjectID = object.ObjectID
	return prevObject, nil
}
func (e *Endpoint) GetObjectsByRange(ctx context.Context, rangeID RangeID) ([]Object, error) {
	logger := e.lg.With(zap.Int64("stream-id", rangeID.StreamID), zap.Int32("range-index", rangeID.Index), traceutil.TraceLogField(ctx))

	objects := make([]Object, 0)
	err := e.ForEachObjectInRange(ctx, rangeID, func(object Object) error {
		objects = append(objects, object)
		return nil
	})
	if err != nil {
		logger.Error("failed to get objects by range", zap.Error(err))
		return nil, errors.Wrap(err, "get objects by range")
	}

	return objects, nil
}

func (e *Endpoint) ForEachObjectInRange(ctx context.Context, rangeID RangeID, f func(object Object) error) error {
	var startID = model.MinObjectID
	for startID >= model.MinStreamID {
		nextID, err := e.forEachObjectInRangeLimited(ctx, rangeID, f, startID, _objectByRangeLimit)
		if err != nil {
			return err
		}
		startID = nextID
	}
	return nil
}

func (e *Endpoint) forEachObjectInRangeLimited(ctx context.Context, rangeID RangeID, f func(object Object) error, startID int64, limit int64) (nextID int64, err error) {
	logger := e.lg.With(zap.Int64("stream-id", rangeID.StreamID), zap.Int32("range-index", rangeID.Index), traceutil.TraceLogField(ctx))

	startKey := objectPath(rangeID.StreamID, rangeID.Index, startID)
	kvs, _, more, err := e.KV.GetByRange(ctx, kv.Range{StartKey: startKey, EndKey: e.endObjectPathInRange(rangeID)}, 0, limit, false)
	if err != nil {
		logger.Error("failed to get objects", zap.Int32("start-id", int32(startID)), zap.Int64("limit", limit), zap.Error(err))
		return model.MinObjectID - 1, errors.Wrap(err, "get objects")
	}

	for _, objectKV := range kvs {
		o := rpcfb.GetRootAsObj(objectKV.Value, 0).UnPack()
		oid, err := objectIDFromPath(objectKV.Key)
		if err != nil {
			logger.Error("failed to parse object ID", zap.Error(err))
			return model.MinObjectID - 1, errors.Wrap(err, "parse object ID")
		}
		nextID = oid + 1

		err = f(Object{ObjT: o, ObjectID: oid})
		if err != nil {
			return model.MinObjectID - 1, err
		}
	}

	if !more {
		// no more objects
		nextID = model.MinObjectID - 1
	}
	return
}

func (e *Endpoint) endObjectPathInRange(rangeID RangeID) []byte {
	return e.KV.GetPrefixRangeEnd([]byte(fmt.Sprintf(_objectInRangePrefixFormat, rangeID.StreamID, rangeID.Index)))
}

func objectPath(streamID int64, rangeIndex int32, objectID int64) []byte {
	res := make([]byte, 0, _objectKeyLen)
	res = fmt.Appendf(res, _objectFormat, streamID, rangeIndex, objectID)
	return res
}

func objectIDFromPath(path []byte) (objectID int64, err error) {
	var rangeID RangeID
	_, err = fmt.Sscanf(string(path), _objectFormat, &rangeID.StreamID, &rangeID.Index, &objectID)
	if err != nil {
		err = errors.Wrapf(err, "invalid object path %s", string(path))
	}
	return
}

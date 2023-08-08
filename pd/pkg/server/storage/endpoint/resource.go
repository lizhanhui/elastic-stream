package endpoint

import (
	"bytes"
	"context"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
)

var (
	ErrUnsupportedResourceType = errors.New("unsupported resource type")
)

type ResourceEndpoint interface {
	// ListResource lists resources of a given type and revision.
	ListResource(ctx context.Context, rv int64, token ContinueToken, limit int32) (resources []*rpcfb.ResourceT, newRV int64, newToken ContinueToken, err error)
	// WatchResource watches resources of given types and revision.
	// If the error is nil, the returned events are not empty.
	WatchResource(ctx context.Context, rv int64, types []rpcfb.ResourceType) (events []*rpcfb.ResourceEventT, newRV int64, err error)
}

func (e *Endpoint) ListResource(ctx context.Context, rv int64, token ContinueToken, limit int32) ([]*rpcfb.ResourceT, int64, ContinueToken, error) {
	logger := e.lg.With(token.ZapFields()...)
	logger = logger.With(zap.Int64("resource-version", rv), zap.Int32("limit", limit), traceutil.TraceLogField(ctx))

	keyPrefix, err := keyPrefix(token.ResourceType)
	if err != nil {
		logger.Error("failed to get key prefix", zap.Error(err))
		return nil, 0, ContinueToken{}, errors.Wrap(err, "get key prefix")
	}
	keyRange := kv.Range{
		StartKey: append(keyPrefix, token.StartKey...),
		EndKey:   e.KV.GetPrefixRangeEnd(keyPrefix),
	}

	kvs, newRV, more, err := e.KV.GetByRange(ctx, keyRange, rv, int64(limit), false)
	if err != nil {
		logger.Error("failed to list resources", zap.Error(err))
		return nil, 0, ContinueToken{}, errors.Wrap(err, "list resources")
	}

	token.More = more
	if !more {
		token.StartKey = nil
	} else {
		token.StartKey = kvs[len(kvs)-1].Key[len(keyPrefix):] // remove the prefix
		token.StartKey = append(token.StartKey, 0)            // start immediately after the last key
	}

	resources := make([]*rpcfb.ResourceT, len(kvs))
	for i, keyValue := range kvs {
		resources[i] = newResource(token.ResourceType, keyValue.Value)
	}

	return resources, newRV, token, nil
}

func (e *Endpoint) WatchResource(ctx context.Context, rv int64, types []rpcfb.ResourceType) ([]*rpcfb.ResourceEventT, int64, error) {
	logger := e.lg.With(zap.Int64("resource-version", rv), zap.Stringers("resource-types", types), traceutil.TraceLogField(ctx))

	prefixes := make([][]byte, len(types))
	for i, resourceType := range types {
		prefix, err := keyPrefix(resourceType)
		if err != nil {
			logger.Error("failed to get key prefix", zap.Error(err))
			return nil, 0, errors.Wrap(err, "get key prefix")
		}
		prefixes[i] = prefix
	}
	filter := func(e kv.Event) bool {
		for _, prefix := range prefixes {
			if bytes.HasPrefix(e.Key, prefix) {
				return true
			}
		}
		return false
	}

	watcher := e.KV.Watch(ctx, nil, rv, filter)
	defer watcher.Close()

	ch := watcher.EventChan()
	select {
	case events := <-ch:
		if err := events.Error; err != nil {
			logger.Error("failed to watch resources", zap.Error(err))
			return nil, 0, errors.Wrap(err, "watch resources")
		}
		resourceEvents := make([]*rpcfb.ResourceEventT, len(events.Events))
		for i, event := range events.Events {
			switch event.Type {
			case kv.Added:
				resourceEvents[i] = &rpcfb.ResourceEventT{Type: rpcfb.EventTypeADDED}
			case kv.Modified:
				resourceEvents[i] = &rpcfb.ResourceEventT{Type: rpcfb.EventTypeMODIFIED}
			case kv.Deleted:
				resourceEvents[i] = &rpcfb.ResourceEventT{Type: rpcfb.EventTypeDELETED}
			}
			resourceType, err := resourceType(event.Key)
			if err != nil {
				// should never happen as we have filtered the events
				logger.Error("failed to get resource type", zap.Binary("event-key", event.Key), zap.Binary("event-value", event.Value), zap.Error(err))
				continue
			}
			resourceEvents[i].Resource = newResource(resourceType, event.Value)
		}
		return resourceEvents, events.Revision, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func keyPrefix(resourceType rpcfb.ResourceType) ([]byte, error) {
	switch resourceType {
	case rpcfb.ResourceTypeRANGE_SERVER:
		return []byte(_rangeServerPrefix), nil
	case rpcfb.ResourceTypeSTREAM:
		return []byte(_streamPrefix), nil
	case rpcfb.ResourceTypeRANGE:
		return []byte(_rangePrefix), nil
	case rpcfb.ResourceTypeOBJECT:
		return []byte(_objectPrefix), nil
	default:
		return nil, errors.Wrapf(ErrUnsupportedResourceType, "unsupported resource type %v", resourceType)
	}
}

func resourceType(key []byte) (rpcfb.ResourceType, error) {
	switch {
	case bytes.HasPrefix(key, []byte(_rangeServerPrefix)):
		return rpcfb.ResourceTypeRANGE_SERVER, nil
	case bytes.HasPrefix(key, []byte(_streamPrefix)):
		return rpcfb.ResourceTypeSTREAM, nil
	case bytes.HasPrefix(key, []byte(_rangePrefix)):
		return rpcfb.ResourceTypeRANGE, nil
	case bytes.HasPrefix(key, []byte(_objectPrefix)):
		return rpcfb.ResourceTypeOBJECT, nil
	default:
		return rpcfb.ResourceTypeUNKNOWN, errors.Errorf("unknown resource type for key %s", key)
	}
}

func newResource(typ rpcfb.ResourceType, data []byte) *rpcfb.ResourceT {
	resource := &rpcfb.ResourceT{Type: typ}
	switch typ {
	case rpcfb.ResourceTypeRANGE_SERVER:
		resource.RangeServer = rpcfb.GetRootAsRangeServer(data, 0).UnPack()
	case rpcfb.ResourceTypeSTREAM:
		resource.Stream = rpcfb.GetRootAsStream(data, 0).UnPack()
	case rpcfb.ResourceTypeRANGE:
		resource.Range = rpcfb.GetRootAsRange(data, 0).UnPack()
	case rpcfb.ResourceTypeOBJECT:
		resource.Object = rpcfb.GetRootAsObj(data, 0).UnPack()
	}
	return resource
}

// ContinueToken is used to continue a list operation from a previous result.
type ContinueToken struct {
	ResourceType rpcfb.ResourceType `json:"rt"`
	// StartKey is the key to start listing from. If empty, start from the beginning.
	StartKey []byte `json:"start,omitempty"`
	More     bool   `json:"more,omitempty"`
}

func (t ContinueToken) ZapFields() []zap.Field {
	return []zap.Field{
		zap.Stringer("token-resource-type", t.ResourceType),
		zap.ByteString("token-start-key", t.StartKey),
	}
}

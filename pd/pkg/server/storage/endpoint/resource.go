package endpoint

import (
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
		EndKey:   e.GetPrefixRangeEnd(keyPrefix),
	}

	kvs, newRV, more, err := e.GetByRange(ctx, keyRange, rv, int64(limit), false)
	if err != nil {
		logger.Error("failed to list resources", zap.Error(err))
		return nil, 0, ContinueToken{}, errors.Wrap(err, "list resources")
	}

	token.StartKey = kvs[len(kvs)-1].Key[len(keyPrefix):] // remove the prefix
	token.StartKey = append(token.StartKey, 0)            // start immediately after the last key
	token.More = more

	resources := make([]*rpcfb.ResourceT, len(kvs))
	for i, keyValue := range kvs {
		resources[i] = &rpcfb.ResourceT{
			Type: token.ResourceType,
			Data: keyValue.Value,
		}
	}

	return resources, newRV, token, nil
}

func keyPrefix(resourceType rpcfb.ResourceType) ([]byte, error) {
	switch resourceType {
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

// ContinueToken is used to continue a list operation from a previous result.
type ContinueToken struct {
	ResourceType rpcfb.ResourceType `json:"rt"`
	// StartKey is the key to start listing from. If empty, start from the beginning.
	StartKey []byte `json:"start"`
	More     bool   `json:"more"`
}

func (t ContinueToken) ZapFields() []zap.Field {
	return []zap.Field{
		zap.Stringer("resource-type", t.ResourceType),
		zap.ByteString("start-key", t.StartKey),
	}
}

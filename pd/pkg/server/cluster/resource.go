package cluster

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/server/storage/endpoint"
	"github.com/AutoMQ/pd/pkg/server/storage/kv"
	"github.com/AutoMQ/pd/pkg/util/traceutil"
	"github.com/AutoMQ/pd/pkg/util/typeutil"
)

var (
	// ErrCompacted is the error when the requested resource version has been compacted.
	ErrCompacted = errors.New("requested resource version has been compacted")
	// ErrInvalidResourceType is the error when the requested resource type is invalid (unknown or unsupported).
	ErrInvalidResourceType = errors.New("invalid resource type")
	// ErrInvalidContinuation is the error when the continuation string is invalid.
	ErrInvalidContinuation = errors.New("invalid continue string")
)

type ResourceService interface {
	// ListResource lists resources of given types.
	// It returns a continuation token if there are more resources to return. The token can be used to continue the listing.
	// In a pagination request, the first request lists resources with the latest resource version, and the following requests use the same resource version.
	// If limit is 0, it returns all resources without pagination.
	// The returned resources are in the same order as the given types.
	// If the returned token is nil, it means the listing is finished.
	// It returns ErrNotLeader if the current PD node is not the leader.
	// It returns ErrCompacted if the requested resource version has been compacted.
	// It returns ErrInvalidResourceType if the requested resource type is invalid.
	// It returns ErrInvalidContinuation if the continuation string is invalid.
	ListResource(ctx context.Context, types []rpcfb.ResourceType, limit int32, continueStr []byte) (resources []*rpcfb.ResourceT, resourceVersion int64, newContinueStr []byte, err error)
	// WatchResource watches resources of given types from the given resource version.
	// If rv is 0, it watches resources with the latest resource version.
	// If rv is greater than 0, the returned events happen AFTER the given resource version.
	// It returns ErrCompacted if the requested resource version has been compacted.
	// It returns ErrInvalidResourceType if the requested resource type is invalid.
	WatchResource(ctx context.Context, rv int64, types []rpcfb.ResourceType) ([]*rpcfb.ResourceEventT, int64, error)
}

func (c *RaftCluster) ListResource(ctx context.Context, types []rpcfb.ResourceType, limit int32, continueStr []byte) ([]*rpcfb.ResourceT, int64, []byte, error) {
	logger := c.lg.With(zap.Stringers("resource-types", types), zap.Int32("limit", limit), zap.ByteString("continue", continueStr), traceutil.TraceLogField(ctx))

	if ok, dup := typeutil.IsUnique(types); !ok {
		return nil, 0, nil, errors.Errorf("duplicate resource type %s", dup)
	}
	err := checkResourceType(types)
	if err != nil {
		logger.Error("invalid resource type", zap.Error(err))
		return nil, 0, nil, err
	}

	var continuation Continuation
	if continueStr == nil {
		tokens := make([]endpoint.ContinueToken, len(types))
		for i, typ := range types {
			tokens[i] = endpoint.ContinueToken{
				ResourceType: typ,
				More:         true,
			}
		}
		continuation.Tokens = tokens
	} else {
		err := json.Unmarshal(continueStr, &continuation)
		if err != nil {
			logger.Error("failed to unmarshal continuation string", zap.Error(err))
			return nil, 0, nil, errors.Wrap(ErrInvalidContinuation, "unmarshal continuation string")
		}
		err = continuation.check(types)
		if err != nil {
			logger.Error("invalid continuation string", zap.Error(err))
			return nil, 0, nil, errors.Wrap(err, "check continuation string")
		}
	}

	var resources []*rpcfb.ResourceT
	for i := range continuation.Tokens {
		token := continuation.Tokens[i]
		if !token.More {
			continue
		}

		logger := logger.With(token.ZapFields()...)
		logger.Debug("start to list resources")
		res, newRV, newToken, err := c.storage.ListResource(ctx, continuation.ResourceVersion, token, limit)
		logger.Debug("finish listing resources", zap.Int("count", len(res)), zap.Int64("new-rv", newRV), zap.Reflect("new-token", newToken), zap.Error(err))
		if err != nil {
			switch {
			case errors.Is(err, kv.ErrTxnFailed):
				err = ErrNotLeader
			case errors.Is(err, kv.ErrCompacted):
				err = ErrCompacted
			default:
				err = errors.Wrap(err, "list resources")
			}
			return nil, 0, nil, err
		}
		resources = append(resources, res...)

		continuation.ResourceVersion = newRV
		continuation.Tokens[i] = newToken
		limit -= int32(len(res))
		if limit <= 0 {
			break
		}
	}

	var newContinueStr []byte
	// If there are more resources, return a continuation token.
	if continuation.Tokens[len(continuation.Tokens)-1].More {
		var err error
		newContinueStr, err = json.Marshal(continuation)
		if err != nil {
			logger.Error("failed to marshal continuation string", zap.Error(err))
			return nil, 0, nil, errors.Wrap(err, "marshal continuation string")
		}
	}
	for _, resource := range resources {
		c.fillResourceInfo(resource)
	}
	return resources, continuation.ResourceVersion, newContinueStr, nil
}

func (c *RaftCluster) WatchResource(ctx context.Context, rv int64, types []rpcfb.ResourceType) ([]*rpcfb.ResourceEventT, int64, error) {
	logger := c.lg.With(zap.Int64("resource-version", rv), zap.Stringers("resource-types", types), traceutil.TraceLogField(ctx))
	types = typeutil.SortAndUnique[rpcfb.ResourceType](types, func(i, j rpcfb.ResourceType) bool { return i < j })

	err := checkResourceType(types)
	if err != nil {
		logger.Error("invalid resource type", zap.Error(err))
		return nil, 0, err
	}

	logger.Debug("start to watch resources")
	rv++ // watch from the next resource version
	evs, newRV, err := c.storage.WatchResource(ctx, rv, types)
	logger.Debug("finish watching resources", zap.Int("event-count", len(evs)), zap.Int64("new-rv", newRV), zap.Error(err))
	if err != nil {
		if errors.Is(err, kv.ErrCompacted) {
			err = ErrCompacted
		}
		return nil, 0, err
	}

	for _, ev := range evs {
		c.fillResourceInfo(ev.Resource)
	}
	return evs, newRV, nil
}

func (c *RaftCluster) fillResourceInfo(resource *rpcfb.ResourceT) {
	if resource.Type == rpcfb.ResourceTypeRANGE {
		c.fillRangeServersInfo(resource.Range.Servers)
	}
}

func checkResourceType(types []rpcfb.ResourceType) error {
	for _, typ := range types {
		if typ == rpcfb.ResourceTypeUNKNOWN {
			return errors.Wrapf(ErrInvalidResourceType, "invalid type %s", typ)
		}
	}
	return nil
}

type Continuation struct {
	ResourceVersion int64                    `json:"rv"`
	Tokens          []endpoint.ContinueToken `json:"tokens"`
}

func (c Continuation) check(types []rpcfb.ResourceType) error {
	if c.ResourceVersion <= 0 {
		return errors.Wrapf(ErrInvalidContinuation, "invalid resource version %d", c.ResourceVersion)
	}

	for i, token := range c.Tokens {
		if token.ResourceType == rpcfb.ResourceTypeUNKNOWN {
			return errors.Wrapf(ErrInvalidResourceType, "invalid type %s in token %d", token.ResourceType, i)
		}
	}

	// Check whether the types are the same.
	// The types are sorted and unique, so we can compare them directly.
	if len(types) != len(c.Tokens) {
		return errors.Wrapf(ErrInvalidContinuation, "type count %d != token count %d", len(types), len(c.Tokens))
	}
	for i, token := range c.Tokens {
		if types[i] != token.ResourceType {
			return errors.Wrapf(ErrInvalidContinuation, "type %s != token %d type %s", types[i], i, token.ResourceType)
		}
	}

	return nil
}

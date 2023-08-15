package model

import (
	"github.com/pkg/errors"
)

// KV errors
var (
	// ErrKVTxnFailed is returned when etcd transaction failed.
	ErrKVTxnFailed = errors.New("etcd transaction failed")
	// ErrKVTooManyTxnOps is returned when the number of operations in a transaction exceeds the limit.
	ErrKVTooManyTxnOps = errors.New("too many txn operations")
	// ErrKVCompacted is returned when the requested revision has been compacted.
	ErrKVCompacted = errors.New("requested revision has been compacted")
	// ErrKVDataModified is returned when the data has been modified when doing transaction.
	ErrKVDataModified = errors.New("data has been modified")
)

// PD errors
var (
	// ErrPDNotLeader is returned when the current node is not the leader.
	ErrPDNotLeader = errors.New("PD not leader")
)

// Range server errors
var (
	// ErrNotEnoughRangeServers is returned when there are not enough range servers to allocate a range.
	ErrNotEnoughRangeServers = errors.New("not enough range servers")
)

// Stream errors
var (
	// ErrStreamNotFound is returned when the stream is not found.
	ErrStreamNotFound = errors.New("stream not found")
	// ErrInvalidStreamEpoch is returned when the stream epoch mismatches.
	ErrInvalidStreamEpoch = errors.New("invalid stream epoch")
	// ErrInvalidStreamOffset is returned when the stream's start offset is invalid.
	ErrInvalidStreamOffset = errors.New("invalid stream offset")
)

// Range errors
var (
	// ErrRangeAlreadyExist is returned when the specified range is already created.
	ErrRangeAlreadyExist = errors.New("range already created")
	// ErrCreateRangeTwice is returned when the range is created twice with the same parameters.
	ErrCreateRangeTwice = errors.New("create range twice")
	// ErrInvalidRangeIndex is returned when the range index is invalid.
	ErrInvalidRangeIndex = errors.New("invalid range index")
	// ErrCreateRangeBeforeSeal is returned when the last range is not sealed.
	ErrCreateRangeBeforeSeal = errors.New("create range before sealing the previous one")
	// ErrInvalidRangeStart is returned when the start offset is invalid.
	ErrInvalidRangeStart = errors.New("invalid range start offset")

	// ErrRangeNotFound is returned when the specified range is not found.
	ErrRangeNotFound = errors.New("range not found")
	// ErrRangeAlreadySealed is returned when the specified range is already sealed.
	ErrRangeAlreadySealed = errors.New("range already sealed")
	// ErrSealRangeTwice is returned when the range is sealed twice with the same end offset.
	ErrSealRangeTwice = errors.New("seal range twice")
	// ErrInvalidRangeEnd is returned when the end offset is invalid.
	ErrInvalidRangeEnd = errors.New("invalid range end offset")
)

// Resource errors
var (
	// ErrResourceVersionCompacted is returned when the requested resource version has been compacted.
	ErrResourceVersionCompacted = errors.New("requested resource version has been compacted")
	// ErrInvalidResourceType is returned when the requested resource type is invalid (unknown or unsupported).
	ErrInvalidResourceType = errors.New("invalid resource type")
	// ErrInvalidResourceContinuation is returned when the continuation string is invalid.
	ErrInvalidResourceContinuation = errors.New("invalid resource continue string")
)

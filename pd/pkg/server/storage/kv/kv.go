// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"context"
)

const (
	// KeySeparator is the separator in keys
	KeySeparator = "/"
)

// Range represents a range of keys.
type Range struct {
	StartKey []byte
	EndKey   []byte
}

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   []byte
	Value []byte
}

type EventType string

const (
	Added    EventType = "ADDED"
	Modified EventType = "MODIFIED"
	Deleted  EventType = "DELETED"
)

// Event represents a single event to a watched resource.
type Event struct {
	Type EventType

	Key []byte

	// Value is
	// - the new value of the key if Type is Added or Modified.
	// - the previous value of the key if Type is Deleted.
	Value []byte
}

// Events represents a list of events.
// All events in Events happen at the same revision.
type Events struct {
	Events []Event

	// Revision is the revision of the KV when the events happen.
	Revision int64

	// Error is the error if an error occurs, otherwise nil.
	// If it is not nil, the Events will be nil.
	// It will be set to ErrCompacted if the revision has been compacted.
	Error error
}

type Filter func(event Event) bool

type Watcher interface {
	// Close closes the watcher and releases the related resources.
	Close()

	// EventChan returns a channel that receives events that happen or happened
	// If an error occurs or Close() is called, the channel will be closed.
	// Events.Revision is unique and monotonically increasing for all events received from the channel.
	EventChan() <-chan Events
}

// BasicKV represents the basic interface for a key-value store.
type BasicKV interface {
	// Get retrieves the value associated with the given key.
	// If the key does not exist, Get returns nil and no error.
	// If the key is empty, Get returns nil and no error.
	Get(ctx context.Context, key []byte) ([]byte, error)

	// Put sets the value for the given key.
	// IF the key is empty, Put does nothing and returns no error.
	// If the key already exists, Put overwrites the existing value.
	// If prevKV is true, the old value (if any) will be returned.
	Put(ctx context.Context, key, value []byte, prevKV bool) ([]byte, error)

	// Delete removes the key-value pair associated with the given key.
	// If the key is empty, Delete does nothing and returns no error.
	// If the key does not exist, Delete does nothing and returns no error.
	// If prevKV is true, the old value (if any) will be returned.
	Delete(ctx context.Context, key []byte, prevKV bool) ([]byte, error)
}

// KV represents a key-value store.
type KV interface {
	BasicKV

	// BatchGet retrieves the values associated with the given keys.
	// If the key does not exist, BatchGet returns no error.
	// Any empty key will be ignored.
	// If inTxn is true, BatchGet will try to get all keys in a single transaction.
	BatchGet(ctx context.Context, keys [][]byte, inTxn bool) ([]KeyValue, error)

	// GetByRange retrieves a list of key-value pairs whose keys fall within the given range (r)
	// and limits the number of results returned to "limit".
	// If the Range.StartKey is empty, GetByRange returns nil and no error.
	// If rev is less than or equal to 0, GetByRange gets the key-value pairs at the latest revision, and returns the revision.
	// If rev is greater than 0, GetByRange gets the key-value pairs at the given revision, and returns the same revision.
	// If limit is 0, GetByRange will return all key-value pairs whose keys fall within the given range (r).
	// If limit is greater than 0, GetByRange will return at most "limit" key-value pairs whose keys fall within the given range (r),
	// and a boolean value indicating whether there are more keys in the range.
	// If desc is true, GetByRange returns the key-value pairs in descending order.
	GetByRange(ctx context.Context, r Range, rev int64, limit int64, desc bool) (kvs []KeyValue, revision int64, more bool, err error)

	// Watch watches on a prefix.
	// If the prefix is empty, Watch will watch on all keys.
	// If rev is less than or equal to 0, Watch watches on the latest revision.
	// If rev is greater than 0, Watch watches on the given revision, which means the watcher will not receive events
	// with a revision less than the given revision.
	// If filter is not nil, only the key-value pairs that pass the filter will be returned.
	Watch(ctx context.Context, prefix []byte, rev int64, filter Filter) Watcher

	// BatchPut sets the value for the given keys.
	// If the key already exists, BatchPut overwrites the existing value.
	// Any empty key will be ignored.
	// If prevKV is true, the old key-value pairs (if any) will be returned.
	// If inTxn is true, BatchPut will try to put all keys in a single transaction.
	BatchPut(ctx context.Context, kvs []KeyValue, prevKV bool, inTxn bool) ([]KeyValue, error)

	// BatchDelete removes the key-value pairs associated with the given keys.
	// If the key does not exist, BatchDelete returns no error.
	// Any empty key will be ignored.
	// If prevKV is true, the old key-value pairs (if any) will be returned.
	// If inTxn is true, BatchDelete will try to delete all keys in a single transaction.
	BatchDelete(ctx context.Context, keys [][]byte, prevKV bool, inTxn bool) ([]KeyValue, error)

	// DeleteByPrefixes removes the key-value pairs associated with the given prefixes in a single transaction.
	// Any empty prefix will be ignored.
	// It returns the number of key-value pairs that are deleted.
	DeleteByPrefixes(ctx context.Context, prefixes [][]byte) (int64, error)

	// ExecInTxn executes the given function in a single transaction.
	// It prioritizes returning the error returned by the function, and then the error occurred within the transaction.
	// If and only if the function returns no error, the transaction will be committed.
	//
	// Note:
	// * Write operations on KV will not take effect immediately,
	//   i.e., you cannot read modifications made in the same transaction.
	// * There is a limitation on the number of write operations, with a default limit of 128.
	//   Do not attempt to perform too many operations in a single transaction.
	// * The kv passed to the function is not thread-safe. DO NOT use it in multiple goroutines.
	// * The flag `prevKV` in Put and Delete will not take effect; Put and Delete will always return nils.
	ExecInTxn(ctx context.Context, f func(kv BasicKV) error) error

	// GetPrefixRangeEnd returns the end key for a prefix range query.
	GetPrefixRangeEnd(prefix []byte) []byte
}

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

// KV represents the basic interface for a key-value store.
type KV interface {
	// Get retrieves the value associated with the given key.
	// If the key does not exist, Get returns nil and no error.
	// If the key is empty, Get returns nil and no error.
	Get(key []byte) ([]byte, error)

	// GetByRange retrieves a list of key-value pairs whose keys fall within the given range (r)
	// and limits the number of results returned to "limit".
	// If the Range.StartKey is empty, GetByRange returns nil and no error.
	GetByRange(r Range, limit int64) (kvs []KeyValue, err error)

	// Put sets the value for the given key.
	// If the key already exists, Put overwrites the existing value and returns the old value.
	// If the key is empty, Put does nothing, returns nil and no error.
	Put(key, value []byte) ([]byte, error)

	// Delete removes the key-value pair associated with the given key.
	// If the key does not exist, Delete returns nil and no error.
	// If the key is empty, Delete does nothing and returns no error.
	Delete(key []byte) ([]byte, error)

	// GetPrefixRangeEnd returns the end key for a prefix range query.
	GetPrefixRangeEnd(prefix []byte) []byte
}

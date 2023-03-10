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
	"github.com/pkg/errors"
)

var (
	// ErrKeyNotExist is the error when the key does not exist.
	ErrKeyNotExist = errors.New("key not exist")
)

// Range represents a range of keys.
type Range struct {
	StartKey string
	EndKey   string
}

// KeyValue represents a key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// KV represents the basic interface for a key-value store.
type KV interface {
	// Get retrieves the value associated with the given key. If the key does not exist, it returns an error.
	Get(key string) (string, error)

	// GetByRange retrieves a list of key-value pairs whose keys fall within the given range (r) and limits the
	// number of results returned to "limit". It returns an error if the operation fails.
	GetByRange(r Range, limit int) (kvs []KeyValue, err error)

	// Put sets the value for the given key. If the key already exists, it overwrites the existing value.
	Put(key, value string) error

	// Delete removes the value associated with the given key. If the key does not exist, it returns ErrKeyNotExist.
	Delete(key string) error

	// GetPrefixRangeEnd returns the end key for a prefix range query.
	GetPrefixRangeEnd(prefix string) string
}

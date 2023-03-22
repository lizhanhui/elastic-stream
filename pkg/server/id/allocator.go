// Copyright 2016 TiKV Project Authors.
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

package id

const (
	_defaultStep = 1000
)

// Allocator is the allocator to generate unique ID.
type Allocator interface {
	// Alloc allocates a new ID.
	Alloc() (uint64, error)

	// AllocN allocates ordered N IDs.
	AllocN(n uint) ([]uint64, error)

	// Rebase re-bases the allocator to a base value.
	// If force is true, it will rebase the allocator even if the base value is smaller than the current one.
	// If force is false, it does nothing and returns nil when the base value is smaller than the current one.
	Rebase(base uint64, force bool) error
}

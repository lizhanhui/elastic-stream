package typeutil

import (
	"sort"

	mapset "github.com/deckarep/golang-set/v2"
)

// FilterZero returns a new slice with all zero values removed.
func FilterZero[T comparable](l []T) (res []T) {
	var n T
	res = make([]T, 0, len(l))
	for _, t := range l {
		if t == n {
			continue
		}
		res = append(res, t)
	}
	return
}

// SortAndUnique sorts the slice and removes all duplicate elements.
// NOTE: the input slice will be modified and should not be used afterwards.
func SortAndUnique[T comparable](l []T, less func(i, j T) bool) []T {
	if len(l) < 2 {
		return l
	}
	sort.Slice(l, func(i, j int) bool { return less(l[i], l[j]) })
	for i := 1; i < len(l); i++ {
		if l[i-1] == l[i] {
			l = append(l[:i], l[i+1:]...)
			i--
		}
	}
	return l
}

// IsUnique checks if all elements in the slice are unique.
// It returns false and the first duplicated element if there are duplicates.
// It returns true and a zero value if the slice is empty or all elements are unique.
func IsUnique[T comparable](l []T) (bool, T) {
	var n T
	if len(l) < 2 {
		return true, n
	}
	set := mapset.NewThreadUnsafeSetWithSize[T](len(l))
	for _, t := range l {
		if !set.Add(t) {
			return false, t
		}
	}
	return true, n
}

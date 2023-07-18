package typeutil

import (
	"sort"
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
// Use like this:
//
//	l := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 9}
//	l = SortAndUnique(l, func(i, j int) bool { return i > j })
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

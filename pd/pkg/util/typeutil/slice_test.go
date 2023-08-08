package typeutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFilterZero(t *testing.T) {
	type s struct {
		i int
	}
	type testCase[T comparable] struct {
		in   []T
		want []T
	}

	t.Parallel()
	re := require.New(t)

	testValue := testCase[s]{
		in:   []s{{i: 0}, {i: 1}, {i: 2}},
		want: []s{{i: 1}, {i: 2}},
	}
	testPointer := testCase[*s]{
		in:   []*s{nil, {i: 0}, {i: 1}, {i: 2}},
		want: []*s{{i: 0}, {i: 1}, {i: 2}},
	}

	re.Equal(testValue.want, FilterZero[s](testValue.in))
	re.Equal(testPointer.want, FilterZero[*s](testPointer.in))
}

func TestSortAndUnique(t *testing.T) {
	type args[T comparable] struct {
		l    []T
		less func(i, j T) bool
	}
	type testCase[T comparable] struct {
		name string
		args args[T]
		want []T
	}
	tests := []testCase[int]{
		{
			name: "normal case",
			args: args[int]{
				l:    []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
				less: func(i, j int) bool { return i < j },
			},
			want: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			name: "sort and unique",
			args: args[int]{
				l:    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 9},
				less: func(i, j int) bool { return i > j },
			},
			want: []int{9, 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			name: "empty slice",
			args: args[int]{
				l:    []int{},
				less: func(i, j int) bool { return i > j },
			},
			want: []int{},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			re := require.New(t)

			l := SortAndUnique(tt.args.l, tt.args.less)
			re.Equal(tt.want, l)
		})
	}
}

func TestIsUnique(t *testing.T) {
	type args[T comparable] struct {
		l []T
	}
	type want[T comparable] struct {
		ok  bool
		dup T
	}
	type testCase[T comparable] struct {
		name string
		args args[T]
		want want[T]
	}
	tests := []testCase[int]{
		{
			name: "normal case",
			args: args[int]{
				l: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
			want: want[int]{
				ok: true,
			},
		},
		{
			name: "not unique",
			args: args[int]{
				l: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 9},
			},
			want: want[int]{
				ok:  false,
				dup: 9,
			},
		},
		{
			name: "empty slice",
			args: args[int]{
				l: []int{},
			},
			want: want[int]{
				ok: true,
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			re := require.New(t)
			ok, dup := IsUnique(tt.args.l)
			re.Equal(tt.want.ok, ok)
			re.Equal(tt.want.dup, dup)
		})
	}
}

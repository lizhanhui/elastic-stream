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

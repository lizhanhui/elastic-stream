package cache

import (
	"reflect"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
)

// Test_isRangeServerEqual will fail if there are new fields in rpcfb.RangeServerT
func Test_isRangeServerEqual(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	var rangeServer1, rangeServer2 rpcfb.RangeServerT
	_ = gofakeit.New(1).Struct(&rangeServer1)
	_ = gofakeit.New(2).Struct(&rangeServer2)

	rangeServer2.ServerId = rangeServer1.ServerId
	rangeServer2.AdvertiseAddr = rangeServer1.AdvertiseAddr
	rangeServer2.State = rangeServer1.State

	// If this test fails, please update the `isRangeServerEqual` function
	re.True(reflect.DeepEqual(rangeServer1, rangeServer2))
	re.True(isRangeServerEqual(rangeServer1, rangeServer2))
}

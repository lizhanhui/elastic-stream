package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	"github.com/AutoMQ/pd/pkg/util/testutil"
)

func TestNoNewFieldsInCreateRangeParam(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	ignoredFields := []string{
		"End",
		"Servers",
		"ReplicaCount",
		"AckCount",
		"OffloadOwner",
	}
	rangeFields := testutil.GetAllFields(rpcfb.RangeT{})
	createRangeParamFields := testutil.GetAllFields(CreateRangeParam{})
	for i := range createRangeParamFields {
		if createRangeParamFields[i] == "StreamID" {
			createRangeParamFields[i] = "StreamId"
		}
	}
	createRangeParamFields = append(createRangeParamFields, ignoredFields...)

	// If this test fails, please update the `CreateRangeParam` struct or the `ignoredFields`
	re.ElementsMatch(rangeFields, createRangeParamFields)
}

func TestNoNewFieldsInSealRangeParam(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	ignoredFields := []string{
		"Start",
		"Servers",
		"ReplicaCount",
		"AckCount",
		"OffloadOwner",
	}
	rangeFields := testutil.GetAllFields(rpcfb.RangeT{})
	sealRangeParamFields := testutil.GetAllFields(SealRangeParam{})
	for i := range sealRangeParamFields {
		if sealRangeParamFields[i] == "StreamID" {
			sealRangeParamFields[i] = "StreamId"
		}
	}
	sealRangeParamFields = append(sealRangeParamFields, ignoredFields...)

	// If this test fails, please update the `SealRangeParam` struct or the `ignoredFields`
	re.ElementsMatch(rangeFields, sealRangeParamFields)
}

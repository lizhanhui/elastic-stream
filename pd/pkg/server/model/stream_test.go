package model

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
	testutil "github.com/AutoMQ/pd/pkg/util/test"
)

func TestNoNewFieldsInCreateStreamParam(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	ignoredFields := []string{
		"StreamId",
		"StartOffset",
		"Epoch",
		"Deleted",
	}
	streamFields := testutil.GetAllFields(rpcfb.StreamT{})
	createStreamParamFields := testutil.GetAllFields(CreateStreamParam{})
	createStreamParamFields = append(createStreamParamFields, ignoredFields...)

	// If this test fails, please update the `CreateStreamParam` struct or the `ignoredFields`
	re.ElementsMatch(streamFields, createStreamParamFields)
}

func TestNoNewFieldsInUpdateStreamParam(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	ignoredFields := []string{
		"StartOffset",
		"Deleted",
	}
	streamFields := testutil.GetAllFields(rpcfb.StreamT{})
	updateStreamParamFields := testutil.GetAllFields(UpdateStreamParam{})
	for i := range updateStreamParamFields {
		if updateStreamParamFields[i] == "StreamID" {
			updateStreamParamFields[i] = "StreamId"
		}
	}
	updateStreamParamFields = append(updateStreamParamFields, ignoredFields...)

	// If this test fails, please update the `UpdateStreamParam` struct or the `ignoredFields`
	re.ElementsMatch(streamFields, updateStreamParamFields)
}

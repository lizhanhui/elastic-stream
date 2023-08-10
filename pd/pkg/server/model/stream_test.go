package model

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/AutoMQ/pd/api/rpcfb/rpcfb"
)

func TestNoNewFieldsInCreateStreamParam(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	ignoredFields := []string{
		"StreamId",
		"StartOffset",
		"Epoch",
	}
	streamFields := getAllFields(rpcfb.StreamT{})
	createStreamParamFields := getAllFields(CreateStreamParam{})
	createStreamParamFields = append(createStreamParamFields, ignoredFields...)

	// If this test fails, please update the `CreateStreamParam` struct or the `ignoredFields`
	re.ElementsMatch(streamFields, createStreamParamFields)
}

func TestNoNewFieldsInUpdateStreamParam(t *testing.T) {
	t.Parallel()
	re := require.New(t)

	ignoredFields := []string{
		"StartOffset",
		"Epoch",
	}
	streamFields := getAllFields(rpcfb.StreamT{})
	updateStreamParamFields := getAllFields(UpdateStreamParam{})
	for i := range updateStreamParamFields {
		if updateStreamParamFields[i] == "StreamID" {
			updateStreamParamFields[i] = "StreamId"
		}
	}
	updateStreamParamFields = append(updateStreamParamFields, ignoredFields...)

	// If this test fails, please update the `UpdateStreamParam` struct or the `ignoredFields`
	re.ElementsMatch(streamFields, updateStreamParamFields)
}

func getAllFields(i any) (fields []string) {
	t := reflect.TypeOf(i)
	for i := 0; i < t.NumField(); i++ {
		fields = append(fields, t.Field(i).Name)
	}
	return
}

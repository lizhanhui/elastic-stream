package protocol

import (
	"github.com/pkg/errors"
)

type unknownFormatter struct{}

var errUnsupported = errors.New("unsupported format")

func (u unknownFormatter) unmarshalListRangeRequest(_ []byte, _ *ListRangeRequest) error {
	return errUnsupported
}

func (u unknownFormatter) marshalListRangeResponse(_ *ListRangeResponse) ([]byte, error) {
	return nil, errUnsupported
}

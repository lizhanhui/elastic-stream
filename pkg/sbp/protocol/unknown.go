package protocol

import (
	"github.com/pkg/errors"
)

type unknownFormatter struct{}

var errUnsupported = errors.New("unsupported format")

func (u unknownFormatter) unmarshalListRangesRequest(_ []byte, _ *ListRangesRequest) error {
	return errUnsupported
}

func (u unknownFormatter) marshalListRangesResponse(_ *ListRangesResponse) ([]byte, error) {
	return nil, errUnsupported
}

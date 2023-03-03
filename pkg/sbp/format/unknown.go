package format

import (
	"github.com/pkg/errors"
)

type unknownFormatter struct{}

var errUnsupported = errors.New("unsupported format")

func (u unknownFormatter) MarshalListRangeRequest(_ *ListRangeRequest) ([]byte, error) {
	return nil, errUnsupported
}

func (u unknownFormatter) UnmarshalListRangeRequest(_ []byte, _ *ListRangeRequest) error {
	return errUnsupported
}

func (u unknownFormatter) MarshalListRangeResponse(_ *ListRangeResponse) ([]byte, error) {
	return nil, errUnsupported
}

func (u unknownFormatter) UnmarshalListRangeResponse(_ []byte, _ *ListRangeResponse) error {
	return errUnsupported
}

package format

const (
	unknown uint8 = iota
	flatBuffer
	protoBuffer
	json
)

var (
	_flatBuffer  = Format{flatBuffer, newFlatBufferFormatter()}
	_protoBuffer = Format{protoBuffer, &unknownFormatter{}}
	_json        = Format{json, &unknownFormatter{}}
	_unknown     = Format{unknown, &unknownFormatter{}}
)

// Format is enumeration of Frame.HeaderFmt
type Format struct {
	code      uint8
	formatter Formatter
}

// NewFormat new a format with code
func NewFormat(code uint8) Format {
	switch code {
	case flatBuffer:
		return _flatBuffer
	case protoBuffer:
		return _protoBuffer
	case json:
		return _json
	default:
		return _unknown
	}
}

// Formatter returns the formatter of this format
func (f Format) Formatter() Formatter {
	return f.formatter
}

// String implements fmt.Stringer
func (f Format) String() string {
	switch f.code {
	case flatBuffer:
		return "FlatBuffer"
	case protoBuffer:
		return "ProtoBuffer"
	case json:
		return "JSON"
	default:
		return "Unknown"
	}
}

// Code returns the format code
func (f Format) Code() uint8 {
	return f.code
}

// Default returns a default format, used when header is empty
func Default() Format {
	return _flatBuffer
}

// FlatBuffer serializes and deserializes the header using "github.com/google/flatbuffers/go"
func FlatBuffer() Format {
	return _flatBuffer
}

// ProtoBuffer serializes and deserializes the header using "github.com/golang/protobuf/proto"
func ProtoBuffer() Format {
	return _protoBuffer
}

// JSON serializes and deserializes the header using "encoding/json"
func JSON() Format {
	return _json
}

type Formatter interface {
	MarshalListRangeRequest(*ListRangeRequest) ([]byte, error)
	UnmarshalListRangeRequest([]byte, *ListRangeRequest) error
	MarshalListRangeResponse(*ListRangeResponse) ([]byte, error)
	UnmarshalListRangeResponse([]byte, *ListRangeResponse) error
}

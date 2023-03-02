package format

const (
	unknown uint8 = iota
	flatBuffer
	protoBuffer
	json
)

var (
	_flatBuffer  = Format{flatBuffer}
	_protoBuffer = Format{protoBuffer}
	_json        = Format{json}
	_unknown     = Format{unknown}
)

// Format is enumeration of Frame.headerFmt
type Format struct {
	code uint8
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

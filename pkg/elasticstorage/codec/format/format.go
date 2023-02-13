package format

type Code uint8

const (
	unknown Code = iota
	flatBuffer
	protoBuffer
	json
)

// Format is enumeration of Frame.headerFmt
type Format struct {
	code Code
}

func NewFormat(code uint8) Format {
	switch Code(code) {
	case flatBuffer:
		return Format{flatBuffer}
	case protoBuffer:
		return Format{protoBuffer}
	case json:
		return Format{json}
	default:
		return Format{unknown}
	}
}

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

func (f Format) Code() uint8 {
	return uint8(f.code)
}

func FlatBufferEnum() Format {
	return Format{flatBuffer}
}

func ProtoBufferEnum() Format {
	return Format{protoBuffer}
}

func JSONEnum() Format {
	return Format{json}
}

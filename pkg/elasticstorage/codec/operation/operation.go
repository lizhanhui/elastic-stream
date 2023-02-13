package operation

const (
	unknown uint16 = iota
	ping
	goAway
	publish
	heartbeat
	listRange
)

// Operation is enumeration of Frame.opCode
type Operation struct {
	code uint16
}

// NewOperation new an operation with code
func NewOperation(code uint16) Operation {
	switch code {
	case ping:
		return Operation{ping}
	case goAway:
		return Operation{goAway}
	case publish:
		return Operation{publish}
	case heartbeat:
		return Operation{heartbeat}
	case listRange:
		return Operation{listRange}
	default:
		return Operation{unknown}
	}
}

// String implements fmt.Stringer
func (o Operation) String() string {
	switch o.code {
	case ping:
		return "Ping"
	case goAway:
		return "GoAway"
	case publish:
		return "Publish"
	case heartbeat:
		return "Heartbeat"
	case listRange:
		return "ListRange"
	default:
		return "Unknown"
	}
}

// Code returns the operation code
func (o Operation) Code() uint16 {
	return o.code
}

// Ping frame is a mechanism for measuring a minimal round-trip time from the sender,
// as well as determining whether an idle connection is still functional
func Ping() Operation {
	return Operation{ping}
}

// GoAway frame is used to initiate shutdown of a connection or to signal serious error conditions
func GoAway() Operation {
	return Operation{goAway}
}

// Publish frame is used to publish message(s)
func Publish() Operation {
	return Operation{publish}
}

// Heartbeat frame is used to send heartbeat
func Heartbeat() Operation {
	return Operation{heartbeat}
}

// ListRange frame is used to list ranges in a stream
func ListRange() Operation {
	return Operation{listRange}
}

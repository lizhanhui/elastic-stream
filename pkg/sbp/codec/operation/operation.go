package operation

const (
	unknown uint16 = iota
	ping
	goAway
	publish
	heartbeat
	listRange
)

var (
	_ping      = Operation{ping}
	_goAway    = Operation{goAway}
	_publish   = Operation{publish}
	_heartbeat = Operation{heartbeat}
	_listRange = Operation{listRange}
	_unknown   = Operation{unknown}
)

// Operation is enumeration of Frame.opCode
type Operation struct {
	code uint16
}

// NewOperation new an operation with code
func NewOperation(code uint16) Operation {
	switch code {
	case ping:
		return _ping
	case goAway:
		return _goAway
	case publish:
		return _publish
	case heartbeat:
		return _heartbeat
	case listRange:
		return _listRange
	default:
		return _unknown
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

// IsControl returns whether o is a control operation
func (o Operation) IsControl() bool {
	switch o.code {
	case ping, goAway, heartbeat:
		return true
	default:
		return false
	}
}

// Ping frame is a mechanism for measuring a minimal round-trip time from the sender,
// as well as determining whether an idle connection is still functional
func Ping() Operation {
	return _ping
}

// GoAway frame is used to initiate shutdown of a connection or to signal serious error conditions
func GoAway() Operation {
	return _goAway
}

// Publish frame is used to publish message(s)
func Publish() Operation {
	return _publish
}

// Heartbeat frame is used to send heartbeat
func Heartbeat() Operation {
	return _heartbeat
}

// ListRange frame is used to list ranges in a stream
func ListRange() Operation {
	return _listRange
}

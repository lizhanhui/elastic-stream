package operation

type Code uint16

const (
	unknown Code = iota
	ping
	goAway
	publish
	heartbeat
	listRange
)

// Operation is enumeration of Frame.opCode
type Operation struct {
	code Code
}

func NewOperation(code Code) Operation {
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

func Ping() Operation {
	return Operation{ping}
}

func GoAway() Operation {
	return Operation{goAway}
}

func Publish() Operation {
	return Operation{publish}
}

func Heartbeat() Operation {
	return Operation{heartbeat}
}

func ListRange() Operation {
	return Operation{listRange}
}

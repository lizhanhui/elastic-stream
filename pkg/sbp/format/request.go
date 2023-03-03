package format

// Request is an SBP request
type Request interface {
	// Unmarshal decodes data into the Request using the specified format
	Unmarshal(fmt Format, data []byte) error
}

// ListRangeRequest is a request to operation.ListRange
type ListRangeRequest struct {
	TimeoutMs uint32

	// The range owner could be a data node or a list of streams.
	RangeOwners []RangeOwner
}

func (l *ListRangeRequest) Unmarshal(fmt Format, data []byte) error {
	return fmt.Formatter().UnmarshalListRangeRequest(data, l)
}

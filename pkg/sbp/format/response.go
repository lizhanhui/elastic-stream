package format

// Response is an SBP response
type Response interface {
	// Marshal encodes the Response using the specified format
	Marshal(fmt Format) ([]byte, error)
}

// ListRangeResponse is a response to operation.ListRange
type ListRangeResponse struct {
	// The time in milliseconds to throttle the client, due to a quota violation or the server is too busy.
	ThrottleTimeMs uint32

	// The responses of list ranges request
	ListResponses []ListRangesResult
}

func (l *ListRangeResponse) Marshal(fmt Format) ([]byte, error) {
	return fmt.Formatter().MarshalListRangeResponse(l)
}

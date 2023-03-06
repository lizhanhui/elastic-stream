package protocol

// ErrorCode is the error code of a response
type ErrorCode int16

const (
	// None means no error
	None ErrorCode = 0

	// Unknown means unexpected server error
	Unknown ErrorCode = 1

	// InvalidRequest means the request is invalid
	InvalidRequest ErrorCode = 2
)

// TimeoutMs is the timeout in request in milliseconds
type TimeoutMs = int32

// ThrottleTimeMs is the time in milliseconds to throttle the client, due to a quota violation or the server is too busy.
type ThrottleTimeMs = int32

// NodeID is the ID of a data node
type NodeID = int32

// StreamID is the ID of a stream
type StreamID = int64

// RangeIndex is the index of a range in a stream
type RangeIndex = int32

// RangeOffset is an offset in a range
type RangeOffset = int64

// ListRangesResult is part of the ListRangesRequest
type ListRangesResult struct {
	// The owner that the returned ranges belong to.
	// Maybe a data node or a stream id.
	RangeOwner RangeOwner

	// The error code, or 0 if there was no error.
	ErrorCode ErrorCode

	// The error message, or omitted if there was no error.
	ErrorMessage string

	// The list of ranges.
	Ranges []Range
}

// RangeOwner is used to fetch the ranges from a specific data node or a specific stream list.
type RangeOwner struct {
	DataNode DataNode

	// The stream id to list the ranges.
	StreamID StreamID
}

// DataNode is a data node in the cluster
type DataNode struct {
	// The node id of the data node.
	NodeID NodeID

	// The advertisement address of the data node, for client traffic from outside.
	// The schema of the address is `host:port`, while host supports both domain name and IPv4/IPv6 address.
	AdvertiseAddr string
}

// Range is a range in a stream
type Range struct {
	// The id of the stream
	StreamID StreamID

	// The index of the range in the stream.
	RangeIndex RangeIndex

	// The start offset of the range.
	StartOffset RangeOffset

	// The end offset of the range.
	// Omitted if the range is open.
	EndOffset RangeOffset

	// The next writable offset for incoming records of the range.
	// It's a snapshot of the next offset of the range, and it may be changed after the response is sent.
	NextOffset RangeOffset

	// The set of all nodes that host this range.
	ReplicaNodes []ReplicaNode
}

// ReplicaNode is a replica node of a range
type ReplicaNode struct {
	DataNode  DataNode
	IsPrimary bool
}

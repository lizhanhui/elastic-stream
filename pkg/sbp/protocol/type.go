package protocol

// ErrorCode is the error code of a response
type ErrorCode uint16

const (
	// None means no error
	None ErrorCode = 0

	// Unknown means unexpected server error
	Unknown ErrorCode = 1

	// InvalidRequest means the request is invalid
	InvalidRequest ErrorCode = 2
)

// NodeID is the ID of a data node
type NodeID uint64

// StreamID is the ID of a stream
type StreamID uint64

// RangeIndex is the index of a range in a stream
type RangeIndex uint32

// RangeOffset is an offset in a range
type RangeOffset uint64

// ListRangesResult is part of the ListRangeRequest
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

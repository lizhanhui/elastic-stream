package codec

import (
	"github.com/AutoMQ/placement-manager/pkg/elasticstorage/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/elasticstorage/codec/operation"
)

// Frame is the load that communicates with Elastic Storage.
//
//	+-----------------------------------------------------------------------+
//	|                           Frame Length (32)                           |
//	+-----------------+-----------------------------------+-----------------+
//	|  Magic Code (8) |        Operation Code (16)        |     Flag (8)    |
//	+-----------------+-----------------------------------+-----------------+
//	|                         Stream Identifier (32)                        |
//	+-----------------+-----------------------------------------------------+
//	|Header Format (8)|                  Header Length (24)                 |
//	+-----------------+-----------------------------------------------------+
//	|                             Header (0...)                           ...
//	+-----------------------------------------------------------------------+
//	|                             Payload (0...)                          ...
//	+-----------------------------------------------------------------------+
//	|                         Payload Checksum (32)                         |
//	+-----------------------------------------------------------------------+

type Frame struct {
	opCode    operation.Operation // opCode determines the format and semantics of the frame
	flag      uint8               // flag is reserved for boolean flags specific to the frame type
	streamId  uint32              // streamId identifies which stream the frame belongs to
	headerFmt format.Format       // headerFmt identifies the format of the header.
	header    []byte
	payload   []byte
}

func NewFrame() {
	// TODO
}

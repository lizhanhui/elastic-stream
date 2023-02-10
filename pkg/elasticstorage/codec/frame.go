package codec

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/elasticstorage/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/elasticstorage/codec/operation"
)

const (
	_fixedHeaderLen = 16
	_minFrameLen    = _fixedHeaderLen
	_maxFrameLen    = 16 * 1024 * 1024

	_magicCode = 23
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
	streamID  uint32              // streamID identifies which stream the frame belongs to
	headerFmt format.Format       // headerFmt identifies the format of the header.
	header    []byte
	payload   []byte
}

// Framer reads and writes Frames
type Framer struct {
	w io.Writer
	r io.Reader

	streamID atomic.Uint32

	// fixedBuf is used to cache the fixed length portion in the frame
	fixedBuf [_fixedHeaderLen]byte

	lg *zap.Logger
}

func NewFramer(w io.Writer, r io.Reader, logger *zap.Logger) *Framer {
	framer := &Framer{
		w:  w,
		r:  r,
		lg: logger,
	}
	framer.streamID = atomic.Uint32{}
	return framer
}

func (fr *Framer) NewFrame(opCode uint16) Frame {
	return Frame{
		opCode:    operation.NewOperation(opCode),
		streamID:  fr.nextID(),
		headerFmt: format.FlatBufferEnum(),
	}
}

func (fr *Framer) nextID() uint32 {
	return fr.streamID.Add(1)
}

// ReadFrame reads a single frame
func (fr *Framer) ReadFrame() (Frame, error) {
	logger := fr.lg

	buf := fr.fixedBuf[:_fixedHeaderLen]
	_, err := io.ReadFull(fr.r, buf)
	if err != nil {
		return Frame{}, errors.Wrap(err, "read fixed header")
	}
	headerBuf := bytes.NewBuffer(buf)

	frameLen := binary.BigEndian.Uint32(headerBuf.Next(4))
	if frameLen < _minFrameLen {
		logger.Error("illegal frame length, fewer than minimum.", zap.Uint32("frame-length", frameLen), zap.Uint32("min-length", _minFrameLen))
		return Frame{}, errors.New("frame too small")
	}
	if frameLen > _maxFrameLen {
		logger.Error("illegal frame length, greater than maximum.", zap.Uint32("frame-length", frameLen), zap.Uint32("max-length", _maxFrameLen))
		return Frame{}, errors.New("frame too large")
	}

	magicCode := headerBuf.Next(1)[0]
	if magicCode != _magicCode {
		logger.Error("illegal magic code.", zap.Uint8("expected", _magicCode), zap.Uint8("got", magicCode))
		return Frame{}, errors.New("magic code mismatch")
	}

	opCode := binary.BigEndian.Uint16(headerBuf.Next(2))
	flag := headerBuf.Next(1)[0]
	streamID := binary.BigEndian.Uint32(headerBuf.Next(4))
	headerFmt := headerBuf.Next(1)[0]
	headerLen := uint32(headerBuf.Next(1)[0])<<16 | uint32(binary.BigEndian.Uint16(headerBuf.Next(2)))
	payloadLen := frameLen + 4 - _fixedHeaderLen - headerLen - 4 // add frameLength width, sub payloadChecksum width

	// TODO malloc and free buffer by github.com/bytedance/gopkg/lang/mcache
	tBuf := make([]byte, headerLen+payloadLen)
	_, err = io.ReadFull(fr.r, tBuf)
	if err != nil {
		return Frame{}, errors.Wrap(err, "read extended header and payload")
	}

	header := tBuf[:headerLen]
	payload := tBuf[headerLen:]

	if payloadLen > 0 {
		var checksum uint32
		err = binary.Read(fr.r, binary.BigEndian, &checksum)
		if err != nil {
			return Frame{}, errors.Wrap(err, "read payload checksum")
		}
		if ckm := crc32.ChecksumIEEE(payload); ckm != checksum {
			logger.Error("payload checksum mismatch.", zap.Uint32("expected", checksum), zap.Uint32("got", ckm))
			return Frame{}, errors.New("payload checksum mismatch")
		}
	}

	frame := Frame{
		opCode:    operation.NewOperation(opCode),
		flag:      flag,
		streamID:  streamID,
		headerFmt: format.NewFormat(headerFmt),
		header:    header,
		payload:   payload,
	}

	return frame, nil
}

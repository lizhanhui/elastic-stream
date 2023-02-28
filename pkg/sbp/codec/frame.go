package codec

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/format"
	"github.com/AutoMQ/placement-manager/pkg/sbp/codec/operation"
)

const (
	_fixedHeaderLen = 16
	_minFrameLen    = _fixedHeaderLen - 4 + 4 // fixed header - header length + checksum
	_maxFrameLen    = 16 * 1024 * 1024

	_magicCode uint8 = 23
)

// Frame is the load in SBP.
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
	OpCode    operation.Operation // OpCode determines the format and semantics of the frame
	Flag      uint8               // Flag is reserved for boolean flags specific to the frame type
	StreamID  uint32              // StreamID identifies which stream the frame belongs to
	HeaderFmt format.Format       // HeaderFmt identifies the format of the Header.
	Header    []byte              // nil for no extended header
	Payload   []byte              // nil for no payload
}

// Framer reads and writes Frames
type Framer struct {
	streamID atomic.Uint32

	r io.Reader
	// fixedBuf is used to cache the fixed length portion in the frame
	fixedBuf [_fixedHeaderLen]byte

	w    io.Writer
	wbuf []byte

	lg *zap.Logger
}

// NewFramer returns a Framer that writes frames to w and reads them from r
func NewFramer(w io.Writer, r io.Reader, logger *zap.Logger) *Framer {
	framer := &Framer{
		w:  w,
		r:  r,
		lg: logger,
	}
	framer.streamID = atomic.Uint32{}
	return framer
}

// NextID generates the next new StreamID
func (fr *Framer) NextID() uint32 {
	return fr.streamID.Add(1)
}

// ReadFrame reads a single frame
func (fr *Framer) ReadFrame() (Frame, error) {
	logger := fr.lg

	buf := fr.fixedBuf[:_fixedHeaderLen]
	_, err := io.ReadFull(fr.r, buf)
	if err != nil {
		logger.Error("failed to read fixed header", zap.Error(err))
		return Frame{}, errors.Wrap(err, "read fixed header")
	}
	headerBuf := bytes.NewBuffer(buf)

	frameLen := binary.BigEndian.Uint32(headerBuf.Next(4))
	if frameLen < _minFrameLen {
		logger.Error("illegal frame length, fewer than minimum", zap.Uint32("frame-length", frameLen), zap.Uint32("min-length", _minFrameLen))
		return Frame{}, errors.New("frame too small")
	}
	if frameLen > _maxFrameLen {
		logger.Error("illegal frame length, greater than maximum", zap.Uint32("frame-length", frameLen), zap.Uint32("max-length", _maxFrameLen))
		return Frame{}, errors.New("frame too large")
	}

	magicCode := headerBuf.Next(1)[0]
	if magicCode != _magicCode {
		logger.Error("illegal magic code", zap.Uint8("expected", _magicCode), zap.Uint8("got", magicCode))
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
		logger.Error("failed to read extended header and payload", zap.Error(err))
		return Frame{}, errors.Wrap(err, "read extended header and payload")
	}

	header := func() []byte {
		if headerLen == 0 {
			return nil
		}
		return tBuf[:headerLen]
	}()
	payload := func() []byte {
		if payloadLen == 0 {
			return nil
		}
		return tBuf[headerLen:]
	}()

	var checksum uint32
	err = binary.Read(fr.r, binary.BigEndian, &checksum)
	if err != nil {
		logger.Error("failed to read payload checksum", zap.Error(err))
		return Frame{}, errors.Wrap(err, "read payload checksum")
	}
	if payloadLen > 0 {
		if ckm := crc32.ChecksumIEEE(payload); ckm != checksum {
			logger.Error("payload checksum mismatch", zap.Uint32("expected", ckm), zap.Uint32("got", checksum))
			return Frame{}, errors.New("payload checksum mismatch")
		}
	}

	frame := Frame{
		OpCode:    operation.NewOperation(opCode),
		Flag:      flag,
		StreamID:  streamID,
		HeaderFmt: format.NewFormat(headerFmt),
		Header:    header,
		Payload:   payload,
	}

	return frame, nil
}

// WriteFrame writes a frame
//
// It will perform exactly one Write to the underlying Writer.
// It is the caller's responsibility not to violate the maximum frame size
// and to not call other Write methods concurrently.
func (fr *Framer) WriteFrame(frame Frame) error {
	fr.startWrite(frame)

	if frame.Header != nil {
		fr.wbuf = append(fr.wbuf, frame.Header...)
	}
	if frame.Payload != nil {
		fr.wbuf = append(fr.wbuf, frame.Payload...)
		fr.wbuf = binary.BigEndian.AppendUint32(fr.wbuf, crc32.ChecksumIEEE(frame.Payload))
	} else {
		// dummy checksum
		fr.wbuf = binary.BigEndian.AppendUint32(fr.wbuf, 0)
	}

	return fr.endWrite()
}

// Write the fixed header
func (fr *Framer) startWrite(frame Frame) {
	fr.wbuf = binary.BigEndian.AppendUint32(fr.wbuf, 0) // 4 bytes of frame length, will be filled in endWrite
	fr.wbuf = append(fr.wbuf, _magicCode)
	fr.wbuf = binary.BigEndian.AppendUint16(fr.wbuf, frame.OpCode.Code())
	fr.wbuf = append(fr.wbuf, frame.Flag)
	fr.wbuf = binary.BigEndian.AppendUint32(fr.wbuf, frame.StreamID)
	fr.wbuf = append(fr.wbuf, frame.HeaderFmt.Code())
	headerLen := len(frame.Header)
	fr.wbuf = append(fr.wbuf, byte(headerLen>>16), byte(headerLen>>8), byte(headerLen))
}

func (fr *Framer) endWrite() error {
	logger := fr.lg
	// Now that we know the final size, fill in the FrameHeader in
	// the space previously reserved for it. Abuse append.
	length := len(fr.wbuf) - 4 // sub frameLen width
	if length > (_maxFrameLen) {
		logger.Error("frame too large, greater than maximum", zap.Int("frame-length", length), zap.Uint32("max-length", _maxFrameLen))
		return errors.New("frame too large")
	}
	_ = binary.BigEndian.AppendUint32(fr.wbuf[:0], uint32(length))

	_, err := fr.w.Write(fr.wbuf)
	if err != nil {
		logger.Error("failed to write frame", zap.Error(err))
		return errors.Wrap(err, "write frame")
	}
	return nil
}

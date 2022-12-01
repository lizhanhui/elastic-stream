use std::io::Cursor;

use byteorder::ReadBytesExt;
use bytes::{Buf, Bytes, BytesMut};
use crc::Crc;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::error::FrameError;

pub(crate) const MAGIC_CODE: u8 = 23;

pub(crate) const MIN_FRAME_LENGTH: u32 = 16;

// Max frame length 16MB
pub(crate) const MAX_FRAME_LENGTH: u32 = 16 * 1024 * 1024;

const CRC32: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISO_HDLC);

pub struct Frame {
    operation_code: OperationCode,
    flag: u8,
    stream_id: u32,
    header_format: HeaderFormat,
    header: Option<Bytes>,
    payload: Option<Bytes>,
}

impl Frame {
    pub(crate) fn check(src: &mut Cursor<&[u8]>) -> Result<(), FrameError> {
        let frame_length = match src.read_u32::<byteorder::NetworkEndian>() {
            Ok(n) => n,
            Err(_) => {
                return Err(FrameError::Incomplete);
            }
        };

        if frame_length < MIN_FRAME_LENGTH {
            return Err(FrameError::BadFrame(format!(
                "Length of the incoming frame is: {}, less than the minimum possible: {}",
                frame_length, MIN_FRAME_LENGTH
            )));
        }

        // Check if the frame length is legal or not.
        if frame_length > MAX_FRAME_LENGTH {
            return Err(FrameError::TooLongFrame {
                found: frame_length,
                max: MAX_FRAME_LENGTH,
            });
        }

        // Check if the frame is complete
        if src.remaining() < frame_length as usize {
            return Err(FrameError::Incomplete);
        }

        // Verify magic code
        let magic_code = src.get_u8();
        if MAGIC_CODE != magic_code {
            return Err(FrameError::MagicCodeMismatch {
                found: magic_code,
                expect: MAGIC_CODE,
            });
        }

        // op code
        src.advance(2);

        // flag
        src.advance(1);

        // stream id
        src.advance(4);

        // header format
        src.advance(1);

        // header length
        let header_length: u32 = src.get_u8() as u32;
        let header_length = header_length << 16 + src.get_u16();
        src.advance(header_length as usize);

        let payload_length = frame_length - header_length - 16;
        let mut payload = None;
        if payload_length > 0 {
            let body = src.copy_to_bytes(payload_length as usize);
            payload = Some(body);
        }

        if let Some(body) = payload {
            let checksum = src.get_u32();
            let mut digest = CRC32.digest();
            digest.update(&body[..]);
            let ckm = digest.finalize();
            if checksum != ckm {
                return Err(FrameError::PayloadChecksumMismatch {
                    expected: checksum,
                    actual: ckm,
                });
            }
        } else {
            // checksum
            src.advance(4);
        }

        Ok(())
    }

    pub(crate) fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
        // Safety: previous `check` method ensures we are having a complete frame to parse
        let frame_length = src.get_u32();
        let mut remaining = frame_length;

        // Skip magic code
        src.advance(1);
        remaining -= 1;

        let op_code = src.get_u16();
        remaining -= 2;
        let op_code = OperationCode::try_from(op_code).unwrap_or(OperationCode::Unknown);

        let flag = src.get_u8();
        remaining -= 1;

        let stream_id = src.get_u32();
        remaining -= 4;

        let header_format = src.get_u8();
        remaining -= 1;
        let header_format = HeaderFormat::try_from(header_format).unwrap_or(HeaderFormat::Unknown);

        let mut frame = Frame {
            operation_code: op_code,
            flag,
            stream_id,
            header_format,
            header: None,
            payload: None,
        };

        let header_length: u32 = src.get_u8() as u32;
        let header_length = header_length << 16 + src.get_u16();
        remaining -= 3;

        if header_length > 0 {
            let header = src.copy_to_bytes(header_length as usize);
            frame.header = Some(header);
        }
        remaining -= header_length;

        let payload_length = remaining - 4;
        if payload_length > 0 {
            let payload = src.copy_to_bytes(payload_length as usize);
            frame.payload = Some(payload);
        }
        remaining -= payload_length;

        // payload checksum
        src.advance(4);
        remaining -= 4;
        debug_assert!(0 == remaining);

        Ok(frame)
    }
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
enum HeaderFormat {
    Unknown = 0,
    FlatBuffer = 1,
    ProtoBuffer = 2,
    JSON = 3,
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u16)]
enum OperationCode {
    Unknown = 0,
    Ping = 1,
    GoAway = 2,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::{BufMut, BytesMut};

    use super::*;

    #[test]
    fn test_num_enum() {
        let res = HeaderFormat::try_from(1u8);
        assert_eq!(Ok(HeaderFormat::FlatBuffer), res);

        let num: u8 = HeaderFormat::JSON.into();
        assert_eq!(3, num);

        let res = OperationCode::try_from(0u16);
        assert_eq!(Ok(OperationCode::Unknown), res);
        let num: u16 = OperationCode::GoAway.into();
        assert_eq!(2, num);
    }

    #[test]
    fn test_check() {
        let raw = [1u8];
        let mut rdr = Cursor::new(&raw[..]);
        let res = Frame::check(&mut rdr);
        assert_eq!(Err(FrameError::Incomplete), res);

        // On read failure, the cursor should be intact.
        assert_eq!(1, rdr.remaining());
    }

    #[test]
    fn test_check_min_frame_length() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(10);

        let mut cursor = Cursor::new(&buffer[..]);
        match Frame::check(&mut cursor) {
            Ok(_) => {
                panic!("Should have detected the frame length issue");
            }
            Err(e) => {
                assert_eq!(
                    FrameError::BadFrame(
                        "Length of the incoming frame is: 10, less than the minimum possible: 16"
                            .to_owned()
                    ),
                    e
                );
            }
        }
    }

    #[test]
    fn test_check_max_frame_length() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MAX_FRAME_LENGTH + 1);

        let mut cursor = Cursor::new(&buffer[..]);
        match Frame::check(&mut cursor) {
            Ok(_) => {
                panic!("Should have detected the frame length issue");
            }
            Err(e) => {
                assert_eq!(
                    FrameError::TooLongFrame {
                        found: MAX_FRAME_LENGTH + 1,
                        max: MAX_FRAME_LENGTH
                    },
                    e
                );
            }
        }
    }

    #[test]
    fn test_check_magic_code() {
        let mut buffer = BytesMut::new();
        buffer.put_u32(MIN_FRAME_LENGTH);
        // magic code
        buffer.put_u8(16u8);
        // operation code
        buffer.put_u16(OperationCode::Ping.into());
        // flag
        buffer.put_u8(0u8);
        // stream identifier
        buffer.put_u32(2);
        // header format + header length
        buffer.put_u32(0);
        // header
        // payload
        // payload checksum
        buffer.put_u32(0);

        let mut cursor = Cursor::new(&buffer[..]);
        match Frame::check(&mut cursor) {
            Ok(_) => {
                panic!("Should have detected the frame magic code mismatch issue");
            }
            Err(e) => {
                assert_eq!(
                    FrameError::MagicCodeMismatch {
                        found: 16u8,
                        expect: MAGIC_CODE
                    },
                    e
                );
            }
        }
    }
}

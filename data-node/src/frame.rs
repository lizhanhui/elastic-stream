use std::io::Cursor;

use byteorder::ReadBytesExt;
use bytes::{Buf, Bytes};
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::error::FrameError;

pub(crate) const MAGIC_CODE: u8 = 23;

pub(crate) const MIN_FRAME_LENGTH: u32 = 16;

// Max frame length 16MB
pub(crate) const MAX_FRAME_LENGTH: u32 = 16 * 1024 * 1024;

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

        // Skip validating the rest fields, leaving them to the `parse` method.
        src.advance(frame_length as usize - 1);

        Ok(())
    }

    pub(crate) fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, FrameError> {
        // Safety: `check` method ensures
        let frame_length = src.get_u32();

        todo!()
    }
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
enum HeaderFormat {
    FlatBuffer = 0,
    ProtoBuffer = 1,
    JSON = 2,
}

#[derive(Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u16)]
enum OperationCode {
    Ping = 0,
    GoAway = 1,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use bytes::{BufMut, BytesMut};

    use super::*;

    #[test]
    fn test_num_enum() {
        let res = HeaderFormat::try_from(0u8);
        assert_eq!(Ok(HeaderFormat::FlatBuffer), res);

        let num: u8 = HeaderFormat::JSON.into();
        assert_eq!(2, num);

        let res = OperationCode::try_from(0u16);
        assert_eq!(Ok(OperationCode::Ping), res);
        let num: u16 = OperationCode::GoAway.into();
        assert_eq!(1, num);
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

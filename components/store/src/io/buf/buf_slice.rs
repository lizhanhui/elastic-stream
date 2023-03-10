use std::{ops::Deref, sync::Arc};

use super::AlignedBuf;

/// Expose cached `Record` to application layer.
///
/// This struct is designed to be pub and `Send`.
pub struct BufSlice {
    /// Shared block cache.
    buf: Arc<AlignedBuf>,

    /// Position within the aligned buffer where the expected payload starts.
    pos: u32,

    /// Limit boundary of the requested `Record`.
    limit: u32,
}

impl BufSlice {
    /// Constructor of the `BufSlice`. Expected to be used internally in the store crate.
    pub(crate) fn new(buf: Arc<AlignedBuf>, pos: u32, limit: u32) -> Self {
        debug_assert!(pos <= limit);
        debug_assert!(limit <= buf.capacity as u32);
        Self { buf, pos, limit }
    }
}

/// Implement `Deref` to `&[u8]` so that application developers may inspect this buffer conveniently.
impl Deref for BufSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let base = unsafe { self.buf.as_ptr().offset(self.pos as isize) };
        unsafe { std::slice::from_raw_parts(base, (self.limit - self.pos) as usize) }
    }
}

/// Implement `Into<&[u8]>`, such that data can be transferred to network without copy.
///
/// If encryption is not needed, we are offering zero-copy out of box.
impl From<&BufSlice> for &[u8] {
    fn from(value: &BufSlice) -> Self {
        let base = unsafe { value.buf.as_ptr().offset(value.pos as isize) };
        unsafe { std::slice::from_raw_parts(base, (value.limit - value.pos) as usize) }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use crate::io::buf::AlignedBuf;

    #[test]
    fn test_buf_slice() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let aligned_buf = Arc::new(AlignedBuf::new(log, 0, 4096, 4096)?);
        let data = "Hello";
        aligned_buf.write_u32(data.len() as u32);
        aligned_buf.write_buf(data.as_bytes());
        let slice_buf = super::BufSlice::new(Arc::clone(&aligned_buf), 4, 4 + data.len() as u32);

        // Verify `Deref` trait
        assert_eq!(data.len(), slice_buf.len());

        // Verify `From` trait
        let greeting = std::str::from_utf8((&slice_buf).into())?;
        assert_eq!(data, greeting);
        Ok(())
    }
}

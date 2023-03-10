use std::{ops::Deref, sync::Arc};

use super::AlignedBuf;

pub struct BufSlice {
    buf: Arc<AlignedBuf>,
    pos: u32,
    limit: u32,
}

impl BufSlice {
    pub(crate) fn new(buf: Arc<AlignedBuf>, pos: u32, limit: u32) -> Self {
        debug_assert!(pos <= limit);
        debug_assert!(limit <= buf.capacity as u32);
        Self { buf, pos, limit }
    }
}

impl Deref for BufSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let base = unsafe { self.buf.as_ptr().offset(self.pos as isize) };
        unsafe { std::slice::from_raw_parts(base, (self.limit - self.pos) as usize) }
    }
}


#[cfg(test)]
mod tests {
    use std::error::Error;

    #[test]
    fn test_buf_slice() -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
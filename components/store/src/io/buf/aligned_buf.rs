use slog::{debug, Logger};
use std::{
    alloc::{self, Layout},
    ops::{Bound, RangeBounds},
    ptr, slice,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::error::StoreError;

/// Memory buffer complying given memory alignment, which is supposed to be used for DirectIO.
///
/// This struct is designed to be NOT `Copy` nor `Clone`; otherwise, we will have double-free issue.
#[derive(Debug)]
pub(crate) struct AlignedBuf {
    log: Logger,

    /// WAL offset
    pub(crate) offset: u64,

    /// Pointer to the allocated memory
    /// TODO: use std::mem::NonNull
    ptr: *mut u8,
    layout: Layout,
    pub(crate) capacity: usize,

    /// Write index
    pub(crate) written: AtomicUsize,
}

impl AlignedBuf {
    pub(crate) fn new(
        log: Logger,
        offset: u64,
        len: usize,
        alignment: usize,
    ) -> Result<Self, StoreError> {
        let capacity = (len + alignment - 1) / alignment * alignment;
        let layout = Layout::from_size_align(capacity, alignment)
            .map_err(|_e| StoreError::MemoryAlignment)?;
        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        Ok(Self {
            log,
            offset,
            ptr,
            layout,
            capacity,
            written: AtomicUsize::new(0),
        })
    }

    /// Judge if this buffer covers specified data region in WAL.
    ///
    /// #Arguments
    /// * `offset` - Offset in WAL
    /// * `len` - Length of the data.
    ///
    /// # Returns
    /// `true` if the cache hit; `false` otherwise.
    pub(crate) fn covers(&self, offset: u64, len: u32) -> bool {
        self.offset <= offset && offset + len as u64 <= self.offset + self.write_pos() as u64
    }

    pub(crate) fn write_pos(&self) -> usize {
        self.written.load(Ordering::Relaxed)
    }

    pub(crate) fn write_u32(&self, value: u32) -> bool {
        if self.written.load(Ordering::Relaxed) + 4 > self.capacity {
            return false;
        }
        let big_endian = value.to_be();
        let data = unsafe { slice::from_raw_parts(ptr::addr_of!(big_endian) as *const u8, 4) };
        self.write_buf(&data[..])
    }

    /// Get u32 in big-endian byte order.
    pub(crate) fn read_u32(&self, pos: usize) -> Result<u32, StoreError> {
        debug_assert!(self.written.load(Ordering::Relaxed) >= pos);
        if self.written.load(Ordering::Relaxed) - pos < std::mem::size_of::<u32>() {
            return Err(StoreError::InsufficientData);
        }
        let value = unsafe { *(self.ptr.offset(pos as isize) as *const u32) };
        Ok(u32::from_be(value))
    }

    pub(crate) fn write_u64(&self, value: u64) -> bool {
        if self.written.load(Ordering::Relaxed) + 8 > self.capacity {
            return false;
        }
        let big_endian = value.to_be();
        let data = unsafe { slice::from_raw_parts(ptr::addr_of!(big_endian) as *const u8, 8) };
        self.write_buf(&data[..])
    }

    pub(crate) fn read_u64(&self, pos: usize) -> Result<u64, StoreError> {
        debug_assert!(self.written.load(Ordering::Relaxed) > pos);
        if pos + 8 > self.written.load(Ordering::Relaxed) {
            return Err(StoreError::InsufficientData);
        }

        let value = unsafe { *(self.ptr.offset(pos as isize) as *const u64) };
        Ok(u64::from_be(value))
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    pub(crate) fn slice<R>(&self, range: R) -> &[u8]
    where
        R: RangeBounds<usize>,
    {
        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("out of bound"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&m) => m.checked_add(1).expect("out of bound"),
            Bound::Excluded(&m) => m,
            Bound::Unbounded => self.written.load(Ordering::Relaxed),
        };
        let len = end - start;

        unsafe { slice::from_raw_parts(self.ptr.offset(start as isize) as *const u8, len) }
    }

    pub(crate) fn write_buf(&self, buf: &[u8]) -> bool {
        let pos = self.written.load(Ordering::Relaxed);
        if pos + buf.len() > self.capacity {
            return false;
        }
        unsafe { ptr::copy_nonoverlapping(buf.as_ptr(), self.ptr.offset(pos as isize), buf.len()) };
        self.written.fetch_add(buf.len(), Ordering::Relaxed);
        true
    }

    /// Remaining space to write.
    pub(crate) fn remaining(&self) -> usize {
        let pos = self.written.load(Ordering::Relaxed);
        debug_assert!(pos <= self.capacity);
        self.capacity - pos
    }

    pub(crate) fn partial(&self) -> bool {
        self.write_pos() > 0 && self.write_pos() < self.capacity
    }
}

/// Return the memory back to allocator.
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.ptr, self.layout) };
        debug!(
            self.log,
            "Deallocated `AlignedBuf`: (offset={}, written: {}, capacity: {})",
            self.offset,
            self.write_pos(),
            self.capacity
        );
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, mem::size_of};

    use super::AlignedBuf;

    #[test]
    fn test_aligned_buf() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let alignment = 4096;
        let buf = AlignedBuf::new(log.clone(), 0, 128, alignment)?;
        assert_eq!(alignment, buf.remaining());
        let v = 1;
        buf.write_u32(1);
        assert_eq!(buf.remaining(), 4096 - size_of::<u32>());

        let value = buf.read_u32(0)?;
        assert_eq!(v, value);

        let v = 42;
        buf.write_u64(v);

        assert_eq!(v, buf.read_u64(4)?);

        let msg = "hello world";
        buf.write_buf(msg.as_bytes());
        assert_eq!(buf.remaining(), 4096 - 4 - 8 - msg.as_bytes().len());

        let payload = std::str::from_utf8(buf.slice(12..))?;
        assert_eq!(payload, msg);
        Ok(())
    }
}

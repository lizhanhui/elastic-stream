use std::{
    alloc::{self, Layout},
    collections::VecDeque,
};

use crate::error::StoreError;

/// Memory buffer complying given memory alignment, which is supposed to be used for DirectIO.
///
/// This struct is designed to be NOT `Copy` nor `Clone`; otherwise, we will have double-free issue.
pub(crate) struct AlignedBuf {
    pub(crate) ptr: *mut u8,
    len: usize,
    layout: Layout,
}

impl AlignedBuf {
    fn new(len: usize, alignment: usize) -> Result<Self, StoreError> {
        let layout =
            Layout::from_size_align(len, alignment).map_err(|_e| StoreError::MemoryAlignment)?;
        let ptr = unsafe { alloc::alloc(layout) };
        Ok(Self { ptr, len, layout })
    }
}

unsafe impl Send for AlignedBuf {}

/// Return the memory back to allocator.
impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.ptr, self.layout) };
    }
}

/// Buffer for `Record` read/write in DirectIO.
pub(crate) struct RecordBuf {
    iov: VecDeque<AlignedBuf>,
}

impl RecordBuf {
    pub(crate) fn new() -> Self {
        Self {
            iov: VecDeque::new(),
        }
    }

    /// Allocate a new buffer for write.
    pub(crate) fn alloc(&mut self, len: usize, alignment: usize) -> Result<(), StoreError> {
        let buf = AlignedBuf::new(len, alignment)?;
        self.iov.push_back(buf);
        Ok(())
    }

    /// Return the pointer to the buffer that is ready for write.
    pub(crate) fn write_buf(&self) -> Option<&AlignedBuf> {
        if let Some(buf) = self.iov.back() {
            Some(buf)
        } else {
            None
        }
    }

    /// Return pointer to the first buffer that is ready for read.
    pub(crate) fn read_buf(&self) -> Option<&AlignedBuf> {
        if let Some(buf) = self.iov.front() {
            Some(buf)
        } else {
            None
        }
    }

    /// Discard the internal buffer that has been read.
    ///
    /// Memory backing the internal buffer will automatically be deallocated.
    pub(crate) fn discard_buf_read(&mut self) {
        self.iov.pop_front();
    }
}

use std::{
    alloc::{self, Layout},
    collections::VecDeque,
};

use bytes::Bytes;

use crate::error::StoreError;

/// Memory buffer complying given memory alignment, which is supposed to be used for DirectIO.
///
/// This struct is designed to be NOT `Copy` nor `Clone`; otherwise, we will have double-free issue.
pub(crate) struct AlignedBuf {
    pub(crate) ptr: *mut u8,
    layout: Layout,
    capacity: usize,
    write_index: usize,
    read_index: usize,
}

impl AlignedBuf {
    fn new(len: usize, alignment: usize) -> Result<Self, StoreError> {
        let capacity = (len + alignment - 1) / alignment * alignment;
        let layout = Layout::from_size_align(capacity, alignment)
            .map_err(|_e| StoreError::MemoryAlignment)?;
        let ptr = unsafe { alloc::alloc(layout) };
        Ok(Self {
            ptr,
            layout,
            capacity,
            write_index: 0,
            read_index: 0,
        })
    }

    pub(crate) fn write_u32(&mut self, value: u32) -> bool {
        if self.write_index + 4 > self.capacity {
            return false;
        }

        let src = std::ptr::addr_of!(value) as *const u8;

        unsafe {
            std::ptr::copy_nonoverlapping(src, self.ptr.offset(self.write_index as isize), 4)
        };
        self.write_index += 4;
        true
    }

    /// Get u32 in big-endian byte order.
    pub(crate) fn read_u32(&mut self) -> Result<u32, StoreError> {
        debug_assert!(self.write_index >= self.read_index);
        if self.write_index - self.read_index < std::mem::size_of::<u32>() {
            return Err(StoreError::InsufficientData);
        }
        let value = unsafe { *(self.ptr.offset(self.read_index as isize) as *const u32) };
        self.read_index += std::mem::size_of::<u32>();
        Ok(value)
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        debug_assert!(self.read_index < self.capacity);
        unsafe { self.ptr.offset(self.read_index as isize) }
    }

    pub(crate) fn write_buf(&mut self, buf: &[u8]) -> bool {
        if self.write_index + buf.len() > self.capacity {
            return false;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                self.ptr.offset(self.write_index as isize),
                buf.len(),
            )
        };
        self.write_index += buf.len();
        true
    }

    /// Remaining space to write.
    pub(crate) fn remaining(&self) -> usize {
        debug_assert!(self.write_index <= self.capacity);
        self.capacity - self.write_index
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

#[cfg(test)]
mod tests {
    use std::{alloc::Layout, error::Error, ffi::CStr, mem::size_of};

    use bytes::{BufMut, BytesMut};

    use crate::error::StoreError;

    use super::AlignedBuf;

    /// Play with Rust ptr.
    #[test]
    fn test_layout() -> Result<(), Box<dyn Error>> {
        let alignment = 4096;
        let len = 512;
        let capacity = (len + alignment - 1) / alignment * alignment;
        let layout = Layout::from_size_align(capacity, alignment)?;

        assert_eq!(alignment, layout.align());
        assert_eq!(alignment, capacity);

        let ptr = unsafe { std::alloc::alloc(layout) };
        let mut buf = BytesMut::with_capacity(alignment);
        buf.resize(alignment - 1, 65);
        buf.put_i8(0);
        unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr, alignment) };
        let s = unsafe { CStr::from_ptr(ptr as *mut i8) };
        assert_eq!(alignment - 1, s.to_str()?.len());
        unsafe { std::alloc::dealloc(ptr, layout) };
        Ok(())
    }

    #[test]
    fn test_ptr() {
        let x = 42;
        let src = std::ptr::addr_of!(x);
        assert_eq!(x, unsafe { *src });

        let src = &x as *const i32;
        assert_eq!(x, unsafe { *src });
    }

    #[test]
    fn test_aligned_buf() -> Result<(), StoreError> {
        let alignment = 4096;
        let mut buf = AlignedBuf::new(128, alignment)?;
        assert_eq!(alignment, buf.remaining());
        let v = 1;
        buf.write_u32(1);
        assert_eq!(buf.remaining(), 4096 - size_of::<u32>());

        let msg = "hello world";
        buf.write_buf(msg.as_bytes());
        assert_eq!(buf.remaining(), 4096 - 4 - msg.as_bytes().len());

        let value = buf.read_u32()?;
        assert_eq!(v, value);

        Ok(())
    }
}

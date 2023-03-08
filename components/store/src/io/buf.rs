use std::{
    alloc::{self, Layout},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use slog::{debug, trace, Logger};

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

    /// Read index
    read: usize,
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
        let ptr = unsafe { alloc::alloc(layout) };
        Ok(Self {
            log,
            offset,
            ptr,
            layout,
            capacity,
            written: AtomicUsize::new(0),
            read: 0,
        })
    }

    pub(crate) fn write_pos(&self) -> usize {
        self.written.load(Ordering::Relaxed)
    }

    pub(crate) fn write_u32(&mut self, value: u32) -> bool {
        if self.written.load(Ordering::Relaxed) + 4 > self.capacity {
            return false;
        }

        let data = unsafe { std::slice::from_raw_parts(std::ptr::addr_of!(value) as *const u8, 4) };
        self.write_buf(&data[..])
    }

    /// Get u32 in big-endian byte order.
    pub(crate) fn read_u32(&mut self) -> Result<u32, StoreError> {
        debug_assert!(self.written.load(Ordering::Relaxed) >= self.read);
        if self.written.load(Ordering::Relaxed) - self.read < std::mem::size_of::<u32>() {
            return Err(StoreError::InsufficientData);
        }
        let value = unsafe { *(self.ptr.offset(self.read as isize) as *const u32) };
        self.read += std::mem::size_of::<u32>();
        Ok(value)
    }

    pub(crate) fn write_u64(&mut self, value: u64) -> bool {
        if self.written.load(Ordering::Relaxed) + 8 > self.capacity {
            return false;
        }

        let data = unsafe { std::slice::from_raw_parts(std::ptr::addr_of!(value) as *const u8, 8) };
        self.write_buf(&data[..])
    }

    pub(crate) fn read_u64(&mut self) -> Result<u64, StoreError> {
        debug_assert!(self.written.load(Ordering::Relaxed) > self.read);
        if self.read + 8 > self.written.load(Ordering::Relaxed) {
            return Err(StoreError::InsufficientData);
        }

        let value = unsafe { *(self.ptr.offset(self.read as isize) as *const u64) };
        self.read += 8;
        Ok(value)
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        debug_assert!(self.read < self.capacity);
        unsafe { self.ptr.offset(self.read as isize) }
    }

    pub(crate) fn write_buf(&self, buf: &[u8]) -> bool {
        let pos = self.written.load(Ordering::Relaxed);
        if pos + buf.len() > self.capacity {
            return false;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(buf.as_ptr(), self.ptr.offset(pos as isize), buf.len())
        };
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

pub(crate) struct AlignedBufWriter {
    log: Logger,
    pub(crate) offset: u64,
    alignment: usize,
    buffers: Vec<Arc<AlignedBuf>>,
}

impl AlignedBufWriter {
    pub(crate) fn new(log: Logger, offset: u64, alignment: usize) -> Self {
        Self {
            log,
            offset,
            alignment,
            buffers: vec![],
        }
    }

    /// Reset offset
    pub(crate) fn offset(&mut self, offset: u64) {
        self.offset = offset;
    }

    /// Must invoke this method to reserve enough memory before writing.
    pub(crate) fn reserve(&mut self, additional: usize) -> Result<(), StoreError> {
        trace!(
            self.log,
            "Try to reserve additional {} bytes for WAL data",
            additional
        );

        // First, allocate memory in alignment blocks, which we enough data to fill and then generate SQEs to submit
        // immediately.
        if additional > self.alignment {
            let size = additional / self.alignment * self.alignment;
            let buf = AlignedBuf::new(self.log.clone(), self.offset, size, self.alignment)?;
            trace!(
                self.log,
                "Reserved {} bytes for WAL data in complete blocks. [{}, {})",
                buf.capacity,
                self.offset,
                self.offset + buf.capacity as u64
            );
            self.offset += buf.capacity as u64;
            self.buffers.push(Arc::new(buf));
        }

        // Reserve memory block, for which we only partial data to fill.
        //
        // These partial data may be merged with future write tasks. Alternatively, we may issue stall-incurring writes if
        // configured amount of time has elapsed before collecting enough data.
        let r = additional % self.alignment;
        if 0 != r {
            let buf = AlignedBuf::new(
                self.log.clone(),
                self.offset,
                self.alignment,
                self.alignment,
            )?;
            trace!(
                self.log,
                "Reserved {} bytes for WAL data that may only fill partial of a block. [{}, {})",
                buf.capacity,
                self.offset,
                self.offset + buf.capacity as u64
            );
            self.offset += buf.capacity as u64;
            self.buffers.push(Arc::new(buf));
        }

        Ok(())
    }

    /// Assume enough aligned memory has been reserved.
    ///
    pub(crate) fn write(&mut self, data: &[u8]) -> Result<(), StoreError> {
        let remaining = data.len();
        let mut pos = 0;

        self.buffers
            .iter_mut()
            .skip_while(|buf| 0 == buf.remaining())
            .map_while(|buf| {
                let r = buf.remaining();
                if r >= remaining - pos {
                    buf.write_buf(&data[pos..]);
                    None
                } else {
                    buf.write_buf(&data[pos..pos + r]);
                    pos += r;
                    Some(())
                }
            })
            .count();

        Ok(())
    }

    pub(crate) fn write_u32(&mut self, value: u32) -> Result<(), StoreError> {
        let data = std::ptr::addr_of!(value);
        let slice =
            unsafe { std::slice::from_raw_parts(data as *const u8, std::mem::size_of::<u32>()) };
        self.write(slice)
    }

    pub(crate) fn write_u64(&mut self, value: u64) -> Result<(), StoreError> {
        let data = std::ptr::addr_of!(value);
        let slice =
            unsafe { std::slice::from_raw_parts(data as *const u8, std::mem::size_of::<u64>()) };
        self.write(slice)
    }

    /// Take backing buffers and generate submission queue entry for each of buf.
    ///
    /// If the backing buffer is full, it will be drained;
    /// If it is partially filled, its `Arc` reference will be cloned.
    pub(crate) fn take(&mut self) -> Vec<Arc<AlignedBuf>> {
        let mut items: Vec<_> = self
            .buffers
            .drain_filter(|buf| 0 == buf.remaining())
            .collect();

        self.buffers
            .iter()
            .filter(|buf| buf.partial())
            .map(|buf| Arc::clone(buf))
            .for_each(|buf| {
                items.push(buf);
            });

        items.iter().for_each(|item| {
            trace!(
                self.log,
                "About to flush aligned buffer{{ offset: {}, written: {}, capacity: {} }} to flush",
                item.offset,
                item.write_pos(),
                item.capacity
            );
        });

        items
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buffers.iter().map(|buf| buf.remaining()).sum()
    }
}

pub(crate) struct AlignedBufReader {
    log: Logger,
}

impl AlignedBufReader {
    pub(crate) fn alloc_read_buf(
        log: Logger,
        offset: u64,
        len: usize,
        alignment: u64,
    ) -> Result<AlignedBuf, StoreError> {
        // Alignment must be positive
        debug_assert_ne!(0, alignment);
        // Alignment must be power of 2.
        debug_assert_eq!(0, alignment & (alignment - 1));
        let from = offset / alignment * alignment;
        let to = (offset + len as u64 + alignment - 1) / alignment * alignment;
        AlignedBuf::new(log, from, (to - from) as usize, alignment as usize)
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
        let log = util::terminal_logger();
        let alignment = 4096;
        let mut buf = AlignedBuf::new(log.clone(), 0, 128, alignment)?;
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

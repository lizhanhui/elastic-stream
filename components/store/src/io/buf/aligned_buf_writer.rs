use super::AlignedBuf;
use crate::error::StoreError;
use slog::{trace, Logger};
use std::{ptr, slice, sync::Arc};

pub(crate) struct AlignedBufWriter {
    log: Logger,

    /// Write cursor in the WAL space.
    ///
    /// # Note
    ///
    /// `cursor` points to position where next write should go. It is not necessarily aligned.
    /// After each write, it should be updated.
    pub(crate) cursor: u64,

    alignment: usize,
    buffers: Vec<Arc<AlignedBuf>>,
}

impl AlignedBufWriter {
    pub(crate) fn new(log: Logger, cursor: u64, alignment: usize) -> Self {
        Self {
            log,
            cursor,
            alignment,
            buffers: vec![],
        }
    }

    /// Reset writer cursor after recovery procedure
    pub(crate) fn reset_cursor(&mut self, cursor: u64) {
        self.cursor = cursor;
    }

    /// Rebase buffer during recovery and prior to writing new records.
    ///
    /// # Note
    /// The last page of WAL may be partially committed on restart. As a result,
    /// we need to reuse the buffer when appending new records.
    ///
    /// Must reset writer cursor prior to rebasing last aligned buffer.
    pub(crate) fn rebase_buf(&mut self, buf: Arc<AlignedBuf>) {
        debug_assert!(
            self.buffers.is_empty(),
            "BufWriter should have not allocated any buffers"
        );
        debug_assert_eq!(self.cursor, buf.wal_offset + buf.limit() as u64);
        self.buffers.push(buf);
    }

    /// Must invoke this method to reserve enough memory before writing.
    pub(crate) fn reserve(&mut self, additional: usize) -> Result<(), StoreError> {
        trace!(
            self.log,
            "Try to reserve additional {} bytes for WAL data",
            additional
        );

        // Calculate WAL offset where new allocated aligned buffer should map to
        let mut offset = self
            .buffers
            .last()
            .map(|buf| buf.wal_offset + buf.capacity as u64)
            .unwrap_or_else(|| {
                debug_assert_eq!(0, self.cursor % self.alignment as u64);
                self.cursor
            });

        // First, allocate memory in alignment blocks, which we enough data to fill and then generate SQEs to submit
        // immediately.
        if additional >= self.alignment {
            let size = additional / self.alignment * self.alignment;
            let buf = AlignedBuf::new(self.log.clone(), offset, size, self.alignment)?;
            trace!(
                self.log,
                "Reserved {} bytes for WAL data in complete blocks. [{}, {})",
                buf.capacity,
                offset,
                offset + buf.capacity as u64
            );
            offset += buf.capacity as u64;
            self.buffers.push(Arc::new(buf));
        }

        // Reserve memory block, for which we only partial data to fill.
        //
        // These partial data may be merged with future write tasks. Alternatively, we may issue stall-incurring writes if
        // configured amount of time has elapsed before collecting enough data.
        let r = additional % self.alignment;
        if 0 != r {
            let buf = AlignedBuf::new(self.log.clone(), offset, self.alignment, self.alignment)?;
            trace!(
                self.log,
                "Reserved {} bytes for WAL data that may only fill partial of a block. [{}, {})",
                buf.capacity,
                offset,
                offset + buf.capacity as u64
            );
            offset += buf.capacity as u64;
            self.buffers.push(Arc::new(buf));
        }

        Ok(())
    }

    /// Assume enough aligned memory has already been reserved.
    pub(crate) fn write(&mut self, data: &[u8]) -> Result<(), StoreError> {
        let remaining = data.len();
        let mut pos = 0;

        self.buffers
            .iter_mut()
            .skip_while(|buf| 0 == buf.remaining())
            .map_while(|buf| {
                let r = buf.remaining();
                if r >= remaining - pos {
                    buf.write_buf(self.cursor + pos as u64, &data[pos..]);
                    pos += &data[pos..].len();
                    None
                } else {
                    buf.write_buf(self.cursor + pos as u64, &data[pos..pos + r]);
                    pos += r;
                    Some(())
                }
            })
            .count();
        debug_assert_eq!(pos, data.len());
        self.cursor += data.len() as u64;

        Ok(())
    }

    pub(crate) fn write_u32(&mut self, value: u32) -> Result<(), StoreError> {
        let big_endian = value.to_be();
        let data = ptr::addr_of!(big_endian);
        let slice = unsafe { slice::from_raw_parts(data as *const u8, std::mem::size_of::<u32>()) };
        self.write(slice)
    }

    pub(crate) fn write_u64(&mut self, value: u64) -> Result<(), StoreError> {
        let big_endian = value.to_be();
        let data = ptr::addr_of!(big_endian);
        let slice = unsafe { slice::from_raw_parts(data as *const u8, std::mem::size_of::<u64>()) };
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
            .map(Arc::clone)
            .for_each(|buf| {
                items.push(buf);
            });

        items.iter().for_each(|item| {
            trace!(self.log, "About to flush {} to disk", item);
        });

        items
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buffers.iter().map(|buf| buf.remaining()).sum()
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use crate::io::buf::AlignedBuf;

    const ALIGNMENT: usize = 512;

    #[test]
    fn test_aligned_buf_writer() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let mut buf_writer = super::AlignedBufWriter::new(log.clone(), 0, ALIGNMENT);
        assert_eq!(0, buf_writer.cursor);
        buf_writer.reset_cursor(4100);
        let aligned_buf = AlignedBuf::new(log.clone(), 4096, 4096, ALIGNMENT)?;
        aligned_buf.write_u32(4096, 100);
        let aligned_buf = Arc::new(aligned_buf);
        buf_writer.rebase_buf(aligned_buf);

        buf_writer.reserve(1024)?;
        assert_eq!(4100, buf_writer.cursor);

        buf_writer.write_u32(101)?;
        assert_eq!(4104, buf_writer.cursor);
        Ok(())
    }

    #[test]
    fn test_reserve() -> Result<(), Box<dyn Error>> {
        let log = test_util::terminal_logger();
        let mut buf_writer = super::AlignedBufWriter::new(log.clone(), 0, ALIGNMENT);
        assert_eq!(0, buf_writer.cursor);
        buf_writer.reserve(ALIGNMENT)?;
        assert_eq!(buf_writer.remaining(), ALIGNMENT);

        buf_writer.reserve(ALIGNMENT + 1)?;
        assert_eq!(buf_writer.remaining(), ALIGNMENT * 3);

        buf_writer.reserve(ALIGNMENT - 1)?;
        assert_eq!(buf_writer.remaining(), ALIGNMENT * 4);

        Ok(())
    }
}

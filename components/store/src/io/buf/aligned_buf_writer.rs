use super::AlignedBuf;
use crate::error::StoreError;
use slog::{trace, Logger};
use std::{ptr, slice, sync::Arc};

pub(crate) struct AlignedBufWriter {
    log: Logger,
    /// It's a aligned wal offset, which is pointing to a absolute address in the wal segment.
    pub(crate) wal_offset: u64,
    alignment: usize,
    buffers: Vec<Arc<AlignedBuf>>,
}

impl AlignedBufWriter {
    pub(crate) fn new(log: Logger, wal_offset: u64, alignment: usize) -> Self {
        Self {
            log,
            wal_offset,
            alignment,
            buffers: vec![],
        }
    }

    /// Reset offset
    pub(crate) fn wal_offset(&mut self, offset: u64) {
        self.wal_offset = offset;
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
            let buf = AlignedBuf::new(self.log.clone(), self.wal_offset, size, self.alignment)?;
            trace!(
                self.log,
                "Reserved {} bytes for WAL data in complete blocks. [{}, {})",
                buf.capacity,
                self.wal_offset,
                self.wal_offset + buf.capacity as u64
            );
            self.wal_offset += buf.capacity as u64;
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
                self.wal_offset,
                self.alignment,
                self.alignment,
            )?;
            trace!(
                self.log,
                "Reserved {} bytes for WAL data that may only fill partial of a block. [{}, {})",
                buf.capacity,
                self.wal_offset,
                self.wal_offset + buf.capacity as u64
            );
            self.wal_offset += buf.capacity as u64;
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
            .map(|buf| Arc::clone(buf))
            .for_each(|buf| {
                items.push(buf);
            });

        items.iter().for_each(|item| {
            trace!(
                self.log,
                "About to flush aligned buffer{{ offset: {}, written: {}, capacity: {} }} to flush",
                item.wal_offset,
                item.limit(),
                item.capacity
            );
        });

        items
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buffers.iter().map(|buf| buf.remaining()).sum()
    }
}

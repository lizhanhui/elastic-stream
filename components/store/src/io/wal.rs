use std::{
    cmp,
    collections::{HashMap, VecDeque},
    io,
    iter::successors,
    sync::Arc,
};

use crate::{
    error::StoreError,
    index::{
        driver::IndexDriver,
        record_handle::{HandleExt, RecordHandle},
    },
    io::record::RecordType,
    io::segment::{LogSegment, Medium, SegmentDescriptor, Status},
};

use io_uring::{opcode, squeue, types};
use log::{debug, error, info, trace, warn};
use model::payload::Payload;
use percentage::Percentage;

/// A WalCache holds the configurations of cache management, and supports count the usage of the memory.
pub(crate) struct WalCache {
    /// The maximum size of the cache,
    /// the reclaim mechanism will ensure that the cache size is less than this value.
    max_cache_size: u64,

    /// The current size of the cache.
    /// The cache size is the sum of the size of all the segment block caches.
    current_cache_size: u64,

    /// When the cache size is greater than the high water mark, a reclaim operation will be triggered.
    high_watermark: usize,
}

/// A WAL contains a list of log segments, and supports open, close, alloc, and other operations.
pub(crate) struct Wal {
    /// I/O Uring instance for write-ahead-log segment file management.
    ///
    /// Unlike `data_uring`, this instance is used to open/fallocate/close/delete log segment files because these opcodes are not
    /// properly supported by the instance armed with the `IOPOLL` feature.
    control_ring: io_uring::IoUring,

    /// Global configuration
    config: Arc<config::Configuration>,

    /// Mapping of on-going file operations between segment offset to file operation `Status`.
    ///
    /// File `Status` migration road-map: OpenAt --> Fallocate --> ReadWrite -> Read -> Close -> Unlink.
    /// Once the status of segment is driven to ReadWrite,
    /// this mapping should be removed.
    inflight_control_tasks: HashMap<u64, Status>,

    /// The container of the log segments.
    segments: VecDeque<LogSegment>,

    /// The cache management of the WAL.
    wal_cache: WalCache,
}

impl Wal {
    pub(crate) fn new(
        control_ring: io_uring::IoUring,
        config: &Arc<config::Configuration>,
    ) -> Self {
        Self {
            control_ring,
            config: Arc::clone(config),
            segments: VecDeque::new(),
            inflight_control_tasks: HashMap::new(),
            wal_cache: WalCache {
                max_cache_size: config.store.max_cache_size,
                current_cache_size: 0,
                high_watermark: config.store.cache_high_watermark,
            },
        }
    }

    pub(crate) fn inflight_control_task_num(&self) -> usize {
        self.inflight_control_tasks.len()
    }

    /// All the segments will be opened after loading from WAL.
    pub(crate) fn load_from_paths(&mut self) -> Result<(), StoreError> {
        let wal_path = self.config.store.path.wal_path();
        let mut segment_files: Vec<_> = wal_path
            .read_dir()?
            .flatten() // Note Result implements FromIterator trait, so `flatten` applies and potential `Err` will be propagated.
            .flat_map(|entry| {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.file_type().is_dir() {
                        warn!("Skip {:?} as it is a directory", entry.path());
                        None
                    } else {
                        let path = entry.path();
                        let path = path.as_path();
                        if let Some(offset) = LogSegment::parse_offset(path) {
                            let log_segment_file = LogSegment::new(
                                &self.config,
                                offset,
                                self.config.store.segment_size,
                                path,
                            );
                            Some(log_segment_file)
                        } else {
                            error!("Failed to parse offset from file name: {:?}", entry.path());
                            None
                        }
                    }
                } else {
                    None
                }
            })
            .flatten()
            .collect();

        // Sort log segment file by file name.
        segment_files.sort();

        // Preallocate segment files until they reach the maximum number specified in the configuration file.
        let next_wal_offset = segment_files
            .last()
            .map(|segment| segment.wal_offset + segment.size)
            .unwrap_or(0);

        let iter = successors(Some(next_wal_offset), |&offset| {
            Some(offset + self.config.store.segment_size)
        })
        .take_while(|&offset| {
            offset + self.config.store.segment_size <= self.config.store.total_segment_file_size
        });

        for offset in iter {
            let log_segment_file = LogSegment::new(
                &self.config,
                offset,
                self.config.store.segment_size,
                wal_path.join(LogSegment::format(offset)).as_path(),
            );
            if let Ok(log_segment_file) = log_segment_file {
                segment_files.push(log_segment_file);
            }
        }

        for mut segment_file in segment_files.into_iter() {
            segment_file.open()?;
            self.segments.push_back(segment_file);
        }

        Ok(())
    }

    pub(crate) fn segment_file_of(&mut self, offset: u64) -> Option<&mut LogSegment> {
        self.segments.iter_mut().rev().find(|segment| {
            segment.wal_offset <= offset && (segment.wal_offset + segment.size > offset)
        })
    }

    /// Return whether has reached end of the WAL
    fn scan_record(
        segment: &mut LogSegment,
        pos: &mut u64,
        indexer: &Arc<IndexDriver>,
    ) -> Result<bool, StoreError> {
        // Ensure `pos` falls into WAL data range covered by `segment`.
        debug_assert!(*pos >= segment.wal_offset, "Invalid WAL offset");
        debug_assert!(
            *pos < segment.wal_offset + segment.size,
            "Invalid WAL offset"
        );

        let mut file_pos = *pos - segment.wal_offset;
        let mut meta_buf = [0; 4];

        let mut buf = bytes::BytesMut::new();
        let mut last_found = false;
        // Find the last continuous record
        loop {
            segment.read_exact_at(&mut meta_buf, file_pos)?;
            file_pos += 4;
            let crc = u32::from_be_bytes(meta_buf);

            segment.read_exact_at(&mut meta_buf, file_pos)?;
            file_pos += 4;
            let len_type = u32::from_be_bytes(meta_buf);
            let len = (len_type >> 8) as usize;

            // Verify the parsed `len` makes sense.
            if file_pos + len as u64 > segment.size || 0 == len {
                info!("Got an invalid record length: `{}`. Stop scanning WAL", len);
                last_found = true;
                segment.status = Status::ReadWrite;
                file_pos -= 8;
                segment.written = file_pos;
                break;
            }

            buf.resize(len, 0);
            segment.read_exact_at(buf.as_mut(), file_pos)?;

            let ckm = LogSegment::checksum_record([&buf], segment.wal_offset);

            if ckm != crc {
                file_pos -= 8;
                segment.written = file_pos;
                segment.status = Status::ReadWrite;
                info!(
                    "Found a record failing CRC32c. Expecting: `{:#08x}`, Actual: `{:#08x}`",
                    crc, ckm
                );
                last_found = true;
                break;
            }

            // Advance the file position
            file_pos += len as u64;

            let record_type = (len_type & 0xFF) as u8;
            if let Ok(t) = RecordType::try_from(record_type) {
                if let RecordType::Zero = t {
                    debug_assert_eq!(segment.size, file_pos, "Should have reached EOF");

                    segment.written = segment.size;
                    segment.status = Status::Read;
                    info!("Reached EOF of {}", segment);

                    // Break if the scan operation reaches the end of file
                    break;
                }
            } else {
                // Panic if the record type is unknown.
                // Panic here is safe since it's in the recovery stage.
                panic!("Unknown record type: {}", record_type);
            }

            // Index the record batch
            match Payload::parse_append_entry(&buf) {
                Ok((Some(entry), len)) => {
                    let stream_id = entry.stream_id as i64;
                    let range = entry.index;
                    let offset = entry.offset.expect("base-offset should have been assigned");
                    let handle = RecordHandle {
                        wal_offset: segment.wal_offset + file_pos - len as u64 - 8,
                        len: len as u32 + 8,
                        ext: HandleExt::BatchSize(entry.len),
                    };
                    trace!("Index RecordBatch[stream-id={}, range={}, base-offset={}, wal-offset={}, len={}]",
                           stream_id, range, offset, handle.wal_offset, handle.len);
                    indexer.index(stream_id, range, offset, handle);
                }

                Ok((None, _)) => {
                    warn!("Buffer does not contain a valid AppendEntry. Skip indexing");
                }
                Err(e) => {
                    error!("Failed to deserialize RecordBatchMeta. Cause: {:?}", e);
                }
            }

            buf.resize(len, 0);
        }
        *pos = segment.wal_offset + file_pos;

        segment.truncate_to(segment.written)?;
        Ok(last_found)
    }

    pub(crate) fn recover(
        &mut self,
        offset: u64,
        indexer: Arc<IndexDriver>,
    ) -> Result<u64, StoreError> {
        let mut pos = offset;

        // Scan, at most, from the first segment
        if let Some(segment) = self.segments.front() {
            if pos < segment.wal_offset {
                warn!(
                    "WAL checkpoint {pos} is less than beginning WAL offset of the first segment file: {}",
                    segment.wal_offset
                );
                pos = segment.wal_offset;
            }
        }

        info!("Start to recover WAL segment files from {pos}");
        let mut need_scan = true;
        for segment in self.segments.iter_mut() {
            if segment.wal_offset + segment.size <= offset {
                segment.status = Status::Read;
                segment.written = segment.size;
                debug!("Mark {} as read-only", segment);
                continue;
            }

            if !need_scan {
                segment.written = 0;
                segment.status = Status::ReadWrite;
                debug!("Mark {} as read-write", segment);
                continue;
            }

            if Self::scan_record(segment, &mut pos, &indexer)? {
                need_scan = false;
                info!("Recovery completed at `{}`", pos);
            }
        }
        info!("Recovery of WAL segment files completed");

        Ok(pos)
    }

    /// Queue and submit a single SQE to the SQ of control io-uring.
    #[inline]
    fn enqueue_and_submit_entry(
        &mut self,
        entry: &squeue::Entry,
        entry_name: &str,
    ) -> Result<(), StoreError> {
        unsafe {
            self.control_ring.submission().push(entry).map_err(|e| {
                error!("Failed to push {} SQE to SQ: {}", entry_name, e.to_string());
                StoreError::IoUring
            })
        }?;
        let _ = self.control_ring.submit().map_err(|e| {
            error!(
                "Failed to submit {} SQE to SQ: {}",
                entry_name,
                e.to_string()
            );
            StoreError::IoUring
        })?;
        Ok(())
    }

    /// Enqueue and submit multiple SQEs to the SQ of control io-uring.
    #[inline]
    fn enqueue_and_submit_entries(&mut self, entries: &[squeue::Entry]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        unsafe {
            self.control_ring
                .submission()
                .push_multiple(entries)
                .map_err(|e| {
                    error!(
                        "Failed to push multiple submission queue entries: {}",
                        e.to_string()
                    );
                    StoreError::IoUring
                })
        }?;
        let _ = self.control_ring.submit().map_err(|e| {
            error!(
                "Failed to submit multiple submission queue entries: {}",
                e.to_string()
            );
            StoreError::IoUring
        })?;
        Ok(())
    }

    /// New a segment, and then open it in the uring driver.
    pub(crate) fn try_open_segment(&mut self) -> Result<(), StoreError> {
        let mut segment = self.alloc_segment()?;
        let offset = segment.wal_offset;
        debug_assert_eq!(segment.status, Status::OpenAt);
        info!("About to create/open LogSegmentFile: `{}`", segment);
        let has_recyclable_segment = self
            .segments
            .front()
            .filter(|segment| segment.status == Status::Recycled)
            .is_some();

        if self.config.store.reclaim_policy.is_recycle() && has_recyclable_segment {
            // Since there exists a recycled segment file, we can reuse it by renaming it.
            let recycled_segment = self.segments.pop_front().unwrap();
            segment.status = Status::RenameAt(recycled_segment.path.clone());
            let old_path = if let Status::RenameAt(ref s) = segment.status {
                s.as_ptr()
            } else {
                unreachable!()
            };
            self.inflight_control_tasks
                .insert(offset, segment.status.clone());
            let sqe = opcode::RenameAt::new(
                types::Fd(libc::AT_FDCWD),
                old_path,
                types::Fd(libc::AT_FDCWD),
                segment.path.as_ptr(),
            )
            .build()
            .user_data(offset);
            self.enqueue_and_submit_entry(&sqe, "RenameAt")?;
            self.segments.push_back(segment);
            Ok(())
        } else {
            let status = segment.status.clone();
            self.inflight_control_tasks.insert(offset, status);
            let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), segment.path.as_ptr())
                .flags(libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT)
                .mode(libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IWGRP)
                .build()
                .user_data(offset);
            self.enqueue_and_submit_entry(&sqe, "OpenAt")?;
            self.segments.push_back(segment);
            Ok(())
        }
    }

    pub(crate) fn check_expired_segment(&mut self) {
        let max_segment_file_num =
            self.config.store.total_segment_file_size / self.config.store.segment_size;
        let cur_segment_file_num = self
            .segments
            .iter()
            .filter(|segment| segment.status == Status::Read)
            .count() as u64;
        if cur_segment_file_num > max_segment_file_num {
            self.segments
                .iter_mut()
                .filter(|segment| segment.status == Status::Read)
                .take((cur_segment_file_num - max_segment_file_num) as usize)
                .for_each(|segment| segment.status = Status::Close);
        }
    }

    pub(crate) fn try_close_segment(&mut self) -> Result<(), StoreError> {
        // check the expired segment, and set their status to Status::Close
        self.check_expired_segment();
        let to_close: Vec<&LogSegment> = self
            .segments
            .iter()
            .take_while(|segment| segment.status == Status::Close)
            .filter(|segment| {
                !self
                    .inflight_control_tasks
                    .contains_key(&segment.wal_offset)
            })
            .collect();

        let mut entries = vec![];

        for segment in to_close {
            self.inflight_control_tasks
                .insert(segment.wal_offset, segment.status.clone());
            if let Some(sd) = segment.sd.as_ref() {
                let sqe = opcode::Close::new(types::Fd(sd.fd))
                    .build()
                    .user_data(segment.wal_offset);
                info!("About to close LogSegmentFile: {}", segment);
                entries.push(sqe);
            }
        }

        self.enqueue_and_submit_entries(&entries)?;
        Ok(())
    }

    /// New a segment, and then open it in the classic way, i.e. without uring.
    /// Notes: this is only used in tests to avoid the complexity of uring.
    #[cfg(test)]
    pub(crate) fn open_segment_directly(&mut self) -> Result<(), StoreError> {
        let mut segment = self.alloc_segment()?;
        segment.open()?;
        self.segments.push_back(segment);
        Ok(())
    }

    /// Try reclaim the cache entries from the segments.
    ///
    /// # Arguments
    /// min_bytes: the minimum free bytes after reclaiming.
    ///
    /// # Returns
    /// The reclaimed bytes and the current cache size.
    #[minitrace::trace]
    pub(crate) fn try_reclaim(&mut self, min_free_bytes: u32) -> (u64, u64) {
        // Calculate the current cache size of all the segments.
        let mut cache_size = 0u64;
        for segment in self.segments.iter() {
            cache_size += segment.block_cache.cache_size() as u64;
        }

        // Calculate the bytes to reclaim, based on the configured `high_watermark`.
        let high_percent = Percentage::from(self.wal_cache.high_watermark);
        let mut to_reclaim = 0;
        if cache_size > high_percent.apply_to(self.wal_cache.max_cache_size) {
            to_reclaim = cache_size - high_percent.apply_to(self.wal_cache.max_cache_size);
        }

        // If the available cache size is less than the `min_free_bytes` after reclaiming, increase the reclaim size.
        let current_free_bytes = self.wal_cache.max_cache_size as i64 - cache_size as i64;

        if current_free_bytes + (to_reclaim as i64) < min_free_bytes as i64 {
            to_reclaim = (min_free_bytes as i64 - current_free_bytes) as u64;
        }

        if to_reclaim == 0 {
            return (0, current_free_bytes as u64);
        }

        // Reclaim the cache entries from the segments.
        let mut reclaimed = 0u64;

        // build a skiplist to sort the segments by the lowest score of block cache.
        let mut score_list = unsafe {
            skiplist::OrderedSkipList::<(&mut LogSegment, u64)>::with_comp(|a, b| a.1.cmp(&b.1))
        };
        self.segments.iter_mut().for_each(|segment| {
            let score = segment.block_cache.min_score();
            if score > 0 {
                score_list.insert((segment, score));
            }
        });

        // Reclaim the cache entries from the segment with lowest score.
        loop {
            if reclaimed >= to_reclaim || score_list.is_empty() {
                break;
            }

            let (segment, _) = score_list.pop_front().unwrap();
            let last_wal_offset = segment.block_cache.wal_offset_of_last_cache_entry();
            let entry = segment.block_cache.remove_by_score();

            if let Some(entry) = entry {
                // The last writable entry should not be reclaimed.
                if segment.status == Status::ReadWrite {
                    // Skip the last writable entry.
                    if entry.wal_offset() == last_wal_offset {
                        continue;
                    }
                }

                reclaimed += entry.capacity() as u64;

                // put the segment into the score list if it still has cache entries.
                let score = segment.block_cache.min_score();
                if score > 0 {
                    score_list.insert((segment, score));
                }
            }
        }

        self.wal_cache.current_cache_size = cache_size - reclaimed;
        (
            reclaimed,
            cmp::max(0, current_free_bytes + reclaimed as i64) as u64,
        )
    }

    fn alloc_segment(&self) -> Result<LogSegment, StoreError> {
        let offset = if self.segments.is_empty() {
            0
        } else if let Some(last) = self.segments.back() {
            last.wal_offset + self.config.store.segment_size
        } else {
            unreachable!("Should-not-reach-here")
        };
        let path = self.config.store.path.wal_path();
        let path = path.join(LogSegment::format(offset));

        let segment = LogSegment::new(
            &self.config,
            offset,
            self.config.store.segment_size,
            path.as_path(),
        )?;

        Ok(segment)
    }

    pub(crate) fn writable_segment_count(&self) -> usize {
        self.segments
            .iter()
            .rev() // from back to front
            .take_while(|segment| segment.status != Status::Read)
            .count()
    }

    /// Delete segments when their backed file is deleted
    pub(crate) fn delete_segments(&mut self, offsets: Vec<u64>) {
        if offsets.is_empty() {
            return;
        }

        self.segments.retain(|segment| {
            !(segment.status == Status::UnlinkAt && offsets.contains(&segment.wal_offset))
        });
    }

    pub(crate) fn control_task_num(&self) -> usize {
        self.inflight_control_tasks.len()
    }

    #[minitrace::trace]
    pub(crate) fn await_control_task_completion(&self) {
        let now = std::time::Instant::now();
        loop {
            match self.control_ring.submit_and_wait(1) {
                Ok(_) => {
                    info!(
                        "Waiting {}us for control plane file system operation",
                        now.elapsed().as_micros()
                    );
                    break;
                }
                Err(e) => {
                    match e.kind() {
                        io::ErrorKind::Interrupted => {
                            // io_uring_enter just propagates underlying EINTR signal up to application, allowing to handle this signal.
                            // For our usage, we just ignore this signal and retry.
                            //
                            // See https://www.spinics.net/lists/io-uring/msg01823.html
                            // Lots of system calls return -EINTR if interrupted by a signal, don't
                            // think there's anything worth fixing there. For the wait part, the
                            // application may want to handle the signal before we can wait again.
                            // We can't go to sleep with a pending signal.
                            warn!("io_uring_enter got an error: {:?}", e);
                            continue;
                        }

                        io::ErrorKind::ResourceBusy => {
                            // If the IORING_FEAT_NODROP feature flag is set, then EBUSY will be returned
                            // if there were overflow entries, IORING_ENTER_GETEVENTS flag is set and not all of
                            // the overflow entries were able to be flushed to the CQ ring.
                            //
                            // See https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html
                            warn!("io_uring_enter got an error: {:?}", e);
                            continue;
                        }

                        _ => {
                            error!("io_uring_enter got an error: {:?}", e);
                            // Fatal errors, crash the process and let watchdog to restart.
                            panic!("io_uring_enter returns error {:?}", e);
                        }
                    }
                }
            }
        }
    }

    #[minitrace::trace]
    pub(crate) fn reap_control_tasks(&mut self) -> Result<(), StoreError> {
        // Map of segment offset to syscall result
        let mut m = HashMap::new();
        {
            let mut cq = self.control_ring.completion();
            loop {
                if cq.is_empty() {
                    break;
                }
                #[allow(clippy::while_let_on_iterator)]
                while let Some(cqe) = cq.next() {
                    m.insert(cqe.user_data(), cqe.result());
                }
                cq.sync();
            }
        }

        m.into_iter()
            .flat_map(|(offset, result)| {
                if self.inflight_control_tasks.remove(&offset).is_none() {
                    error!(
                        "`file_op` map should have a record for log segment with offset: {}",
                        offset
                    );
                    return Err(StoreError::Internal(
                        "file_op misses expected in-progress offset-status entry".to_owned(),
                    ));
                }
                self.on_file_op_completion(offset, result)
            })
            .count();

        Ok(())
    }

    fn on_file_op_completion(&mut self, offset: u64, result: i32) -> Result<(), StoreError> {
        let mut to_remove = vec![];
        let recycled = self.config.store.reclaim_policy.is_recycle();
        if let Some(segment) = self.segment_file_of(offset) {
            if -1 == result {
                error!("LogSegment file operation failed: {}", segment);
                return Err(StoreError::System(result));
            }
            match segment.status {
                Status::OpenAt => {
                    info!(
                        "LogSegmentFile: `{}` is created and open with FD: {}",
                        segment, result
                    );
                    segment.sd = Some(SegmentDescriptor {
                        medium: Medium::Ssd,
                        fd: result,
                        base_ptr: 0,
                    });
                    segment.status = Status::Fallocate64;

                    info!(
                        "About to fallocate LogSegmentFile: `{}` with FD: {}",
                        segment, result
                    );
                    let sqe = opcode::Fallocate::new(types::Fd(result), segment.size)
                        .offset(0)
                        .mode(libc::FALLOC_FL_ZERO_RANGE)
                        .build()
                        .user_data(offset);
                    unsafe {
                        self.control_ring.submission().push(&sqe).map_err(|e| {
                            error!("Failed to submit Fallocate SQE to io_uring SQ: {:?}", e);
                            StoreError::IoUring
                        })
                    }?;
                    self.inflight_control_tasks
                        .insert(offset, Status::Fallocate64);
                }

                Status::Fallocate64 => {
                    // Sync file metadata
                    info!("Fallocate of LogSegmentFile `{}` completed", segment);
                    segment.status = Status::Fsync;
                    info!("About to fsync LogSegmentFile: `{}`", segment);

                    let sqe = opcode::Fsync::new(types::Fd(segment.sd.as_ref().unwrap().fd))
                        .build()
                        .user_data(segment.wal_offset);
                    unsafe {
                        self.control_ring.submission().push(&sqe).map_err(|e| {
                            error!("Failed to submit Fsync SQE to io_uring SQ: {:?}", e);
                            StoreError::IoUring
                        })
                    }?;
                    self.inflight_control_tasks.insert(offset, Status::Fsync);
                }

                Status::Fsync => {
                    info!("Fsync of LogSegmentFile `{}` completed", segment);
                    segment.status = Status::ReadWrite;
                }

                Status::Close => {
                    info!("LogSegmentFile: `{}` is closed", segment);
                    if !recycled {
                        segment.sd = None;
                        segment.status = Status::UnlinkAt;
                        info!("About to delete LogSegmentFile `{}`", segment);
                        let sqe = opcode::UnlinkAt::new(
                            types::Fd(libc::AT_FDCWD),
                            segment.path.as_ptr() as *const libc::c_char,
                        )
                        .build()
                        .flags(squeue::Flags::empty())
                        .user_data(offset);
                        unsafe {
                            self.control_ring.submission().push(&sqe).map_err(|e| {
                                error!("Failed to push Unlink SQE to SQ: {:?}", e);
                                StoreError::IoUring
                            })
                        }?;
                        self.inflight_control_tasks.insert(offset, Status::UnlinkAt);
                    } else {
                        info!("LogSegmentFile `{}` become recycled", segment);
                        segment.sd = None;
                        segment.status = Status::Recycled;
                    }
                }
                Status::UnlinkAt => {
                    info!("LogSegmentFile: `{}` is deleted", segment);
                    to_remove.push(offset)
                }
                Status::RenameAt(..) => {
                    info!("LogSegmentFile: `{}` is renamed", segment);
                    segment.status = Status::OpenAt;
                    let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), segment.path.as_ptr())
                        .flags(libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT)
                        .mode(libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IWGRP)
                        .build()
                        .user_data(offset);
                    unsafe {
                        self.control_ring.submission().push(&sqe).map_err(|e| {
                            error!("Failed to push OpenAt SQE to submission queue: {:?}", e);
                            StoreError::IoUring
                        })?
                    };
                    self.inflight_control_tasks.insert(offset, Status::OpenAt);
                }
                _ => {}
            };
        }

        // It's OK to submit 0 entry.
        self.control_ring.submit().map_err(|e| {
            error!("Failed to submit SQEs to SQ: {:?}", e);
            StoreError::IoUring
        })?;

        self.delete_segments(to_remove);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::StoreError;
    use crate::io::buf::AlignedBuf;
    use crate::io::segment::{LogSegment, Status};
    use log::error;
    use std::error::Error;
    use std::fs::File;
    use std::sync::Arc;

    use super::Wal;

    fn create_wal(cfg: &Arc<config::Configuration>) -> Result<Wal, StoreError> {
        let control_ring = io_uring::IoUring::builder().dontfork().build(32).map_err(|e| {
            error!( "Failed to build I/O Uring instance for write-ahead-log segment file management: {:?}", e);
            StoreError::IoUring
        })?;

        Ok(Wal::new(control_ring, cfg))
    }

    #[test]
    fn test_load_wals() -> Result<(), StoreError> {
        let store_base = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_base(store_base.path().to_str().unwrap());
        cfg.check_and_apply()
            .expect("Failed to check-and-apply configuration");
        let segment_sum = cfg.store.total_segment_file_size / cfg.store.segment_size;
        let config = Arc::new(cfg);
        let mut wal = create_wal(&config)?;
        wal.load_from_paths()?;
        assert_eq!(segment_sum, wal.segments.len() as u64);
        Ok(())
    }

    #[test]
    fn test_expand_wals() -> Result<(), StoreError> {
        let store_base = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_base(store_base.path().to_str().unwrap());
        cfg.check_and_apply()
            .expect("Failed to check-and-apply configuration");
        let segment_size = cfg.store.segment_size;
        let segment_sum = cfg.store.total_segment_file_size / cfg.store.segment_size;
        let config = Arc::new(cfg);
        let files: Vec<_> = (0..10)
            .map(|i| {
                let f = config
                    .store
                    .path
                    .wal_path()
                    .join(LogSegment::format(i * segment_size));
                File::create(f.as_path())
            })
            .try_collect()?;
        assert_eq!(10, files.len());
        // Prepare log segment files
        let mut wal = create_wal(&config)?;
        wal.load_from_paths()?;
        assert_eq!(segment_sum, wal.segments.len() as u64);
        Ok(())
    }

    #[test]
    fn test_alloc_segment() -> Result<(), StoreError> {
        let wal_dir = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_wal(wal_dir.path().to_str().unwrap());
        let config = Arc::new(cfg);
        let mut wal = create_wal(&config)?;

        let segment = wal.alloc_segment()?;
        assert_eq!(0, segment.wal_offset);
        wal.segments.push_back(segment);

        let segment = wal.alloc_segment()?;
        assert_eq!(config.store.segment_size, segment.wal_offset);
        Ok(())
    }

    #[test]
    fn test_writable_segment_count() -> Result<(), StoreError> {
        let wal_dir = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_wal(wal_dir.path().to_str().unwrap());
        let config = Arc::new(cfg);
        let mut wal = create_wal(&config)?;

        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(1, wal.writable_segment_count());
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(2, wal.writable_segment_count());

        wal.segments.front_mut().unwrap().status = Status::Read;
        assert_eq!(1, wal.writable_segment_count());

        Ok(())
    }

    #[test]
    fn test_segment_file_of() -> Result<(), StoreError> {
        let wal_dir = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_wal(wal_dir.path().to_str().unwrap());
        let config = Arc::new(cfg);
        let mut wal = create_wal(&config)?;

        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(2, wal.segments.len());

        // Ensure we can get the right
        let segment = wal
            .segment_file_of(config.store.segment_size - 1)
            .ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(0, segment.wal_offset);

        let segment = wal.segment_file_of(0).ok_or(StoreError::AllocLogSegment)?;
        assert_eq!(0, segment.wal_offset);

        let segment = wal
            .segment_file_of(config.store.segment_size)
            .ok_or(StoreError::AllocLogSegment)?;

        assert_eq!(config.store.segment_size, segment.wal_offset);

        Ok(())
    }

    #[test]
    fn test_delete_segments() -> Result<(), Box<dyn Error>> {
        let wal_dir = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_wal(wal_dir.path().to_str().unwrap());
        let config = Arc::new(cfg);
        let mut wal = create_wal(&config)?;

        let segment = wal.alloc_segment()?;
        wal.segments.push_back(segment);
        assert_eq!(1, wal.segments.len());
        let offsets = wal
            .segments
            .iter_mut()
            .map(|segment| {
                segment.status = Status::UnlinkAt;
                segment.wal_offset
            })
            .collect();
        wal.delete_segments(offsets);
        assert_eq!(0, wal.segments.len());
        Ok(())
    }

    /// Test try_reclaim_segments
    #[test]
    fn test_try_reclaim_segments() -> Result<(), StoreError> {
        let wal_dir = tempfile::tempdir().map_err(StoreError::IO)?;
        let mut cfg = config::Configuration::default();
        cfg.store.path.set_wal(wal_dir.path().to_str().unwrap());
        let config = Arc::new(cfg);
        let mut wal = create_wal(&config)?;

        (0..2)
            .map(|_| wal.open_segment_directly())
            .collect::<Result<Vec<()>, StoreError>>()?;

        let (reclaimed_bytes, free_bytes) = wal.try_reclaim(4096);
        assert_eq!(0, reclaimed_bytes);
        assert_eq!(free_bytes, wal.wal_cache.max_cache_size);

        let cache_size_of_single_segment = wal.wal_cache.max_cache_size / 2;

        wal.segments.iter_mut().for_each(|segment| {
            segment.status = Status::Read;
            segment.written = segment.size;

            // Fill the block cache of the segment, so that it can be reclaimed
            // Each segment occupies 50% of the cache, and each cache entry occupies 1/8 of the cache
            let cache_size_of_single_entry = cache_size_of_single_segment / 8;
            let mut num_entries = cache_size_of_single_segment / cache_size_of_single_entry;

            // +1 to make sure the cache is full and the current cache size is over the max cache size,
            // to cover the edge case
            num_entries += 1;
            (0..num_entries).for_each(|index| {
                let buf = Arc::new(
                    AlignedBuf::new(
                        segment.wal_offset + index * cache_size_of_single_entry,
                        cache_size_of_single_entry as usize,
                        config.store.alignment,
                    )
                    .unwrap(),
                );
                buf.increase_written(cache_size_of_single_entry as usize);
                segment.block_cache.add_entry(buf);
            });

            assert_eq!(
                num_entries as u32 * cache_size_of_single_entry as u32,
                segment.block_cache.cache_size()
            );
        });

        let (_reclaimed_bytes, _free_bytes) = wal.try_reclaim(4096);
        Ok(())
    }
}

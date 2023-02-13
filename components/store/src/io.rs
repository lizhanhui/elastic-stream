use crate::error::StoreError;
use crossbeam::channel::{Receiver, Sender};
use std::os::fd::AsRawFd;

const DEFAULT_MAX_IO_DEPTH: u32 = 4096;
const DEFAULT_SQPOLL_CPU: u32 = 1;
const DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT: u32 = 2;
const DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT: u32 = 2;

struct Options {
    io_depth: u32,

    /// Bind the kernel's poll thread to the specified cpu.
    sqpoll_cpu: u32,

    max_workers: [u32; 2],
}

impl Default for Options {
    fn default() -> Self {
        Self {
            io_depth: DEFAULT_MAX_IO_DEPTH,
            sqpoll_cpu: DEFAULT_SQPOLL_CPU,
            max_workers: [
                DEFAULT_MAX_BOUNDED_URING_WORKER_COUNT,
                DEFAULT_MAX_UNBOUNDED_URING_WORKER_COUNT,
            ],
        }
    }
}

struct IO {
    uring: io_uring::IoUring,
    sender: Sender<()>,
    receiver: Receiver<()>,
}

impl IO {
    pub(crate) fn new(mut options: Options) -> Result<Self, StoreError> {
        let uring = io_uring::IoUring::builder()
            .dontfork()
            .setup_iopoll()
            .setup_sqpoll_cpu(options.sqpoll_cpu)
            .setup_r_disabled()
            .build(options.io_depth)
            .map_err(|_e| StoreError::IO_URING)?;

        let submitter = uring.submitter();
        submitter.register_iowq_max_workers(&mut options.max_workers)?;
        submitter.register_enable_rings()?;

        let (sender, receiver) = crossbeam::channel::unbounded();

        Ok(Self {
            uring,
            sender,
            receiver,
        })
    }

    pub(crate) fn run(&mut self) {
        let mut in_flight_requests = 0;
        loop {
            loop {
                if self.uring.params().sq_entries() <= in_flight_requests {
                    break;
                }

                if let Ok(_) = self.receiver.try_recv() {
                    in_flight_requests += 1;
                    todo!("Convert item to IO task");
                }
            }

            if let Ok(_reaped) = self.uring.submit_and_wait(1) {
                let mut completion = self.uring.completion();
                while let Some(_cqe) = completion.next() {
                    in_flight_requests -= 1;
                }
                completion.sync();
            } else {
                todo!("Log error");
                break;
            }
        }
    }
}

impl AsRawFd for IO {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.uring.as_raw_fd()
    }
}

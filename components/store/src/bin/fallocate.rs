use std::{
    alloc::{self, Layout},
    error::Error,
    ffi::CStr,
    fs::OpenOptions,
    os::{fd::AsRawFd, unix::prelude::OpenOptionsExt},
};

use io_uring::{opcode, register, types, IoUring};

fn main() -> Result<(), Box<dyn Error>> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .mode(libc::S_IRWXU | libc::S_IRWXG)
        .open("/data/abc.txt")?;

    let mut uring = IoUring::builder()
        .dontfork()
        .setup_iopoll()
        .setup_sqpoll(2000)
        .setup_sqpoll_cpu(1)
        .setup_r_disabled()
        .build(1024)?;

    let mut probe = register::Probe::new();
    uring.submitter().register_probe(&mut probe)?;
    uring.submitter().register_iowq_max_workers(&mut [2, 2])?;
    uring.submitter().register_enable_rings()?;

    if !probe.is_supported(opcode::Fallocate64::CODE) {
        eprintln!("Fallocate is not supported");
        return Ok(());
    }

    let mut ring = IoUring::builder()
        .dontfork()
        .setup_attach_wq(uring.as_raw_fd())
        .build(1024)?;

    const len: i64 = 1024 * 1024;

    // Fallocate to change file size
    {
        let fallocate_e = opcode::Fallocate64::new(types::Fd(file.as_raw_fd()), len)
            .mode(0)
            .offset64(0)
            .build()
            .user_data(1);
        unsafe { ring.submission().push(&fallocate_e)? };

        ring.submit_and_wait(1)?;

        let mut cq = ring.completion();
        while let Some(cqe) = cq.next() {
            println!("{cqe:#?}");
            perror(cqe.result())?;
        }
        cq.sync();
    }

    // Write 4KiB data
    {
        let alignment = 4096;
        let buf_size = 4096;

        let layout = Layout::from_size_align(buf_size, alignment)?;
        let ptr = unsafe { alloc::alloc(layout) };

        unsafe { libc::memset(ptr as *mut libc::c_void, 65, buf_size as libc::size_t) };

        let write_e = opcode::Write::new(types::Fd(file.as_raw_fd()), ptr, buf_size as u32)
            .offset(0)
            .build()
            .user_data(2);

        unsafe { uring.submission().push_multiple(&[write_e])? };
        uring.submit_and_wait(1)?;
        let mut cq = uring.completion();
        while let Some(cqe) = cq.next() {
            println!("{cqe:#?}");
            perror(cqe.result())?;
        }
    }

    Ok(())
}

fn perror(errno: i32) -> Result<(), Box<dyn Error>> {
    if errno >= 0 {
        return Ok(());
    }
    let ptr = unsafe { libc::strerror(-errno) };
    let str = unsafe { CStr::from_ptr(ptr) };
    println!("Reported Error Message: {}", str.to_str()?);
    Ok(())
}

use std::{cell::UnsafeCell, error::Error, ffi::CString, io::BufRead, os::fd::RawFd, rc::Rc};

use io_uring::{opcode, register, types, IoUring};

const WAL_FILE_SEGMENT_LENGTH: i64 = 1024 * 1024;

/// Create file with fixed size
///
/// If the file from `stdin` is not a absolute path, it will be created relative to current work directory
/// If it is absolute path, then current working directory is ignored.
fn main() -> Result<(), Box<dyn Error>> {
    let uring = IoUring::builder()
        .dontfork()
        // .setup_iopoll()
        .setup_sqpoll(2000)
        .setup_sqpoll_cpu(1)
        .setup_r_disabled()
        .build(1024)?;

    let mut probe = register::Probe::new();
    uring.submitter().register_probe(&mut probe)?;
    uring.submitter().register_iowq_max_workers(&mut [2, 2])?;
    uring.submitter().register_enable_rings()?;

    let uring = Rc::new(UnsafeCell::new(uring));

    let stdin = std::io::stdin();
    loop {
        let mut handle = stdin.lock();
        let mut file_name = String::new();
        handle.read_line(&mut file_name)?;
        let file_name = file_name.trim_end().to_owned();
        println!("File to create: {}", file_name);

        if file_name.eq_ignore_ascii_case("quit") {
            break;
        }

        let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), file_name.as_ptr() as *const i8)
            .flags(libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT)
            .mode(libc::S_IRWXU | libc::S_IRWXG)
            .build()
            .user_data(1); // `1` means need to fallocate for the result FD.

        let mut in_flights = 0;

        unsafe { (&mut *uring.get()).submission().push(&sqe)? };
        in_flights += 1;

        loop {
            unsafe { &mut *uring.get() }.submit_and_wait(1)?;

            let mut cq = unsafe { &mut *uring.get() }.completion();
            if let Some(cqe) = cq.next() {
                in_flights -= 1;
                println!("{cqe:#?}");
                if cqe.result() < 0 {
                    let ptr = unsafe { libc::strerror(-cqe.result()) };
                    match unsafe { CString::from_raw(ptr) }.into_string() {
                        Ok(s) => {
                            println!("{}", s);
                        }
                        Err(e) => {
                            eprintln!("Failed to convert CString to String: {:?}", e);
                        }
                    };
                    // Something is wrong
                    return Ok(());
                }

                if cqe.user_data() == 1 {
                    fallocate(
                        Rc::clone(&uring),
                        cqe.result(),
                        WAL_FILE_SEGMENT_LENGTH,
                        &mut in_flights,
                    )?;
                }
            }
            cq.sync();

            if 0 == in_flights {
                break;
            }
        }
    }

    Ok(())
}

fn fallocate(
    ring: Rc<UnsafeCell<IoUring>>,
    fd: RawFd,
    len: i64,
    in_flights: &mut usize,
) -> Result<(), Box<dyn Error>> {
    let sqe = opcode::Fallocate64::new(types::Fd(fd), len)
        .mode(0)
        .offset64(0)
        .build()
        .user_data(0);
    unsafe { (&mut *ring.get()).submission().push(&sqe)? };
    *in_flights += 1;
    Ok(())
}
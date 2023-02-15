use std::{error::Error, ffi::CString, io::BufRead};

use io_uring::{opcode, register, types, IoUring};

enum CreateFile {
    Open,
    Fallocate,
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut uring = IoUring::builder()
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

    let stdin = std::io::stdin();
    loop {
        let mut handle = stdin.lock();
        let mut file_name = String::new();
        handle.read_line(&mut file_name)?;
        let file_name = file_name.trim_end().to_owned();
        println!("File to create: {}", file_name);

        let sqe = opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), file_name.as_ptr() as *const i8)
            .flags(libc::O_CREAT | libc::O_RDWR | libc::O_DIRECT)
            .mode(libc::S_IRWXU | libc::S_IRWXG)
            .build()
            .user_data(1);

        unsafe { uring.submission().push(&sqe)? };

        uring.submit_and_wait(1)?;

        let mut cq = uring.completion();
        if let Some(cqe) = cq.next() {
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
                break;
            }
        }
        cq.sync();
    }

    Ok(())
}

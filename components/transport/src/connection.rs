use crate::connection_state::ConnectionState;
use crate::ConnectionError;
use crate::WriteTask;
use bytes::{Buf, BytesMut};
use codec::error::FrameError;
use codec::frame::Frame;
use local_sync::{
    mpsc::unbounded::{self, Rx, Tx},
    oneshot,
};
use log::debug;
use log::{error, info, trace, warn};
use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Cursor;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::fd::IntoRawFd;
use std::rc::Rc;
use std::time::Duration;
use tokio_uring::net::TcpStream;

const BUFFER_SIZE: usize = 4 * 1024;

pub struct Connection {
    /// Underlying TCP stream.
    stream: Option<Rc<TcpStream>>,

    /// Local socket address
    local_addr: Option<SocketAddr>,

    /// Peer socket address
    remote_addr: SocketAddr,

    /// Connection state
    state: Rc<RefCell<ConnectionState>>,

    /// Read buffer for this connection.
    buffer: UnsafeCell<BytesMut>,

    /// Write buffer for this connection.
    ///
    /// Writes from concurrent coroutines are linearized by this MPSC channel.
    tx: RefCell<Option<Tx<WriteTask>>>,

    /// Pending writes that arrives during establishing of a TCP connection.
    tasks: UnsafeCell<VecDeque<WriteTask>>,
}

impl Connection {
    fn spawn_write_loop(
        stream: Rc<TcpStream>,
        mut rx: Rx<WriteTask>,
        connection_state: Rc<RefCell<ConnectionState>>,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
    ) {
        tokio_uring::spawn(async move {
            trace!("Start write coroutine loop for {remote_addr}");
            loop {
                match rx.recv().await {
                    Some(task) => {
                        trace!(
                            "Write-frame-task[stream-id={}] received, start writing to {remote_addr}",
                            task.frame.stream_id,

                        );

                        if let Err(e) = Self::write(&stream, &task.frame, remote_addr).await {
                            match e {
                                ConnectionError::EncodeFrame(e) => {
                                    error!("Failed to encode frame: {}", e);
                                    let _ =
                                        task.observer.send(Err(ConnectionError::EncodeFrame(e)));
                                }

                                ConnectionError::Network(e) => {
                                    error!("Failed to write frame to network due to {}, closing underlying TcpStream", e);
                                    *connection_state.borrow_mut() = ConnectionState::Closed;
                                    let _ = stream.shutdown(Shutdown::Both);
                                    break;
                                }

                                ConnectionError::NotConnected | ConnectionError::Timeout { .. } => {
                                    unreachable!("Should not reach here");
                                }
                            }
                        } else {
                            let _ = task.observer.send(Ok(()));
                        }
                    }
                    None => {
                        info!(
                            "Connection {local_addr} --> {remote_addr} should be closed, stop write coroutine loop"
                        );
                        break;
                    }
                }
            }
        });
    }

    pub fn new(remote_addr: SocketAddr) -> Self {
        Self {
            remote_addr,
            stream: None,
            local_addr: None,
            state: Rc::new(RefCell::new(ConnectionState::Unspecified)),
            buffer: UnsafeCell::new(BytesMut::with_capacity(BUFFER_SIZE)),
            tx: RefCell::new(None),
            tasks: UnsafeCell::new(VecDeque::new()),
        }
    }

    /// Create connection with an established `TcpStream`.
    pub fn with_stream(
        stream: TcpStream,
        remote_addr: SocketAddr,
    ) -> Result<Self, ConnectionError> {
        let local_addr = Self::local_addr_of(&stream).map_err(|e| {
            error!(
                "Failed to acquire local address from the given TcpStream: {:?}",
                e
            );
            ConnectionError::Network(e)
        })?;

        let (tx, rx) = unbounded::channel();
        let stream = Rc::new(stream);
        let state = Rc::new(RefCell::new(ConnectionState::Active));
        Self::spawn_write_loop(
            Rc::clone(&stream),
            rx,
            Rc::clone(&state),
            local_addr,
            remote_addr,
        );

        Ok(Self {
            remote_addr,
            stream: Some(stream),
            local_addr: Some(local_addr),
            state,
            buffer: UnsafeCell::new(BytesMut::with_capacity(BUFFER_SIZE)),
            tx: RefCell::new(Some(tx)),
            tasks: UnsafeCell::new(VecDeque::new()),
        })
    }

    pub async fn connect(&mut self, timeout: Duration) -> Result<(), ConnectionError> {
        *self.state.borrow_mut() = ConnectionState::Connecting;

        // Clean all buffered `WriteTask`s after completion of establishing connection
        let _drain = BufferedTaskDrain {
            tasks: unsafe { &mut *self.tasks.get() },
        };
        let connect = TcpStream::connect(self.remote_addr);
        let connect = tokio::time::timeout(timeout, connect);
        let stream = match connect.await {
            Ok(res) => match res {
                Ok(stream) => {
                    debug!("Connection to {} established", self.remote_addr);
                    stream
                }
                Err(e) => {
                    *self.state.borrow_mut() = ConnectionState::Closed;
                    error!("Failed to connect to {}: {:?}", self.remote_addr, e);
                    return Err(ConnectionError::Network(e));
                }
            },
            Err(elapsed) => {
                *self.state.borrow_mut() = ConnectionState::Closed;
                error!(
                    "Failed to connect to {} due to timeout, elapsed {elapsed}",
                    self.remote_addr
                );
                return Err(ConnectionError::Timeout {
                    target: self.remote_addr,
                    elapsed,
                });
            }
        };

        let local_addr = Self::local_addr_of(&stream).map_err(|e| {
            error!(
                "Failed to acquire local address of an established connection: {:?}",
                e
            );
            let _ = stream.shutdown(Shutdown::Both);
            *self.state.borrow_mut() = ConnectionState::Closed;
            ConnectionError::Network(e)
        })?;

        stream.set_nodelay(true).map_err(|e| {
            error!("Failed to disable TCP Nagle's algorithm");
            ConnectionError::Network(e)
        })?;

        let stream = Rc::new(stream);

        let (tx, rx) = unbounded::channel();

        *self.state.borrow_mut() = ConnectionState::Active;
        self.local_addr = Some(local_addr);

        // While this coroutine is establishing TCP connection, other coroutine tasks
        // may have queued up some pending requests.
        // Now that we got a connection, it's time to write and flush them to network.
        let tasks = unsafe { &mut *self.tasks.get() };
        while let Some(task) = tasks.pop_front() {
            let _ = tx.send(task);
        }
        self.tx = RefCell::new(Some(tx));

        Self::spawn_write_loop(
            Rc::clone(&stream),
            rx,
            Rc::clone(&self.state),
            local_addr,
            self.remote_addr,
        );
        self.stream = Some(stream);

        Ok(())
    }

    async fn write(
        stream: &Rc<TcpStream>,
        frame: &Frame,
        remote_addr: SocketAddr,
    ) -> Result<(), ConnectionError> {
        let mut buffers = frame.encode()?;

        let total = buffers.iter().map(|b| b.len()).sum::<usize>();
        trace!("Get {} bytes to write to: {}", total, remote_addr);
        let mut remaining = total;
        loop {
            let (res, _buffers) = stream.writev(buffers).await;
            buffers = _buffers;
            let mut n = res?;
            debug_assert!(n <= remaining, "Data written to socket buffer should be less than or equal to remaining bytes to write");
            if n == remaining {
                if remaining == total {
                    // First time to write
                    trace!("Wrote {}/{} bytes to {}", n, total, remote_addr);
                } else {
                    // Last time of writing: the remaining are all written.
                    remaining -= n;
                    trace!(
                        "Wrote {}/{} bytes to {}. Overall, {}/{} is written.",
                        n,
                        total,
                        remote_addr,
                        total - remaining,
                        total
                    );
                }
                break;
            } else {
                remaining -= n;
                trace!(
                    "Wrote {} bytes to {}. Overall, {}/{} is written",
                    n,
                    remote_addr,
                    total - remaining,
                    total,
                );
                // Drain/advance buffers that are already written.
                buffers
                    .extract_if(|buffer| {
                        if buffer.len() <= n {
                            n -= buffer.len();
                            // Remove it
                            true
                        } else {
                            if n > 0 {
                                buffer.advance(n);
                                n = 0;
                            }
                            // Keep the buffer slice
                            false
                        }
                    })
                    .count();
            }
        }
        Ok(())
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    #[allow(clippy::mut_from_ref)]
    pub fn buf_mut(&self) -> &mut BytesMut {
        unsafe { &mut *self.buffer.get() }
    }

    pub fn buf(&self) -> &BytesMut {
        unsafe { &*self.buffer.get() }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&self) -> Result<Option<Frame>, FrameError> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Try to allocate more memory from allocator
            let buffer = self.buf_mut();
            if buffer.spare_capacity_mut().len() < BUFFER_SIZE {
                buffer.reserve(BUFFER_SIZE);
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            let len = buffer.len();
            let buf = buffer.split_off(len);
            let (res, buf) = match &self.stream {
                Some(stream) => stream.read(buf).await,
                None => {
                    return Err(FrameError::ConnectionReset);
                }
            };
            buffer.unsplit(buf);

            let read = match res {
                Ok(n) => {
                    trace!("Read {} bytes from {}", n, self.remote_addr);
                    n
                }
                Err(_e) => {
                    info!("Failed to read data from {}", self.remote_addr);
                    0
                }
            };

            if 0 == read {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if buffer.is_empty() {
                    return Ok(None);
                } else {
                    warn!("Discarded {} bytes", buffer.len());
                    return Err(FrameError::ConnectionReset);
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&self) -> Result<Option<Frame>, FrameError> {
        use FrameError::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let buffer = self.buf();
        let mut buf = Cursor::new(&buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buf_mut().advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }

            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),

            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e),
        }
    }

    pub async fn write_frame(&self, frame: Frame) -> Result<(), ConnectionError> {
        let (tx, rx) = oneshot::channel();
        let task = WriteTask {
            frame,
            observer: tx,
        };

        match *self.tx.borrow() {
            Some(ref sq) => {
                sq.send(task).map_err(|_e| {
                    warn!(
                        "Failed to send frame to connection's internal MPSC channel. TcpStream has been closed",
                    );
                    ConnectionError::Network(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Connection MPSC channel closed"))
                })?;
            }

            None => {
                if self.state() == ConnectionState::Connecting {
                    // Another coroutine task is establishing a connection, put current write request into
                    // write buffer.
                    let tasks = unsafe { &mut *self.tasks.get() };
                    tasks.push_back(task);
                } else {
                    return Err(ConnectionError::NotConnected);
                }
            }
        }

        rx.await.map_err(|_e| {
            ConnectionError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Connection is closed",
            ))
        })?
    }

    pub fn close(&self) -> std::io::Result<()> {
        if ConnectionState::Active == *self.state.borrow() {
            *self.state.borrow_mut() = ConnectionState::Closed;

            // Drop tx, therefore, close the write coroutine loop task.
            self.tx.borrow_mut().take();

            if let Some(ref stream) = self.stream {
                return stream.shutdown(Shutdown::Both);
            }
        }
        Ok(())
    }

    pub fn state(&self) -> ConnectionState {
        *self.state.borrow()
    }

    /// Expose this method, allowing upper layer to flag this connection as going away.
    pub fn set_state(&self, state: ConnectionState) {
        *self.state.borrow_mut() = state;
    }

    pub fn local_addr_of(stream: &TcpStream) -> std::io::Result<SocketAddr> {
        let fd = stream.as_raw_fd();
        let std_tcp_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
        let local_addr = std_tcp_stream.local_addr();
        let _ = std_tcp_stream.into_raw_fd();
        local_addr
    }

    pub fn peer_addr_of(stream: &TcpStream) -> std::io::Result<SocketAddr> {
        let fd = stream.as_raw_fd();
        let std_tcp_stream = unsafe { std::net::TcpStream::from_raw_fd(fd) };
        let peer_addr = std_tcp_stream.peer_addr();
        let _ = std_tcp_stream.into_raw_fd();
        peer_addr
    }
}

impl Display for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.local_addr {
            Some(addr) => {
                write!(f, "{} --> {}", addr, self.remote_addr)
            }
            None => {
                write!(f, "127.0.0.1:0 --> {}", self.remote_addr)
            }
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        info!("Close connection {}", self);
        let _ = self.close();
    }
}

/// `RAII` for buffered write tasks during connection in progress
struct BufferedTaskDrain<'a> {
    tasks: &'a mut VecDeque<WriteTask>,
}

impl<'a> Drop for BufferedTaskDrain<'a> {
    fn drop(&mut self) {
        self.tasks.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, thread::JoinHandle, time::Duration};

    use bytes::{Buf, BufMut, BytesMut};
    use protocol::rpc::header::OperationCode;
    use tokio::{io::AsyncReadExt, net::TcpListener};

    #[test]
    fn test_bytes_concept() {
        let mut buf = BytesMut::with_capacity(128);
        buf.put_i32(1);
        assert_eq!(128, buf.capacity());
        assert_eq!(4, buf.len());
        assert_eq!(4, buf.remaining());
        assert_eq!(124, buf.spare_capacity_mut().len());

        buf.get_u16();

        assert_eq!(126, buf.capacity());
        assert_eq!(2, buf.len());
        assert_eq!(2, buf.remaining());
        assert_eq!(124, buf.spare_capacity_mut().len());

        buf.put_u8(4u8);
        assert_eq!(126, buf.capacity());
        assert_eq!(3, buf.len());
        assert_eq!(3, buf.remaining());
        assert_eq!(123, buf.spare_capacity_mut().len());

        buf.put_u8(2u8);
        assert_eq!(126, buf.capacity());
        assert_eq!(4, buf.len());
        assert_eq!(4, buf.remaining());
        assert_eq!(122, buf.spare_capacity_mut().len());

        buf.get_u32();
        assert_eq!(122, buf.capacity());
        assert_eq!(0, buf.len());
        assert_eq!(0, buf.remaining());
        assert_eq!(122, buf.spare_capacity_mut().len());
    }

    fn run_server(
        counter: tokio::sync::oneshot::Sender<usize>,
        port: tokio::sync::oneshot::Sender<u16>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let handle = std::thread::Builder::new()
            .name("Test-Server".to_owned())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let local_addr = listener.local_addr().unwrap();
                    port.send(local_addr.port()).unwrap();
                    let (mut stream, _addr) = listener.accept().await.unwrap();
                    let mut read = 0;
                    let mut buf = BytesMut::with_capacity(1024);
                    while let Ok(n) = stream.read_buf(&mut buf).await {
                        if 0 == n {
                            break;
                        }
                        read += n;
                        buf.clear();
                    }
                    counter.send(read).unwrap();
                });
            })?;
        Ok(handle)
    }

    #[test]
    fn test_write_frame() -> Result<(), Box<dyn Error>> {
        ulog::try_init_log();
        let (counter_tx, counter_rx) = tokio::sync::oneshot::channel();
        let (port_tx, port_rx) = tokio::sync::oneshot::channel();

        let handle = run_server(counter_tx, port_tx).unwrap();

        let port = port_rx.blocking_recv()?;

        tokio_uring::start(async {
            let address = format!("127.0.0.1:{}", port);
            {
                let remote_addr = address.parse().unwrap();

                let mut connection = super::Connection::new(remote_addr);
                connection.connect(Duration::from_secs(3)).await.unwrap();

                let mut frame = codec::frame::Frame::new(OperationCode::ALLOCATE_ID);
                let mut payload = vec![];
                (0..8).for_each(|_| {
                    let mut buf = BytesMut::with_capacity(1024 * 1024);
                    buf.resize(1024 * 1024, 8u8);
                    payload.push(buf.freeze());
                });
                frame.payload = Some(payload);
                connection.write_frame(frame).await.unwrap();
                connection.close().unwrap();
            }

            let total = counter_rx.await.unwrap();
            assert_eq!(total, 1024 * 1024 * 8 + 20);
        });

        let _ = handle.join();

        Ok(())
    }
}

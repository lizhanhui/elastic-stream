use crate::ConnectionError;
use crate::WriteTask;
use bytes::{Buf, BytesMut};
use codec::error::FrameError;
use codec::frame::Frame;
use local_sync::{mpsc, oneshot};
use log::{error, info, trace, warn};
use monoio::io::{AsyncReadRent, AsyncWriteRent, AsyncWriteRentExt};
use std::io::Cursor;

const BUFFER_SIZE: usize = 4 * 1024;

pub struct ChannelReader<S> {
    stream: S,
    peer_address: String,
    /// Read buffer for this connection.
    buffer: BytesMut,
}

impl<S> ChannelReader<S>
where
    S: AsyncReadRent,
{
    pub fn new(stream: S, peer_address: String) -> Self {
        Self {
            stream,
            peer_address,
            buffer: BytesMut::with_capacity(BUFFER_SIZE),
        }
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
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // Try to allocate more memory from allocator
            if self.buffer.spare_capacity_mut().len() < BUFFER_SIZE {
                self.buffer.reserve(BUFFER_SIZE);
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            let len = self.buffer.len();
            let buf = self.buffer.split_off(len);
            let (res, buf) = self.stream.read(buf).await;
            self.buffer.unsplit(buf);

            let read = match res {
                Ok(n) => {
                    trace!("Read {} bytes from {}", n, self.peer_address);
                    n
                }
                Err(_e) => {
                    info!("Failed to read data from {}", self.peer_address);
                    0
                }
            };

            if 0 == read {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    warn!("Discarded {} bytes", self.buffer.len());
                    return Err(FrameError::ConnectionReset);
                }
            }
        }
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> Result<Option<Frame>, FrameError> {
        use FrameError::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

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
                self.buffer.advance(len);

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
}

#[derive(Clone)]
pub struct ChannelWriter {
    peer_address: String,
    tx: mpsc::unbounded::Tx<WriteTask>,
}

impl ChannelWriter {
    pub fn new<S>(mut stream: S, peer_address: String) -> Self
    where
        S: AsyncWriteRent + 'static,
    {
        let (tx, mut rx) = mpsc::unbounded::channel::<WriteTask>();
        let target_address = peer_address.clone();
        monoio::spawn(async move {
            trace!("Start write coroutine loop for {}", &target_address);
            loop {
                match rx.recv().await {
                    Some(task) => {
                        trace!(
                            "Write-frame-task[stream-id={}] received, start writing to {}",
                            task.frame.stream_id,
                            &target_address
                        );

                        if let Err(e) = Self::write(&mut stream, &task.frame, &target_address).await
                        {
                            match e {
                                ConnectionError::EncodeFrame(e) => {
                                    error!("Failed to encode frame: {}", e.to_string());
                                    let _ =
                                        task.observer.send(Err(ConnectionError::EncodeFrame(e)));
                                }

                                ConnectionError::Network(e) => {
                                    error!("Failed to write frame to network due to {}, closing underlying TcpStream", e.to_string());
                                    let _ = stream.shutdown().await;
                                    break;
                                }
                            }
                        } else {
                            let _ = task.observer.send(Ok(()));
                        }
                    }
                    None => {
                        match stream.shutdown().await {
                            Ok(()) => {
                                info!("Closed write half of connection to {}", &target_address);
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to close write half of connection to {}",
                                    &target_address
                                );
                            }
                        }
                        break;
                    }
                }
            }
        });

        Self { peer_address, tx }
    }

    async fn write<S>(
        stream: &mut S,
        frame: &Frame,
        peer_address: &str,
    ) -> Result<(), ConnectionError>
    where
        S: AsyncWriteRent,
    {
        let buffers = frame.encode()?;
        let io_vec = buffers
            .iter()
            .map(|buf| {
                let ptr = buf.as_ptr();
                let len = buf.len();
                libc::iovec {
                    iov_base: ptr as *mut libc::c_void,
                    iov_len: len as _,
                }
            })
            .collect::<Vec<_>>();
        let (res, _b) = stream.write_vectored_all(io_vec).await;
        let written = res.map_err(ConnectionError::Network)?;
        trace!("Wrote {written} bytes to {peer_address}");
        Ok(())
    }

    pub async fn write_frame(&self, frame: Frame) -> Result<(), ConnectionError> {
        let (tx, rx) = oneshot::channel();
        let task = WriteTask {
            frame,
            observer: tx,
        };

        self.tx.send(task).map_err(|_e| {
            warn!(
                "Failed to send frame to connection's internal MPSC channel. TcpStream has been closed",
            );
            ConnectionError::Network(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Connection MPSC channel closed"))
        })?;

        rx.await.map_err(|_e| {
            ConnectionError::Network(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "Connection is closed",
            ))
        })?
    }

    pub fn peer_address(&self) -> &str {
        &self.peer_address
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, thread::JoinHandle};

    use bytes::{Buf, BufMut, BytesMut};
    use monoio::{
        io::{AsyncReadRent, Splitable},
        net::{TcpListener, TcpStream},
    };
    use protocol::rpc::header::OperationCode;

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
        counter: flume::Sender<usize>,
        port: flume::Sender<u16>,
    ) -> Result<JoinHandle<()>, Box<dyn Error>> {
        let handle = std::thread::Builder::new()
            .name("Test-Server".to_owned())
            .spawn(move || {
                let rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                    let local_addr = listener.local_addr().unwrap();
                    port.send(local_addr.port()).unwrap();
                    let (mut stream, _addr) = listener.accept().await.unwrap();
                    let mut read = 0;
                    let mut buf = BytesMut::with_capacity(1024);
                    while let (res, buf_) = stream.read(buf).await {
                        let n = res.unwrap();
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
        let (counter_tx, counter_rx) = flume::bounded(1);
        let (port_tx, port_rx) = flume::bounded(1);

        let handle = run_server(counter_tx, port_tx).unwrap();

        let port = port_rx.recv()?;
        let rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()?;

        rt.block_on(async move {
            let address = format!("127.0.0.1:{}", port);
            {
                let tcp_stream = TcpStream::connect(&address).await.unwrap();
                tcp_stream.set_nodelay(true).unwrap();
                let (read_half, write_half) = tcp_stream.into_split();
                let channel_writer = super::ChannelWriter::new(write_half, address.clone());
                let mut frame = codec::frame::Frame::new(OperationCode::ALLOCATE_ID);
                let mut payload = vec![];
                (0..8).for_each(|_| {
                    let mut buf = BytesMut::with_capacity(1024 * 1024);
                    buf.resize(1024 * 1024, 8u8);
                    payload.push(buf.freeze());
                });
                frame.payload = Some(payload);
                channel_writer.write_frame(frame).await.unwrap();
                drop(channel_writer);
            }

            let total = counter_rx.recv_async().await.unwrap();
            assert_eq!(total, 1024 * 1024 * 8 + 20);
        });

        let _ = handle.join();

        Ok(())
    }
}

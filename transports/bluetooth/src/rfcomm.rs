// Copyright 2019 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use crate::{Addr, rfcomm_socket::RfcommSocket};
use futures::{prelude::*, try_ready};
use std::{fmt, io, mem};

/// A stream, similar to `TcpStream`, that operates over the RFCOMM Bluetooth protocol.
pub struct RfcommStream {
    inner: tokio_reactor::PollEvented<RfcommSocket>,
}

impl RfcommStream {
    /// Creates a stream that tries to connect to the given Bluetooth device on the given port.
    pub fn connect(dest: Addr, port: u8) -> RfcommStreamFuture {
        let socket = match RfcommSocket::connect(dest, port) {
            Ok(s) => s,
            Err(err) => return RfcommStreamFuture {
                inner: RfcommStreamFutureInner::Error(err)
            },
        };

        RfcommStreamFuture {
            inner: RfcommStreamFutureInner::Waiting(tokio_reactor::PollEvented::new(socket))
        }
    }
}

/// Bluetooth dialing in progress.
pub struct RfcommStreamFuture {
    inner: RfcommStreamFutureInner,
}

enum RfcommStreamFutureInner {
    /// We are waiting for the socket to be writable.
    Waiting(tokio_reactor::PollEvented<RfcommSocket>),
    /// An error happened during the construction of the socket.
    Error(io::Error),
    /// The future is finished. Polling it again should error or panic.
    Finished,
}

impl Future for RfcommStreamFuture {
    type Item = RfcommStream;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match mem::replace(&mut self.inner, RfcommStreamFutureInner::Finished) {
            RfcommStreamFutureInner::Waiting(socket) => match socket.poll_write_ready() {
                Ok(Async::Ready(_)) => {
                    Ok(Async::Ready(RfcommStream {
                        inner: socket,
                    }))
                }
                Ok(Async::NotReady) => {
                    self.inner = RfcommStreamFutureInner::Waiting(socket);
                    Ok(Async::NotReady)
                }
                Err(err) => Err(err),
            },
            RfcommStreamFutureInner::Error(err) => Err(err),
            RfcommStreamFutureInner::Finished => panic!("future polled after finished"),
        }
    }
}

impl io::Read for RfcommStream {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.inner.read(buf)
    }
}

impl tokio_io::AsyncRead for RfcommStream {
    // TODO: specialize functions
}

impl io::Write for RfcommStream {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        self.inner.flush()
    }
}

impl tokio_io::AsyncWrite for RfcommStream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        self.inner.shutdown()
    }
}

impl fmt::Debug for RfcommStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Ok((addr, port)) = self.inner.get_ref().getsockname() {
            f.debug_tuple("RfcommStream")
                .field(&addr)
                .field(&port)
                .finish()
        } else {
            f.debug_tuple("RfcommStream").finish()
        }
    }
}

impl fmt::Debug for RfcommStreamFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let RfcommStreamFutureInner::Waiting(inner) = &self.inner {
            if let Ok((addr, port)) = inner.get_ref().getsockname() {
                return f.debug_tuple("RfcommStreamFuture")
                    .field(&addr)
                    .field(&port)
                    .finish();
            }
        }

        f.debug_tuple("RfcommStreamFuture").finish()
    }
}

/// Stream that listens for incoming RFCOMM connections.
///
/// Implements the `Stream` trait and yields incoming connections.
pub struct RfcommListener {
    inner: tokio_reactor::PollEvented<RfcommSocket>,
}

impl RfcommListener {
    /// Builds a listener that listens on the given address with the given port.
    pub fn bind(addr: Addr, port: u8) -> Result<(RfcommListener, u8), io::Error> {
        crate::discoverable::enable_discoverable(&addr)?;

        let (socket, actual_port) = if port != 0 {
            (RfcommSocket::bind(addr, port)?, port)
        } else {
            (1..30)
                .filter_map(|port| {
                    RfcommSocket::bind(addr, port).ok().map(|s| (s, port))
                })
                .next()
                .ok_or_else(io::Error::last_os_error)?
        };

        let inner = RfcommListener {
            inner: tokio_reactor::PollEvented::new(socket),
        };

        Ok((inner, actual_port))
    }
}

impl Stream for RfcommListener {
    type Item = (RfcommStream, Addr);
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let ready = mio::Ready::readable();
        try_ready!(self.inner.poll_read_ready(ready));

        match self.inner.get_ref().accept() {
            Ok((client, addr)) => {
                let stream = RfcommStream {
                    inner: tokio_reactor::PollEvented::new(client)
                };
                Ok(Async::Ready(Some((stream, addr))))
            },
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                self.inner.clear_read_ready(ready)?;
                Ok(Async::NotReady)
            }
            Err(e) => Err(e),
        }
    }
}

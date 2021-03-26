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

//! Noise protocol I/O.

mod framed;
pub mod handshake;

use bytes::Bytes;
use framed::{MAX_FRAME_LEN, NoiseFramed};
use futures::ready;
use futures::prelude::*;
use log::trace;
use std::{cmp::min, fmt, io, pin::Pin, task::{Context, Poll}};

/// A noise session to a remote.
///
/// `T` is the type of the underlying I/O resource.
pub struct NoiseOutput<T> {
    io: NoiseFramed<T, snow::TransportState>,
    recv_buffer: Bytes,
    recv_offset: usize,
    send_buffer: Vec<u8>,
    send_offset: usize,
}

impl<T> fmt::Debug for NoiseOutput<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoiseOutput")
            .field("io", &self.io)
            .finish()
    }
}

impl<T> NoiseOutput<T> {
    fn new(io: NoiseFramed<T, snow::TransportState>) -> Self {
        NoiseOutput {
            io,
            recv_buffer: Bytes::new(),
            recv_offset: 0,
            send_buffer: Vec::new(),
            send_offset: 0,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for NoiseOutput<T> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        loop {
            let len = self.recv_buffer.len();
            let off = self.recv_offset;
            if len > 0 {
                let n = min(len - off, buf.len());
                buf[.. n].copy_from_slice(&self.recv_buffer[off .. off + n]);
                trace!("read: copied {}/{} bytes", off + n, len);
                self.recv_offset += n;
                if len == self.recv_offset {
                    trace!("read: frame consumed");
                    // Drop the existing view so `NoiseFramed` can reuse
                    // the buffer when polling for the next frame below.
                    self.recv_buffer = Bytes::new();
                }
                return Poll::Ready(Ok(n))
            }

            match Pin::new(&mut self.io).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(0)),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                Poll::Ready(Some(Ok(frame))) => {
                    self.recv_buffer = frame;
                    self.recv_offset = 0;
                }
            }
        }
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for NoiseOutput<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = Pin::into_inner(self);
        let mut io = Pin::new(&mut this.io);
        let frame_buf = &mut this.send_buffer;

        // The MAX_FRAME_LEN is the maximum buffer size before a frame must be sent.
        if this.send_offset == MAX_FRAME_LEN {
            trace!("write: sending {} bytes", MAX_FRAME_LEN);
            ready!(io.as_mut().poll_ready(cx))?;
            io.as_mut().start_send(&frame_buf)?;
            this.send_offset = 0;
        }

        let off = this.send_offset;
        let n = min(MAX_FRAME_LEN, off.saturating_add(buf.len()));
        this.send_buffer.resize(n, 0u8);
        let n = min(MAX_FRAME_LEN - off, buf.len());
        this.send_buffer[off .. off + n].copy_from_slice(&buf[.. n]);
        this.send_offset += n;
        trace!("write: buffered {} bytes", this.send_offset);

        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = Pin::into_inner(self);
        let mut io = Pin::new(&mut this.io);
        let frame_buf = &mut this.send_buffer;

        // Check if there is still one more frame to send.
        if this.send_offset > 0 {
            ready!(io.as_mut().poll_ready(cx))?;
            trace!("flush: sending {} bytes", this.send_offset);
            io.as_mut().start_send(&frame_buf)?;
            this.send_offset = 0;
        }

        io.as_mut().poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>>{
        ready!(self.as_mut().poll_flush(cx))?;
        Pin::new(&mut self.io).poll_close(cx)
    }
}

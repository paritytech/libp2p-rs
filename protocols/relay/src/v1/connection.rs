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

use bytes::Bytes;
use futures::channel::oneshot;
use futures::io::{AsyncRead, AsyncWrite};
use libp2p_swarm::NegotiatedSubstream;
use std::io::{Error, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A [`NegotiatedSubstream`] acting as a relayed [`Connection`].
#[derive(Debug)]
pub struct Connection {
    /// [`Connection`] might at first return data, that was already read during relay negotiation.
    initial_data: Bytes,
    stream: NegotiatedSubstream,
    /// Notifies the other side of the channel of this [`Connection`] being dropped.
    _notifier: oneshot::Sender<()>,
}

impl Unpin for Connection {}

impl Connection {
    pub fn new(
        initial_data: Bytes,
        stream: NegotiatedSubstream,
        notifier: oneshot::Sender<()>,
    ) -> Self {
        Connection {
            initial_data,
            stream,

            _notifier: notifier,
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        Pin::new(&mut self.stream).poll_close(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        bufs: &[IoSlice],
    ) -> Poll<Result<usize, Error>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Error>> {
        if !self.initial_data.is_empty() {
            let n = std::cmp::min(self.initial_data.len(), buf.len());
            let data = self.initial_data.split_to(n);
            buf[0..n].copy_from_slice(&data[..]);
            return Poll::Ready(Ok(n));
        }

        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

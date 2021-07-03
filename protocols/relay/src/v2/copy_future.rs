// Copyright 2020 Parity Technologies (UK) Ltd.
// Copyright 2021 Protocol Labs.
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

//! Helper to interconnect two substreams, connecting the receiver side of A with the sender side of
//! B and vice versa.
//!
//! Inspired by [`futures::io::Copy`].

use futures::future::Future;
use futures::future::FutureExt;
use futures::io::{AsyncBufRead, BufReader};
use futures::io::{AsyncRead, AsyncWrite};
use futures::ready;
use futures_timer::Delay;
use std::convert::TryInto;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct CopyFuture<S, D> {
    src: BufReader<S>,
    dst: BufReader<D>,

    max_circuit_duration: Delay,
    max_circuit_bytes: u64,
    bytes_sent: u64,
}

impl<S: AsyncRead, D: AsyncRead> CopyFuture<S, D> {
    pub fn new(src: S, dst: D, max_circuit_duration: Duration, max_circuit_bytes: u64) -> Self {
        CopyFuture {
            src: BufReader::new(src),
            dst: BufReader::new(dst),
            max_circuit_duration: Delay::new(max_circuit_duration),
            max_circuit_bytes,
            bytes_sent: Default::default(),
        }
    }
}

impl<S, D> Future for CopyFuture<S, D>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: AsyncRead + AsyncWrite + Unpin,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        loop {
            if this.bytes_sent > this.max_circuit_bytes {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Max circuit bytes reached.",
                )));
            }

            enum Status {
                Pending,
                Done,
                Progressed,
            }

            let src_status = match forward_data(&mut this.src, &mut this.dst, cx) {
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => Status::Done,
                Poll::Ready(Ok(i)) => {
                    this.bytes_sent += i;
                    Status::Progressed
                }
                Poll::Pending => Status::Pending,
            };

            let dst_status = match forward_data(&mut this.dst, &mut this.src, cx) {
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(0)) => Status::Done,
                Poll::Ready(Ok(i)) => {
                    this.bytes_sent += i;
                    Status::Progressed
                }
                Poll::Pending => Status::Pending,
            };

            match (src_status, dst_status) {
                // Both source and destination are done sending data.
                (Status::Done, Status::Done) => return Poll::Ready(Ok(())),
                // Either source or destination made progress.
                (Status::Progressed, _) | (_, Status::Progressed) => {}
                // Both are pending. Check if max circuit duration timer fired, otherwise return
                // Poll::Pending.
                (Status::Pending, Status::Pending) => break,
                // One is done sending data, the other is pending. Check if timer fired, otherwise
                // return Poll::Pending.
                (Status::Pending, Status::Done) | (Status::Done, Status::Pending) => break,
            }
        }

        if let Poll::Ready(()) = this.max_circuit_duration.poll_unpin(cx) {
            return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
        }

        Poll::Pending
    }
}

/// Forwards data from `source` to `destination`.
///
/// Returns `0` when done, i.e. `source` having reached EOF, returns number of bytes sent otherwise,
/// thus indicating progress.
fn forward_data<S: AsyncBufRead + Unpin, D: AsyncWrite + Unpin>(
    mut src: &mut S,
    mut dst: &mut D,
    cx: &mut Context<'_>,
) -> Poll<io::Result<u64>> {
    let buffer = ready!(Pin::new(&mut src).poll_fill_buf(cx))?;
    if buffer.is_empty() {
        ready!(Pin::new(&mut dst).poll_flush(cx))?;
        ready!(Pin::new(&mut dst).poll_close(cx))?;
        return Poll::Ready(Ok(0));
    }

    let i = ready!(Pin::new(dst).poll_write(cx, buffer))?;
    if i == 0 {
        return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
    }
    Pin::new(src).consume(i);

    Poll::Ready(Ok(i.try_into().expect("usize to fit into u64.")))
}

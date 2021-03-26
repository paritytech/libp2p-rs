// Copyright 2020 Parity Technologies (UK) Ltd.
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
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub struct CopyFuture<S, D> {
    src: BufReader<S>,
    dst: BufReader<D>,

    active_timeout: Delay,
    configured_timeout: Duration,
}

impl<S: AsyncRead, D: AsyncRead> CopyFuture<S, D> {
    pub fn new(src: S, dst: D, timeout: Duration) -> Self {
        CopyFuture {
            src: BufReader::new(src),
            dst: BufReader::new(dst),
            active_timeout: Delay::new(timeout),
            configured_timeout: timeout,
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

        let mut reset_timer = false;

        loop {
            enum Status {
                Pending,
                Done,
                Progressed,
            }

            let src_status = match forward_data(&mut this.src, &mut this.dst, cx) {
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(true)) => Status::Done,
                Poll::Ready(Ok(false)) => Status::Progressed,
                Poll::Pending => Status::Pending,
            };

            let dst_status = match forward_data(&mut this.dst, &mut this.src, cx) {
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(true)) => Status::Done,
                Poll::Ready(Ok(false)) => Status::Progressed,
                Poll::Pending => Status::Pending,
            };

            match (src_status, dst_status) {
                // Both source and destination are done sending data.
                (Status::Done, Status::Done) => return Poll::Ready(Ok(())),
                // Either source or destination made progress, thus reset timer.
                (Status::Progressed, _) | (_, Status::Progressed) => reset_timer = true,
                // Both are pending. Check if timer fired, otherwise return Poll::Pending.
                (Status::Pending, Status::Pending) => break,
                // One is done sending data, the other is pending. Check if timer fired, otherwise
                // return Poll::Pending.
                (Status::Pending, Status::Done) | (Status::Done, Status::Pending) => break,
            }
        }

        if reset_timer {
            this.active_timeout = Delay::new(this.configured_timeout);
        }

        if let Poll::Ready(()) = this.active_timeout.poll_unpin(cx) {
            return Poll::Ready(Err(io::ErrorKind::TimedOut.into()));
        }

        Poll::Pending
    }
}

/// Forwards data from `source` to `destination`.
///
/// Returns `true` when done, i.e. `source` having reached EOF, returns false otherwise, thus
/// indicating progress.
fn forward_data<S: AsyncBufRead + Unpin, D: AsyncWrite + Unpin>(
    mut src: &mut S,
    mut dst: &mut D,
    cx: &mut Context<'_>,
) -> Poll<io::Result<bool>> {
    let buffer = ready!(Pin::new(&mut src).poll_fill_buf(cx))?;
    if buffer.is_empty() {
        ready!(Pin::new(&mut dst).poll_flush(cx))?;
        ready!(Pin::new(&mut dst).poll_close(cx))?;
        return Poll::Ready(Ok(true));
    }

    let i = ready!(Pin::new(dst).poll_write(cx, buffer))?;
    if i == 0 {
        return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
    }
    Pin::new(src).consume(i);

    Poll::Ready(Ok(false))
}

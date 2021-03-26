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

use super::{Provider, IfEvent, Incoming};

use async_io_crate::Async;
use futures::{
    future::{BoxFuture, FutureExt},
};
use std::io;
use std::task::{Poll, Context};
use std::net;

#[derive(Copy, Clone)]
pub enum Tcp {}

impl Provider for Tcp {
    type Stream = Async<net::TcpStream>;
    type Listener = Async<net::TcpListener>;
    type IfWatcher = if_watch::IfWatcher;

    fn if_watcher() -> BoxFuture<'static, io::Result<Self::IfWatcher>> {
        if_watch::IfWatcher::new().boxed()
    }

    fn new_listener(l: net::TcpListener) -> io::Result<Self::Listener> {
        Async::new(l)
    }

    fn new_stream(s: net::TcpStream) -> BoxFuture<'static, io::Result<Self::Stream>> {
        async move {
            let stream = Async::new(s)?;
            stream.writable().await?;
            Ok(stream)
        }.boxed()
    }

    fn poll_accept(l: &mut Self::Listener, cx: &mut Context<'_>) -> Poll<io::Result<Incoming<Self::Stream>>> {
        let (stream, remote_addr) = loop {
            match l.poll_readable(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => match l.accept().now_or_never() {
                    Some(Err(e)) => return Poll::Ready(Err(e)),
                    Some(Ok(res)) => break res,
                    None => {
                        // Since it doesn't do any harm, account for false positives of
                        // `poll_readable` just in case, i.e. try again.
                    }
                }
            }
        };

        let local_addr = stream.get_ref().local_addr()?;

        Poll::Ready(Ok(Incoming { stream, local_addr, remote_addr }))
    }

    fn poll_interfaces(w: &mut Self::IfWatcher, cx: &mut Context<'_>) -> Poll<io::Result<IfEvent>> {
        w.poll_unpin(cx).map_ok(|e| match e {
            if_watch::IfEvent::Up(a) => IfEvent::Up(a),
            if_watch::IfEvent::Down(a) => IfEvent::Down(a),
        })
    }
}

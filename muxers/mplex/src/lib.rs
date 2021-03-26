// Copyright 2018 Parity Technologies (UK) Ltd.
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

mod codec;
mod config;
mod io;

pub use config::{MplexConfig, MaxBufferBehaviour};

use codec::LocalStreamId;
use std::{cmp, iter, task::Context, task::Poll};
use bytes::Bytes;
use libp2p_core::{
    StreamMuxer,
    muxing::StreamMuxerEvent,
    upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeInfo},
};
use parking_lot::Mutex;
use futures::{prelude::*, future, ready};

impl UpgradeInfo for MplexConfig {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/mplex/6.7.0")
    }
}

impl<C> InboundUpgrade<C> for MplexConfig
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Multiplex<C>;
    type Error = io::Error;
    type Future = future::Ready<Result<Self::Output, io::Error>>;

    fn upgrade_inbound(self, socket: C, _: Self::Info) -> Self::Future {
        future::ready(Ok(Multiplex {
            io: Mutex::new(io::Multiplexed::new(socket, self)),
        }))
    }
}

impl<C> OutboundUpgrade<C> for MplexConfig
where
    C: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Multiplex<C>;
    type Error = io::Error;
    type Future = future::Ready<Result<Self::Output, io::Error>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        future::ready(Ok(Multiplex {
            io: Mutex::new(io::Multiplexed::new(socket, self))
        }))
    }
}

/// Multiplexer. Implements the `StreamMuxer` trait.
///
/// This implementation isn't capable of detecting when the underlying socket changes its address,
/// and no [`StreamMuxerEvent::AddressChange`] event is ever emitted.
pub struct Multiplex<C> {
    io: Mutex<io::Multiplexed<C>>
}

impl<C> StreamMuxer for Multiplex<C>
where
    C: AsyncRead + AsyncWrite + Unpin
{
    type Substream = Substream;
    type OutboundSubstream = OutboundSubstream;
    type Error = io::Error;

    fn poll_event(&self, cx: &mut Context<'_>)
        -> Poll<io::Result<StreamMuxerEvent<Self::Substream>>>
    {
        let stream_id = ready!(self.io.lock().poll_next_stream(cx))?;
        let stream = Substream::new(stream_id);
        Poll::Ready(Ok(StreamMuxerEvent::InboundSubstream(stream)))
    }

    fn open_outbound(&self) -> Self::OutboundSubstream {
        OutboundSubstream {}
    }

    fn poll_outbound(&self, cx: &mut Context<'_>, _: &mut Self::OutboundSubstream)
        -> Poll<Result<Self::Substream, io::Error>>
    {
        let stream_id = ready!(self.io.lock().poll_open_stream(cx))?;
        Poll::Ready(Ok(Substream::new(stream_id)))
    }

    fn destroy_outbound(&self, _substream: Self::OutboundSubstream) {
        // Nothing to do, since `open_outbound` creates no new local state.
    }

    fn read_substream(&self, cx: &mut Context<'_>, substream: &mut Self::Substream, buf: &mut [u8])
        -> Poll<Result<usize, io::Error>>
    {
        loop {
            // Try to read from the current (i.e. last received) frame.
            if !substream.current_data.is_empty() {
                let len = cmp::min(substream.current_data.len(), buf.len());
                buf[..len].copy_from_slice(&substream.current_data.split_to(len));
                return Poll::Ready(Ok(len));
            }

            // Read the next data frame from the multiplexed stream.
            match ready!(self.io.lock().poll_read_stream(cx, substream.id))? {
                Some(data) => { substream.current_data = data; }
                None => { return Poll::Ready(Ok(0)) }
            }
        }
    }

    fn write_substream(&self, cx: &mut Context<'_>, substream: &mut Self::Substream, buf: &[u8])
        -> Poll<Result<usize, io::Error>>
    {
        self.io.lock().poll_write_stream(cx, substream.id, buf)
    }

    fn flush_substream(&self, cx: &mut Context<'_>, substream: &mut Self::Substream)
        -> Poll<Result<(), io::Error>>
    {
        self.io.lock().poll_flush_stream(cx, substream.id)
    }

    fn shutdown_substream(&self, cx: &mut Context<'_>, substream: &mut Self::Substream)
        -> Poll<Result<(), io::Error>>
    {
        self.io.lock().poll_close_stream(cx, substream.id)
    }

    fn destroy_substream(&self, sub: Self::Substream) {
        self.io.lock().drop_stream(sub.id);
    }

    fn close(&self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.io.lock().poll_close(cx)
    }

    fn flush_all(&self, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.io.lock().poll_flush(cx)
    }
}

/// Active attempt to open an outbound substream.
pub struct OutboundSubstream {}

/// Active substream to the remote.
pub struct Substream {
    /// The unique, local identifier of the substream.
    id: LocalStreamId,
    /// The current data frame the substream is reading from.
    current_data: Bytes,
}

impl Substream {
    fn new(id: LocalStreamId) -> Self {
        Self { id, current_data: Bytes::new() }
    }
}

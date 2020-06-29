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

use crate::connection::{Connection, ConnectionEvent};
use crate::error::Error;

use libp2p_core::StreamMuxer;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    fmt,
    task::{Context, Poll, Waker},
};

/// State for a single opened QUIC connection.
// TODO: the inner `Mutex` should theoretically be a `futures::lock::Mutex` (or something similar),
// in order to sleep the current task if concurrent access to the connection is required
pub struct QuicMuxer {
    inner: Mutex<QuicMuxerInner>,
}

/// Mutex-protected fields of [`QuicMuxer`].
struct QuicMuxerInner {
    /// Inner connection object that yields events.
    connection: Connection,
    /// State of all the substreams that the muxer reports as open.
    substreams: HashMap<quinn_proto::StreamId, SubstreamState>,
    /// Waker to wake if a new outgoing substream is opened.
    poll_substream_opened_waker: Option<Waker>,
    /// Waker to wake if the connection is closed.
    poll_close_waker: Option<Waker>,
}

/// State of a single substream.
#[derive(Default)]
struct SubstreamState {
    /// Waker to wake if the substream becomes readable.
    read_waker: Option<Waker>,
    /// Waker to wake if the substream becomes writable.
    write_waker: Option<Waker>,
    /// Waker to wake if the substream becomes closed.
    finished_waker: Option<Waker>,
}

impl QuicMuxer {
    /// Crate-internal function that builds a [`QuicMuxer`] from a raw connection.
    ///
    /// # Panic
    ///
    /// Panics if `connection.is_handshaking()` returns `true`.
    pub(crate) fn from_connection(connection: Connection) -> Self {
        assert!(!connection.is_handshaking());

        QuicMuxer {
            inner: Mutex::new(QuicMuxerInner {
                connection,
                substreams: Default::default(),
                poll_substream_opened_waker: None,
                poll_close_waker: None,
            }),
        }
    }
}

impl StreamMuxer for QuicMuxer {
    type OutboundSubstream = ();
    type Substream = quinn_proto::StreamId;
    type Error = Error;

    fn poll_inbound(&self, cx: &mut Context<'_>) -> Poll<Result<Self::Substream, Self::Error>> {
        // We use `poll_inbound` to perform the background processing of the entire connection.
        let mut inner = self.inner.lock();
        tracing::trace!("poll_inbound called for side {:?}", inner.connection.side());

        while let Poll::Ready(event) = inner.connection.poll_event(cx) {
            match event {
                ConnectionEvent::Connected => {
                    log::error!("Unexpected Connected event on established QUIC connection");
                }
                ConnectionEvent::ConnectionLost(_) => {
                    if let Some(waker) = inner.poll_close_waker.take() {
                        waker.wake();
                    }
                }

                ConnectionEvent::StreamOpened => {
                    if let Some(waker) = inner.poll_substream_opened_waker.take() {
                        waker.wake();
                    }
                }
                ConnectionEvent::StreamReadable(substream) => {
                    if let Some(substream) = inner.substreams.get_mut(&substream) {
                        if let Some(waker) = substream.read_waker.take() {
                            waker.wake();
                        }
                    }
                }
                ConnectionEvent::StreamWritable(substream) => {
                    if let Some(substream) = inner.substreams.get_mut(&substream) {
                        if let Some(waker) = substream.write_waker.take() {
                            waker.wake();
                        }
                    }
                }
                ConnectionEvent::StreamFinished(substream) => {
                    if let Some(substream) = inner.substreams.get_mut(&substream) {
                        if let Some(waker) = substream.read_waker.take() {
                            waker.wake();
                        }
                        if let Some(waker) = substream.write_waker.take() {
                            waker.wake();
                        }
                        if let Some(waker) = substream.finished_waker.take() {
                            waker.wake();
                        }
                    }
                }

                // Do nothing as this is handled below.
                ConnectionEvent::StreamAvailable => {}
            }
        }

        if let Some(substream) = inner.connection.pop_incoming_substream() {
            inner.substreams.insert(substream, Default::default());
            Poll::Ready(Ok(substream))
        } else {
            Poll::Pending
        }
    }

    fn open_outbound(&self) -> Self::OutboundSubstream {}

    fn poll_outbound(
        &self,
        cx: &mut Context<'_>,
        _: &mut Self::OutboundSubstream,
    ) -> Poll<Result<Self::Substream, Self::Error>> {
        // Note that this implementation makes it possible to poll the same outbound substream
        // over and over again and get new substreams. Using the API this way is invalid and would
        // normally result in a panic, but we decide to just ignore this question.

        let mut inner = self.inner.lock();
        if let Some(substream) = inner.connection.pop_outgoing_substream() {
            inner.substreams.insert(substream, Default::default());
            return Poll::Ready(Ok(substream));
        }

        // Register `cx.waker()` as having to be woken up once a substream is available.
        if !inner
            .poll_substream_opened_waker
            .as_ref()
            .map_or(false, |w| w.will_wake(cx.waker()))
        {
            inner.poll_substream_opened_waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }

    fn destroy_outbound(&self, _: Self::OutboundSubstream) {}

    fn is_remote_acknowledged(&self) -> bool {
        // TODO: stub
        true
    }

    fn write_substream(
        &self,
        cx: &mut Context<'_>,
        substream: &mut Self::Substream,
        buf: &[u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut inner = self.inner.lock();

        match inner.connection.write_substream(*substream, buf) {
            Ok(bytes) => Poll::Ready(Ok(bytes)),
            Err(quinn_proto::WriteError::Stopped(_)) => Poll::Ready(Ok(0)), // EOF
            Err(quinn_proto::WriteError::Blocked) => {
                if let Some(substream) = inner.substreams.get_mut(substream) {
                    if !substream
                        .write_waker
                        .as_ref()
                        .map_or(false, |w| w.will_wake(cx.waker()))
                    {
                        substream.write_waker = Some(cx.waker().clone());
                    }
                }
                Poll::Pending
            }
            Err(quinn_proto::WriteError::UnknownStream) => {
                log::error!(
                    "The application used a connection that is already being \
                    closed. This is a bug in the application or in libp2p."
                );
                Poll::Pending
            }
        }
    }

    /// Try to read from a substream. This will return an error if the substream has
    /// not yet been written to.
    fn read_substream(
        &self,
        cx: &mut Context<'_>,
        substream: &mut Self::Substream,
        buf: &mut [u8],
    ) -> Poll<Result<usize, Self::Error>> {
        let mut inner = self.inner.lock();

        match inner.connection.read_substream(*substream, buf) {
            Ok(bytes) => Poll::Ready(Ok(bytes)),
            Err(quinn_proto::ReadError::Reset(_)) => Poll::Ready(Ok(0)), // EOF
            Err(quinn_proto::ReadError::Blocked) => {
                if let Some(substream) = inner.substreams.get_mut(substream) {
                    if !substream
                        .read_waker
                        .as_ref()
                        .map_or(false, |w| w.will_wake(cx.waker()))
                    {
                        substream.read_waker = Some(cx.waker().clone());
                    }
                }
                Poll::Pending
            }
            Err(quinn_proto::ReadError::UnknownStream) => {
                log::error!(
                    "The application used a connection that is already being \
                    closed. This is a bug in the application or in libp2p."
                );
                Poll::Pending
            }
        }
    }

    fn shutdown_substream(
        &self,
        _cx: &mut Context<'_>,
        substream: &mut Self::Substream,
    ) -> Poll<Result<(), Self::Error>> {
        let mut inner = self.inner.lock();

        // TODO: needs more work
        inner.connection.shutdown_substream(*substream);
        Poll::Ready(Ok(()))
    }

    fn destroy_substream(&self, substream: Self::Substream) {
        let mut inner = self.inner.lock();
        inner.substreams.remove(&substream);
    }

    fn flush_substream(
        &self,
        _cx: &mut Context<'_>,
        _substream: &mut Self::Substream,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn flush_all(&self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO: call poll_transmit() and stuff
        Poll::Ready(Ok(()))
    }

    fn close(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // StreamMuxer's `close` documentation mentions that it automatically implies `flush_all`.
        if let Poll::Pending = self.flush_all(cx)? {
            return Poll::Pending;
        }

        // TODO: poll if closed or something

        let mut inner = self.inner.lock();
        //self.connection.close();

        // Register `cx.waker()` as being woken up if the connection closes.
        if !inner
            .poll_close_waker
            .as_ref()
            .map_or(false, |w| w.will_wake(cx.waker()))
        {
            inner.poll_close_waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl fmt::Debug for QuicMuxer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_tuple("QuicMuxer").finish()
    }
}

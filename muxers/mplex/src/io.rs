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

use bytes::Bytes;
use crate::{MplexConfig, MaxBufferBehaviour};
use crate::codec::{Codec, Frame, LocalStreamId, RemoteStreamId};
use log::{debug, trace};
use futures::{prelude::*, ready, stream::Fuse};
use futures::task::{AtomicWaker, ArcWake, waker_ref, WakerRef};
use asynchronous_codec::Framed;
use nohash_hasher::{IntMap, IntSet};
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::collections::VecDeque;
use std::{cmp, fmt, io, mem, sync::Arc, task::{Context, Poll, Waker}};

pub use std::io::{Result, Error, ErrorKind};

/// A connection identifier.
///
/// Randomly generated and mainly intended to improve log output
/// by scoping substream IDs to a connection.
#[derive(Clone, Copy)]
struct ConnectionId(u64);

impl fmt::Debug for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:16x}", self.0)
    }
}

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:16x}", self.0)
    }
}
/// A multiplexed I/O stream.
pub struct Multiplexed<C> {
    /// A unique ID for the multiplexed stream (i.e. connection).
    id: ConnectionId,
    /// The current operating status of the multiplex stream.
    status: Status,
    /// The underlying multiplexed I/O stream.
    io: Fuse<Framed<C, Codec>>,
    /// The configuration.
    config: MplexConfig,
    /// The buffer of new inbound substreams that have not yet
    /// been drained by `poll_next_stream`. This buffer is
    /// effectively bounded by `max_substreams - substreams.len()`.
    open_buffer: VecDeque<LocalStreamId>,
    /// Whether a flush is pending due to one or more new outbound
    /// `Open` frames, before reading frames can proceed.
    pending_flush_open: IntSet<LocalStreamId>,
    /// The stream that currently blocks reading for all streams
    /// due to a full buffer, if any. Only applicable for use
    /// with [`MaxBufferBehaviour::Block`].
    blocking_stream: Option<LocalStreamId>,
    /// Pending frames to send at the next opportunity.
    ///
    /// An opportunity for sending pending frames is every flush
    /// or read operation. In the former case, sending of all
    /// pending frames must complete before the flush can complete.
    /// In the latter case, the read operation can proceed even
    /// if some or all of the pending frames cannot be sent.
    pending_frames: VecDeque<Frame<LocalStreamId>>,
    /// The managed substreams.
    substreams: IntMap<LocalStreamId, SubstreamState>,
    /// The ID for the next outbound substream.
    next_outbound_stream_id: LocalStreamId,
    /// Registry of wakers for pending tasks interested in reading.
    notifier_read: Arc<NotifierRead>,
    /// Registry of wakers for pending tasks interested in writing.
    notifier_write: Arc<NotifierWrite>,
    /// Registry of wakers for pending tasks interested in opening
    /// an outbound substream, when the configured limit is reached.
    ///
    /// As soon as the number of substreams drops below this limit,
    /// these tasks are woken.
    notifier_open: NotifierOpen,
}

/// The operation status of a `Multiplexed` I/O stream.
#[derive(Debug)]
enum Status {
    /// The stream is considered open and healthy.
    Open,
    /// The stream has been actively closed.
    Closed,
    /// The stream has encountered a fatal error.
    Err(io::Error),
}

impl<C> Multiplexed<C>
where
    C: AsyncRead + AsyncWrite + Unpin
{
    /// Creates a new multiplexed I/O stream.
    pub fn new(io: C, config: MplexConfig) -> Self {
        let id = ConnectionId(rand::random());
        debug!("New multiplexed connection: {}", id);
        Multiplexed {
            id,
            config,
            status: Status::Open,
            io: Framed::new(io, Codec::new()).fuse(),
            open_buffer: Default::default(),
            substreams: Default::default(),
            pending_flush_open: Default::default(),
            pending_frames: Default::default(),
            blocking_stream: None,
            next_outbound_stream_id: LocalStreamId::dialer(0),
            notifier_read: Arc::new(NotifierRead {
                read_stream: Mutex::new(Default::default()),
                next_stream: AtomicWaker::new(),
            }),
            notifier_write: Arc::new(NotifierWrite {
                pending: Mutex::new(Default::default()),
            }),
            notifier_open: NotifierOpen {
                pending: Default::default()
            }
        }
    }

    /// Flushes the underlying I/O stream.
    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &self.status {
            Status::Closed => return Poll::Ready(Ok(())),
            Status::Err(e) => return Poll::Ready(Err(io::Error::new(e.kind(), e.to_string()))),
            Status::Open => {}
        }

        // Send any pending frames.
        ready!(self.send_pending_frames(cx))?;

        // Flush the underlying I/O stream.
        let waker = NotifierWrite::register(&self.notifier_write, cx.waker());
        match ready!(self.io.poll_flush_unpin(&mut Context::from_waker(&waker))) {
            Err(e) => Poll::Ready(self.on_error(e)),
            Ok(()) => {
                self.pending_flush_open = Default::default();
                Poll::Ready(Ok(()))
            }
        }
    }

    /// Closes the underlying I/O stream.
    ///
    /// > **Note**: No `Close` or `Reset` frames are sent on open substreams
    /// > before closing the underlying connection. However, the connection
    /// > close implies a flush of any frames already sent.
    pub fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &self.status {
            Status::Closed => return Poll::Ready(Ok(())),
            Status::Err(e) => return Poll::Ready(Err(io::Error::new(e.kind(), e.to_string()))),
            Status::Open => {}
        }

        // Note: We do not make the effort to send pending `Reset` frames
        // here, we only close (and thus flush) the underlying I/O stream.

        let waker = NotifierWrite::register(&self.notifier_write, cx.waker());
        match self.io.poll_close_unpin(&mut Context::from_waker(&waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(self.on_error(e)),
            Poll::Ready(Ok(())) => {
                self.pending_frames = VecDeque::new();
                // We do not support read-after-close on the underlying
                // I/O stream, hence clearing the buffer and substreams.
                self.open_buffer = Default::default();
                self.substreams = Default::default();
                self.status = Status::Closed;
                Poll::Ready(Ok(()))
            }
        }
    }

    /// Waits for a new inbound substream, returning the corresponding `LocalStreamId`.
    ///
    /// If the number of already used substreams (i.e. substreams that have not
    /// yet been dropped via `drop_substream`) reaches the configured
    /// `max_substreams`, any further inbound substreams are immediately reset
    /// until existing substreams are dropped.
    ///
    /// Data frames read for existing substreams in the context of this
    /// method call are buffered and tasks interested in reading from
    /// these substreams are woken. If a substream buffer is full and
    /// [`MaxBufferBehaviour::Block`] is used, this method is blocked
    /// (i.e. `Pending`) on some task reading from the substream whose
    /// buffer is full.
    pub fn poll_next_stream(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<LocalStreamId>> {
        self.guard_open()?;

        // Try to read from the buffer first.
        if let Some(stream_id) = self.open_buffer.pop_back() {
            return Poll::Ready(Ok(stream_id));
        }

        debug_assert!(self.open_buffer.is_empty());
        let mut num_buffered = 0;

        loop {
            // Whenever we may have completely filled a substream
            // buffer while waiting for the next inbound stream,
            // yield to give the current task a chance to read
            // from the respective substreams.
            if num_buffered == self.config.max_buffer_len {
                cx.waker().clone().wake();
                return Poll::Pending
            }

            // Wait for the next inbound `Open` frame.
            match ready!(self.poll_read_frame(cx, None))? {
                Frame::Open { stream_id } => {
                    if let Some(id) = self.on_open(stream_id)? {
                        return Poll::Ready(Ok(id))
                    }
                }
                Frame::Data { stream_id, data } => {
                    self.buffer(stream_id.into_local(), data)?;
                    num_buffered += 1;
                }
                Frame::Close { stream_id } => {
                    self.on_close(stream_id.into_local());
                }
                Frame::Reset { stream_id } => {
                    self.on_reset(stream_id.into_local())
                }
            }
        }
    }

    /// Creates a new (outbound) substream, returning the allocated stream ID.
    pub fn poll_open_stream(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<LocalStreamId>> {
        self.guard_open()?;

        // Check the stream limits.
        if self.substreams.len() >= self.config.max_substreams {
            debug!("{}: Maximum number of substreams reached ({})",
                self.id, self.config.max_substreams);
            self.notifier_open.register(cx.waker());
            return Poll::Pending
        }

        // Send the `Open` frame.
        let waker = NotifierWrite::register(&self.notifier_write, cx.waker());
        match ready!(self.io.poll_ready_unpin(&mut Context::from_waker(&waker))) {
            Ok(()) => {
                let stream_id = self.next_outbound_stream_id();
                let frame = Frame::Open { stream_id };
                match self.io.start_send_unpin(frame) {
                    Ok(()) => {
                        self.substreams.insert(stream_id, SubstreamState::Open {
                            buf: Default::default()
                        });
                        debug!("{}: New outbound substream: {} (total {})",
                            self.id, stream_id, self.substreams.len());
                        // The flush is delayed and the `Open` frame may be sent
                        // together with other frames in the same transport packet.
                        self.pending_flush_open.insert(stream_id);
                        Poll::Ready(Ok(stream_id))
                    }
                    Err(e) => Poll::Ready(self.on_error(e)),
                }
            },
            Err(e) => Poll::Ready(self.on_error(e))
        }
    }

    /// Immediately drops a substream.
    ///
    /// All locally allocated resources for the dropped substream
    /// are freed and the substream becomes unavailable for both
    /// reading and writing immediately. The remote is informed
    /// based on the current state of the substream:
    ///
    /// * If the substream was open, a `Reset` frame is sent at
    ///   the next opportunity.
    /// * If the substream was half-closed, i.e. a `Close` frame
    ///   has already been sent, nothing further happens.
    /// * If the substream was half-closed by the remote, i.e.
    ///   a `Close` frame has already been received, a `Close`
    ///   frame is sent at the next opportunity.
    ///
    /// If the multiplexed stream is closed or encountered
    /// an error earlier, or there is no known substream with
    /// the given ID, this is a no-op.
    ///
    /// > **Note**: All substreams obtained via `poll_next_stream`
    /// > or `poll_open_stream` must eventually be "dropped" by
    /// > calling this method when they are no longer used.
    pub fn drop_stream(&mut self, id: LocalStreamId) {
        // Check if the underlying stream is ok.
        match self.status {
            Status::Closed | Status::Err(_) => return,
            Status::Open => {},
        }

        // If there is still a task waker interested in reading from that
        // stream, wake it to avoid leaving it dangling and notice that
        // the stream is gone. In contrast, wakers for write operations
        // are all woken on every new write opportunity.
        self.notifier_read.wake_read_stream(id);

        // Remove the substream, scheduling pending frames as necessary.
        match self.substreams.remove(&id) {
            None => {},
            Some(state) => {
                // If we fell below the substream limit, notify tasks that had
                // interest in opening an outbound substream earlier.
                let below_limit = self.substreams.len() == self.config.max_substreams - 1;
                if below_limit {
                    self.notifier_open.wake_all();
                }
                // Schedule any pending final frames to send, if necessary.
                match state {
                    SubstreamState::Closed { .. } => {}
                    SubstreamState::SendClosed { .. } => {}
                    SubstreamState::Reset { .. } => {}
                    SubstreamState::RecvClosed { .. } => {
                        if self.check_max_pending_frames().is_err() {
                            return
                        }
                        trace!("{}: Pending close for stream {}", self.id, id);
                        self.pending_frames.push_front(Frame::Close { stream_id: id });
                    }
                    SubstreamState::Open { .. } => {
                        if self.check_max_pending_frames().is_err() {
                            return
                        }
                        trace!("{}: Pending reset for stream {}", self.id, id);
                        self.pending_frames.push_front(Frame::Reset { stream_id: id });
                    }
                }
            }
        }
    }

    /// Writes data to a substream.
    pub fn poll_write_stream(&mut self, cx: &mut Context<'_>, id: LocalStreamId, buf: &[u8])
        -> Poll<io::Result<usize>>
    {
        self.guard_open()?;

        // Check if the stream is open for writing.
        match self.substreams.get(&id) {
            None | Some(SubstreamState::Reset { .. }) =>
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Some(SubstreamState::SendClosed { .. }) | Some(SubstreamState::Closed { .. }) =>
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
            Some(SubstreamState::Open { .. }) | Some(SubstreamState::RecvClosed { .. }) => {
                // Substream is writeable. Continue.
            }
        }

        // Determine the size of the frame to send.
        let frame_len = cmp::min(buf.len(), self.config.split_send_size);

        // Send the data frame.
        ready!(self.poll_send_frame(cx, || {
            let data = Bytes::copy_from_slice(&buf[.. frame_len]);
            Frame::Data { stream_id: id, data }
        }))?;

        Poll::Ready(Ok(frame_len))
    }

    /// Reads data from a substream.
    ///
    /// Data frames read for substreams other than `id` in the context
    /// of this method call are buffered and tasks interested in reading
    /// from these substreams are woken. If a substream buffer is full
    /// and [`MaxBufferBehaviour::Block`] is used, reading the next data
    /// frame for `id` is blocked on some task reading from the blocking
    /// stream's full buffer first.
    ///
    /// New inbound substreams (i.e. `Open` frames) read in the context of
    /// this method call are buffered up to the configured `max_substreams`
    /// and under consideration of the number of already used substreams,
    /// thereby waking the task that last called `poll_next_stream`, if any.
    /// Inbound substreams received in excess of that limit are immediately reset.
    pub fn poll_read_stream(&mut self, cx: &mut Context<'_>, id: LocalStreamId)
        -> Poll<io::Result<Option<Bytes>>>
    {
        self.guard_open()?;

        // Try to read from the buffer first.
        if let Some(state) = self.substreams.get_mut(&id) {
            let buf = state.recv_buf();
            if !buf.is_empty() {
                if self.blocking_stream == Some(id) {
                    // Unblock reading new frames.
                    self.blocking_stream = None;
                    ArcWake::wake_by_ref(&self.notifier_read);
                }
                let data = buf.remove(0);
                return Poll::Ready(Ok(Some(data)))
            }
            // If the stream buffer "spilled" onto the heap, free that memory.
            buf.shrink_to_fit();
        }

        let mut num_buffered = 0;

        loop {
            // Whenever we may have completely filled a substream
            // buffer of another substream while waiting for the
            // next frame for `id`, yield to give the current task
            // a chance to read from the other substream(s).
            if num_buffered == self.config.max_buffer_len {
                cx.waker().clone().wake();
                return Poll::Pending
            }

            // Check if the targeted substream (if any) reached EOF.
            if !self.can_read(&id) {
                // Note: Contrary to what is recommended by the spec, we must
                // return "EOF" also when the stream has been reset by the
                // remote, as the `StreamMuxer::read_substream` contract only
                // permits errors on "terminal" conditions, e.g. if the connection
                // has been closed or on protocol misbehaviour.
                return Poll::Ready(Ok(None))
            }

            // Read the next frame.
            match ready!(self.poll_read_frame(cx, Some(id)))? {
                Frame::Data { data, stream_id } if stream_id.into_local() == id => {
                    return Poll::Ready(Ok(Some(data)))
                },
                Frame::Data { stream_id, data } => {
                    // The data frame is for a different stream than the one
                    // currently being polled, so it needs to be buffered and
                    // the interested tasks notified.
                    self.buffer(stream_id.into_local(), data)?;
                    num_buffered += 1;
                }
                frame @ Frame::Open { .. } => {
                    if let Some(id) = self.on_open(frame.remote_id())? {
                        self.open_buffer.push_front(id);
                        trace!("{}: Buffered new inbound stream {} (total: {})", self.id, id, self.open_buffer.len());
                        self.notifier_read.wake_next_stream();
                    }
                }
                Frame::Close { stream_id } => {
                    let stream_id = stream_id.into_local();
                    self.on_close(stream_id);
                    if id == stream_id {
                        return Poll::Ready(Ok(None))
                    }
                }
                Frame::Reset { stream_id } => {
                    let stream_id = stream_id.into_local();
                    self.on_reset(stream_id);
                    if id == stream_id {
                        return Poll::Ready(Ok(None))
                    }
                }
            }
        }
    }

    /// Flushes a substream.
    ///
    /// > **Note**: This is equivalent to `poll_flush()`, i.e. to flushing
    /// > all substreams, except that this operation returns an error if
    /// > the underlying I/O stream is already closed.
    pub fn poll_flush_stream(&mut self, cx: &mut Context<'_>, id: LocalStreamId)
        -> Poll<io::Result<()>>
    {
        self.guard_open()?;

        ready!(self.poll_flush(cx))?;
        trace!("{}: Flushed substream {}", self.id, id);

        Poll::Ready(Ok(()))
    }

    /// Closes a stream for writing.
    ///
    /// > **Note**: As opposed to `poll_close()`, a flush it not implied.
    pub fn poll_close_stream(&mut self, cx: &mut Context<'_>, id: LocalStreamId)
        -> Poll<io::Result<()>>
    {
        self.guard_open()?;

        match self.substreams.remove(&id) {
            None => Poll::Ready(Ok(())),
            Some(SubstreamState::SendClosed { buf }) => {
                self.substreams.insert(id, SubstreamState::SendClosed { buf });
                Poll::Ready(Ok(()))
            }
            Some(SubstreamState::Closed { buf }) => {
                self.substreams.insert(id, SubstreamState::Closed { buf });
                Poll::Ready(Ok(()))
            }
            Some(SubstreamState::Reset { buf }) => {
                self.substreams.insert(id, SubstreamState::Reset { buf });
                Poll::Ready(Ok(()))
            }
            Some(SubstreamState::Open { buf }) => {
                if self.poll_send_frame(cx, || Frame::Close { stream_id: id })?.is_pending() {
                    self.substreams.insert(id, SubstreamState::Open { buf });
                    Poll::Pending
                } else {
                    debug!("{}: Closed substream {} (half-close)", self.id, id);
                    self.substreams.insert(id, SubstreamState::SendClosed { buf });
                    Poll::Ready(Ok(()))
                }
            }
            Some(SubstreamState::RecvClosed { buf }) => {
                if self.poll_send_frame(cx, || Frame::Close { stream_id: id })?.is_pending() {
                    self.substreams.insert(id, SubstreamState::RecvClosed { buf });
                    Poll::Pending
                } else {
                    debug!("{}: Closed substream {}", self.id, id);
                    self.substreams.insert(id, SubstreamState::Closed { buf });
                    Poll::Ready(Ok(()))
                }
            }
        }
    }

    /// Sends a (lazily constructed) mplex frame on the underlying I/O stream.
    ///
    /// The frame is only constructed if the underlying sink is ready to
    /// send another frame.
    fn poll_send_frame<F>(&mut self, cx: &mut Context<'_>, frame: F)
        -> Poll<io::Result<()>>
    where
        F: FnOnce() -> Frame<LocalStreamId>
    {
        let waker = NotifierWrite::register(&self.notifier_write, cx.waker());
        match ready!(self.io.poll_ready_unpin(&mut Context::from_waker(&waker))) {
            Ok(()) => {
                let frame = frame();
                trace!("{}: Sending {:?}", self.id, frame);
                match self.io.start_send_unpin(frame) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(self.on_error(e))
                }
            },
            Err(e) => Poll::Ready(self.on_error(e))
        }
    }

    /// Reads the next frame from the underlying I/O stream.
    ///
    /// The given `stream_id` identifies the substream in which
    /// the current task is interested and wants to be woken up for,
    /// in case new frames can be read. `None` means interest in
    /// frames for any substream.
    fn poll_read_frame(&mut self, cx: &mut Context<'_>, stream_id: Option<LocalStreamId>)
        -> Poll<io::Result<Frame<RemoteStreamId>>>
    {
        // Try to send pending frames, if there are any, without blocking,
        if let Poll::Ready(Err(e)) = self.send_pending_frames(cx) {
            return Poll::Ready(Err(e))
        }

        // Perform any pending flush before reading.
        if let Some(id) = &stream_id {
            if self.pending_flush_open.contains(id) {
                trace!("{}: Executing pending flush for {}.", self.id, id);
                ready!(self.poll_flush(cx))?;
                self.pending_flush_open = Default::default();
            }
        }

        // Check if there is a blocked stream.
        if let Some(blocked_id) = &self.blocking_stream {
            // We have a blocked stream and cannot continue reading
            // new frames until frames are taken from the blocked stream's
            // buffer.

            // Try to wake a pending reader of the blocked stream.
            if !self.notifier_read.wake_read_stream(*blocked_id) {
                // No task dedicated to the blocked stream woken, so schedule
                // this task again to have a chance at progress.
                trace!("{}: No task to read from blocked stream. Waking current task.", self.id);
                cx.waker().clone().wake();
            } else if let Some(id) = stream_id {
                // We woke some other task, but are still interested in
                // reading `Data` frames from the current stream when unblocked.
                debug_assert!(blocked_id != &id, "Unexpected attempt at reading a new \
                    frame from a substream with a full buffer.");
                let _ = NotifierRead::register_read_stream(&self.notifier_read, cx.waker(), id);
            } else {
                // We woke some other task but are still interested in
                // reading new `Open` frames when unblocked.
                let _ = NotifierRead::register_next_stream(&self.notifier_read, cx.waker());
            }

            return Poll::Pending
        }

        // Try to read another frame from the underlying I/O stream.
        let waker = match stream_id {
            Some(id) => NotifierRead::register_read_stream(&self.notifier_read, cx.waker(), id),
            None => NotifierRead::register_next_stream(&self.notifier_read, cx.waker())
        };
        match ready!(self.io.poll_next_unpin(&mut Context::from_waker(&waker))) {
            Some(Ok(frame)) => {
                trace!("{}: Received {:?}", self.id, frame);
                Poll::Ready(Ok(frame))
            }
            Some(Err(e)) => Poll::Ready(self.on_error(e)),
            None => Poll::Ready(self.on_error(io::ErrorKind::UnexpectedEof.into()))
        }
    }

    /// Processes an inbound `Open` frame.
    fn on_open(&mut self, id: RemoteStreamId) -> io::Result<Option<LocalStreamId>> {
        let id = id.into_local();

        if self.substreams.contains_key(&id) {
            debug!("{}: Received unexpected `Open` frame for open substream {}", self.id, id);
            return self.on_error(io::Error::new(io::ErrorKind::Other,
                "Protocol error: Received `Open` frame for open substream."))
        }

        if self.substreams.len() >= self.config.max_substreams {
            debug!("{}: Maximum number of substreams exceeded: {}",
                self.id, self.config.max_substreams);
            self.check_max_pending_frames()?;
            debug!("{}: Pending reset for new stream {}", self.id, id);
            self.pending_frames.push_front(Frame::Reset {
                stream_id: id
            });
            return Ok(None)
        }

        self.substreams.insert(id, SubstreamState::Open {
            buf: Default::default()
        });

        debug!("{}: New inbound substream: {} (total {})", self.id, id, self.substreams.len());

        Ok(Some(id))
    }

    /// Processes an inbound `Reset` frame.
    fn on_reset(&mut self, id: LocalStreamId) {
        if let Some(state) = self.substreams.remove(&id) {
            match state {
                SubstreamState::Closed { .. } => {
                    trace!("{}: Ignoring reset for mutually closed substream {}.", self.id, id);
                }
                SubstreamState::Reset { .. } => {
                    trace!("{}: Ignoring redundant reset for already reset substream {}",
                        self.id, id);
                }
                SubstreamState::RecvClosed { buf } |
                SubstreamState::SendClosed { buf } |
                SubstreamState::Open { buf } => {
                    debug!("{}: Substream {} reset by remote.", self.id, id);
                    self.substreams.insert(id, SubstreamState::Reset { buf });
                    // Notify tasks interested in reading from that stream,
                    // so they may read the EOF.
                    NotifierRead::wake_read_stream(&self.notifier_read, id);
                }
            }
        } else {
            trace!("{}: Ignoring `Reset` for unknown substream {}. Possibly dropped earlier.",
                self.id, id);
        }
    }

    /// Processes an inbound `Close` frame.
    fn on_close(&mut self, id: LocalStreamId) {
        if let Some(state) = self.substreams.remove(&id) {
            match state {
                SubstreamState::RecvClosed { .. } | SubstreamState::Closed { .. } => {
                    debug!("{}: Ignoring `Close` frame for closed substream {}",
                        self.id, id);
                    self.substreams.insert(id, state);
                },
                SubstreamState::Reset { buf } => {
                    debug!("{}: Ignoring `Close` frame for already reset substream {}",
                        self.id, id);
                    self.substreams.insert(id, SubstreamState::Reset { buf });
                }
                SubstreamState::SendClosed { buf } => {
                    debug!("{}: Substream {} closed by remote (SendClosed -> Closed).",
                        self.id, id);
                    self.substreams.insert(id, SubstreamState::Closed { buf });
                    // Notify tasks interested in reading, so they may read the EOF.
                    self.notifier_read.wake_read_stream(id);
                },
                SubstreamState::Open { buf } => {
                    debug!("{}: Substream {} closed by remote (Open -> RecvClosed)",
                        self.id, id);
                    self.substreams.insert(id, SubstreamState::RecvClosed { buf });
                    // Notify tasks interested in reading, so they may read the EOF.
                    self.notifier_read.wake_read_stream(id);
                },
            }
        } else {
            trace!("{}: Ignoring `Close` for unknown substream {}. Possibly dropped earlier.",
                self.id, id);
        }
    }

    /// Generates the next outbound stream ID.
    fn next_outbound_stream_id(&mut self) -> LocalStreamId {
        let id = self.next_outbound_stream_id;
        self.next_outbound_stream_id = self.next_outbound_stream_id.next();
        id
    }

    /// Checks whether a substream is open for reading.
    fn can_read(&self, id: &LocalStreamId) -> bool {
        match self.substreams.get(id) {
            Some(SubstreamState::Open { .. }) | Some(SubstreamState::SendClosed { .. }) => true,
            _ => false,
        }
    }

    /// Sends pending frames, without flushing.
    fn send_pending_frames(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while let Some(frame) = self.pending_frames.pop_back() {
            if self.poll_send_frame(cx, || {
                frame.clone()
            })?.is_pending() {
                self.pending_frames.push_back(frame);
                return Poll::Pending
            }
        }

        Poll::Ready(Ok(()))
    }

    /// Records a fatal error for the multiplexed I/O stream.
    fn on_error<T>(&mut self, e: io::Error) -> io::Result<T> {
        debug!("{}: Multiplexed connection failed: {:?}", self.id, e);
        self.status = Status::Err(io::Error::new(e.kind(), e.to_string()));
        self.pending_frames =  Default::default();
        self.substreams = Default::default();
        self.open_buffer = Default::default();
        Err(e)
    }

    /// Checks that the multiplexed stream has status `Ok`,
    /// i.e. is not closed and did not encounter a fatal error.
    fn guard_open(&self) -> io::Result<()> {
        match &self.status {
            Status::Closed => Err(io::Error::new(io::ErrorKind::Other, "Connection is closed")),
            Status::Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
            Status::Open => Ok(())
        }
    }

    /// Checks that the permissible limit for pending outgoing frames
    /// has not been reached.
    fn check_max_pending_frames(&mut self) -> io::Result<()> {
        if self.pending_frames.len() >= self.config.max_substreams + EXTRA_PENDING_FRAMES {
            return self.on_error(io::Error::new(io::ErrorKind::Other,
                "Too many pending frames."));
        }
        Ok(())
    }

    /// Buffers a data frame for a particular substream, if possible.
    ///
    /// If the new data frame exceeds the `max_buffer_len` for the buffer
    /// of the substream, the behaviour depends on the configured
    /// [`MaxBufferBehaviour`]. Note that the excess frame is still
    /// buffered in that case (but no further frames will be).
    ///
    /// Fails the entire multiplexed stream if too many pending `Reset`
    /// frames accumulate when using [`MaxBufferBehaviour::ResetStream`].
    fn buffer(&mut self, id: LocalStreamId, data: Bytes) -> io::Result<()> {
        let state = if let Some(state) = self.substreams.get_mut(&id) {
            state
        } else {
            trace!("{}: Dropping data {:?} for unknown substream {}", self.id, data, id);
            return Ok(())
        };

        let buf = if let Some(buf) = state.recv_buf_open() {
            buf
        } else {
            trace!("{}: Dropping data {:?} for closed or reset substream {}", self.id, data, id);
            return Ok(())
        };

        debug_assert!(buf.len() <= self.config.max_buffer_len);
        trace!("{}: Buffering {:?} for stream {} (total: {})", self.id, data, id, buf.len() + 1);
        buf.push(data);
        self.notifier_read.wake_read_stream(id);
        if buf.len() > self.config.max_buffer_len {
            debug!("{}: Frame buffer of stream {} is full.", self.id, id);
            match self.config.max_buffer_behaviour {
                MaxBufferBehaviour::ResetStream => {
                    let buf = buf.clone();
                    self.check_max_pending_frames()?;
                    self.substreams.insert(id, SubstreamState::Reset { buf });
                    debug!("{}: Pending reset for stream {}", self.id, id);
                    self.pending_frames.push_front(Frame::Reset {
                        stream_id: id
                    });
                }
                MaxBufferBehaviour::Block => {
                    self.blocking_stream = Some(id);
                }
            }
        }

        Ok(())
    }
}

type RecvBuf = SmallVec<[Bytes; 10]>;

/// The operating states of a substream.
#[derive(Clone, Debug)]
enum SubstreamState {
    /// An `Open` frame has been received or sent.
    Open { buf: RecvBuf },
    /// A `Close` frame has been sent, but the stream is still open
    /// for reading (half-close).
    SendClosed { buf: RecvBuf },
    /// A `Close` frame has been received but the stream is still
    /// open for writing (remote half-close).
    RecvClosed { buf: RecvBuf },
    /// A `Close` frame has been sent and received but the stream
    /// has not yet been dropped and may still have buffered
    /// frames to read.
    Closed { buf: RecvBuf },
    /// The stream has been reset by the local or remote peer but has
    /// not yet been dropped and may still have buffered frames to read.
    Reset { buf: RecvBuf }
}

impl SubstreamState {
    /// Mutably borrows the substream's receive buffer.
    fn recv_buf(&mut self) -> &mut RecvBuf {
        match self {
            SubstreamState::Open { buf } => buf,
            SubstreamState::SendClosed { buf } => buf,
            SubstreamState::RecvClosed { buf } => buf,
            SubstreamState::Closed { buf } => buf,
            SubstreamState::Reset { buf } => buf,
        }
    }

    /// Mutably borrows the substream's receive buffer if the substream
    /// is still open for reading, `None` otherwise.
    fn recv_buf_open(&mut self) -> Option<&mut RecvBuf> {
        match self {
            SubstreamState::Open { buf } => Some(buf),
            SubstreamState::SendClosed { buf } => Some(buf),
            SubstreamState::RecvClosed { .. } => None,
            SubstreamState::Closed { .. } => None,
            SubstreamState::Reset { .. } => None,
        }
    }
}

struct NotifierRead {
    /// The waker of the currently pending task that last
    /// called `poll_next_stream`, if any.
    next_stream: AtomicWaker,
    /// The wakers of currently pending tasks that last
    /// called `poll_read_stream` for a particular substream.
    read_stream: Mutex<IntMap<LocalStreamId, Waker>>,
}

impl NotifierRead {
    /// Registers a task to be woken up when new `Data` frames for a particular
    /// stream can be read.
    ///
    /// The returned waker should be passed to an I/O read operation
    /// that schedules a wakeup, if the operation is pending.
    #[must_use]
    fn register_read_stream<'a>(self: &'a Arc<Self>, waker: &Waker, id: LocalStreamId)
        -> WakerRef<'a>
    {
        let mut pending = self.read_stream.lock();
        pending.insert(id, waker.clone());
        waker_ref(self)
    }

    /// Registers a task to be woken up when new `Open` frames can be read.
    ///
    /// The returned waker should be passed to an I/O read operation
    /// that schedules a wakeup, if the operation is pending.
    #[must_use]
    fn register_next_stream<'a>(self: &'a Arc<Self>, waker: &Waker) -> WakerRef<'a> {
        self.next_stream.register(waker);
        waker_ref(self)
    }

    /// Wakes the task pending on `poll_read_stream` for the
    /// specified stream, if any.
    fn wake_read_stream(&self, id: LocalStreamId) -> bool {
        let mut pending = self.read_stream.lock();

        if let Some(waker) = pending.remove(&id) {
            waker.wake();
            return true
        }

        false
    }

    /// Wakes the task pending on `poll_next_stream`, if any.
    fn wake_next_stream(&self) {
        self.next_stream.wake();
    }
}

impl ArcWake for NotifierRead {
    fn wake_by_ref(this: &Arc<Self>) {
        let wakers = mem::take(&mut *this.read_stream.lock());
        for (_, waker) in wakers {
            waker.wake();
        }
        this.wake_next_stream();
    }
}

struct NotifierWrite {
    /// List of wakers to wake when write operations on the
    /// underlying I/O stream can proceed.
    pending: Mutex<Vec<Waker>>,
}

impl NotifierWrite {
    /// Registers interest of a task in writing to some substream.
    ///
    /// The returned waker should be passed to an I/O write operation
    /// that schedules a wakeup if the operation is pending.
    #[must_use]
    fn register<'a>(self: &'a Arc<Self>, waker: &Waker) -> WakerRef<'a> {
        let mut pending = self.pending.lock();
        if pending.iter().all(|w| !w.will_wake(waker)) {
            pending.push(waker.clone());
        }
        waker_ref(self)
    }
}

impl ArcWake for NotifierWrite {
    fn wake_by_ref(this: &Arc<Self>) {
        let wakers = mem::take(&mut *this.pending.lock());
        for waker in wakers {
            waker.wake();
        }
    }
}

struct NotifierOpen {
    /// Wakers of pending tasks interested in creating new
    /// outbound substreams.
    pending: Vec<Waker>,
}

impl NotifierOpen {
    /// Registers interest of a task in opening a new outbound substream.
    fn register(&mut self, waker: &Waker) {
        if self.pending.iter().all(|w| !w.will_wake(waker)) {
            self.pending.push(waker.clone());
        }
    }

    fn wake_all(&mut self) {
        let wakers = mem::take(&mut self.pending);
        for waker in wakers {
            waker.wake();
        }
    }
}

/// The maximum number of pending reset or close frames to send
/// we are willing to buffer beyond the configured substream limit.
/// This extra leeway bounds resource usage while allowing some
/// back-pressure when sending out these frames.
///
/// If too many pending frames accumulate, the multiplexed stream is
/// considered unhealthy and terminates with an error.
const EXTRA_PENDING_FRAMES: usize = 1000;

#[cfg(test)]
mod tests {
    use async_std::task;
    use bytes::BytesMut;
    use futures::prelude::*;
    use asynchronous_codec::{Decoder, Encoder};
    use quickcheck::*;
    use rand::prelude::*;
    use std::collections::HashSet;
    use std::num::NonZeroU8;
    use std::ops::DerefMut;
    use std::pin::Pin;
    use super::*;

    impl Arbitrary for MaxBufferBehaviour {
        fn arbitrary<G: Gen>(g: &mut G) -> MaxBufferBehaviour {
            *[MaxBufferBehaviour::Block, MaxBufferBehaviour::ResetStream].choose(g).unwrap()
        }
    }

    impl Arbitrary for MplexConfig {
        fn arbitrary<G: Gen>(g: &mut G) -> MplexConfig {
            MplexConfig {
                max_substreams: g.gen_range(1, 100),
                max_buffer_len: g.gen_range(1, 1000),
                max_buffer_behaviour: MaxBufferBehaviour::arbitrary(g),
                split_send_size: g.gen_range(1, 10000),
            }
        }
    }

    /// Memory-backed "connection".
    struct Connection {
        /// The buffer that the `Multiplexed` stream reads from.
        r_buf: BytesMut,
        /// The buffer that the `Multiplexed` stream writes to.
        w_buf: BytesMut,
        /// Whether the connection should return EOF on the next read.
        eof: bool,
    }

    impl AsyncRead for Connection {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut [u8]
        ) -> Poll<io::Result<usize>> {
            if self.eof {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()))
            }
            let n = std::cmp::min(buf.len(), self.r_buf.len());
            let data = self.r_buf.split_to(n);
            buf[..n].copy_from_slice(&data[..]);
            if n == 0 {
                Poll::Pending
            } else {
                Poll::Ready(Ok(n))
            }
        }
    }

    impl AsyncWrite for Connection {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8]
        ) -> Poll<io::Result<usize>> {
            self.w_buf.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _: &mut Context<'_>
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _: &mut Context<'_>
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn max_buffer_behaviour() {
        let _ = env_logger::try_init();

        fn prop(cfg: MplexConfig, overflow: NonZeroU8) {
            let mut r_buf = BytesMut::new();
            let mut codec = Codec::new();

            // Open the maximum number of inbound streams.
            for i in 0 .. cfg.max_substreams {
                let stream_id = LocalStreamId::dialer(i as u32);
                codec.encode(Frame::Open { stream_id  }, &mut r_buf).unwrap();
            }

            // Send more data on stream 0 than the buffer permits.
            let stream_id = LocalStreamId::dialer(0);
            let data = Bytes::from("Hello world");
            for _ in 0 .. cfg.max_buffer_len + overflow.get() as usize {
                codec.encode(Frame::Data { stream_id, data: data.clone() }, &mut r_buf).unwrap();
            }

            // Setup the multiplexed connection.
            let conn = Connection { r_buf, w_buf: BytesMut::new(), eof: false };
            let mut m = Multiplexed::new(conn, cfg.clone());

            task::block_on(future::poll_fn(move |cx| {
                // Receive all inbound streams.
                for i in 0 .. cfg.max_substreams {
                    match m.poll_next_stream(cx) {
                        Poll::Pending => panic!("Expected new inbound stream."),
                        Poll::Ready(Err(e)) => panic!("{:?}", e),
                        Poll::Ready(Ok(id)) => {
                            assert_eq!(id, LocalStreamId::listener(i as u32));
                        }
                    };
                }

                // Polling again for an inbound stream should yield `Pending`
                // after reading and buffering data frames up to the limit.
                let id = LocalStreamId::listener(0);
                match m.poll_next_stream(cx) {
                    Poll::Ready(r) => panic!("Unexpected result for next stream: {:?}", r),
                    Poll::Pending => {
                        // We expect the implementation to yield when the buffer
                        // is full but before it is exceeded and the max buffer
                        // behaviour takes effect, giving the current task a
                        // chance to read from the buffer. Here we just read
                        // again to provoke the max buffer behaviour.
                        assert_eq!(
                            m.substreams.get_mut(&id).unwrap().recv_buf().len(),
                            cfg.max_buffer_len
                        );
                        match m.poll_next_stream(cx) {
                            Poll::Ready(r) => panic!("Unexpected result for next stream: {:?}", r),
                            Poll::Pending => {
                                // Expect the buffer for stream 0 to be exceeded, triggering
                                // the max. buffer behaviour.
                                assert_eq!(
                                    m.substreams.get_mut(&id).unwrap().recv_buf().len(),
                                    cfg.max_buffer_len + 1
                                );
                            }
                        }
                    }
                }

                // Expect either a `Reset` to be sent or all reads to be
                // blocked `Pending`, depending on the `MaxBufferBehaviour`.
                match cfg.max_buffer_behaviour {
                    MaxBufferBehaviour::ResetStream => {
                        let _ = m.poll_flush_stream(cx, id);
                        let w_buf = &mut m.io.get_mut().deref_mut().w_buf;
                        let frame = codec.decode(w_buf).unwrap();
                        let stream_id = stream_id.into_remote();
                        assert_eq!(frame, Some(Frame::Reset { stream_id }));
                    }
                    MaxBufferBehaviour::Block => {
                        assert!(m.poll_next_stream(cx).is_pending());
                        for i in 1 .. cfg.max_substreams {
                            let id = LocalStreamId::listener(i as u32);
                            assert!(m.poll_read_stream(cx, id).is_pending());
                        }
                    }
                }

                // Drain the buffer by reading from the stream.
                for _ in 0 .. cfg.max_buffer_len + 1 {
                    match m.poll_read_stream(cx, id) {
                        Poll::Ready(Ok(Some(bytes))) => {
                            assert_eq!(bytes, data);
                        }
                        x => panic!("Unexpected: {:?}", x)
                    }
                }

                // Read from the stream after the buffer has been drained,
                // expecting either EOF or further data, depending on
                // the `MaxBufferBehaviour`.
                match cfg.max_buffer_behaviour {
                    MaxBufferBehaviour::ResetStream => {
                        // Expect to read EOF
                        match m.poll_read_stream(cx, id) {
                            Poll::Ready(Ok(None)) => {},
                            poll => panic!("Unexpected: {:?}", poll)
                        }
                    }
                    MaxBufferBehaviour::Block => {
                        // Expect to be able to continue reading.
                        match m.poll_read_stream(cx, id) {
                            Poll::Ready(Ok(Some(bytes))) => assert_eq!(bytes, data),
                            Poll::Pending => assert_eq!(overflow.get(), 1),
                            poll => panic!("Unexpected: {:?}", poll)
                        }
                    }
                }

                Poll::Ready(())
            }));
        }

        quickcheck(prop as fn(_,_))
    }

    #[test]
    fn close_on_error() {
        let _ = env_logger::try_init();

        fn prop(cfg: MplexConfig, num_streams: NonZeroU8) {
            let num_streams = cmp::min(cfg.max_substreams, num_streams.get() as usize);

            // Setup the multiplexed connection.
            let conn = Connection {
                r_buf: BytesMut::new(),
                w_buf: BytesMut::new(),
                eof: false
            };
            let mut m = Multiplexed::new(conn, cfg.clone());

            // Run the test.
            let mut opened = HashSet::new();
            task::block_on(future::poll_fn(move |cx| {
                // Open a number of streams.
                for _ in 0 .. num_streams {
                    let id = ready!(m.poll_open_stream(cx)).unwrap();
                    assert!(opened.insert(id));
                    assert!(m.poll_read_stream(cx, id).is_pending());
                }

                // Abruptly "close" the connection.
                m.io.get_mut().deref_mut().eof = true;

                // Reading from any stream should yield an error and all streams
                // should be closed due to the failed connection.
                assert!(opened.iter().all(|id| match m.poll_read_stream(cx, *id) {
                    Poll::Ready(Err(e)) => e.kind() == io::ErrorKind::UnexpectedEof,
                    _ => false
                }));

                assert!(m.substreams.is_empty());

                Poll::Ready(())
            }))
        }

        quickcheck(prop as fn(_,_))
    }
}

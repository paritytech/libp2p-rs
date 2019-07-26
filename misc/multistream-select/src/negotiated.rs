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

use bytes::BytesMut;
use crate::protocol::{Protocol, MessageReader, Message, Version, ProtocolError};
use futures::{prelude::*, Async, try_ready};
use log::debug;
use tokio_io::{AsyncRead, AsyncWrite};
use std::{mem, io, fmt, error::Error};

/// An I/O stream that has settled on an (application-layer) protocol to use.
///
/// A `Negotiated` represents an I/O stream that has _settled_ on a protocol
/// to use. In particular, it does not imply that all of the protocol negotiation
/// frames have been sent and / or received, just that the selected protocol
/// is fully determined. This is to allow the last protocol negotiation frames
/// sent by a peer to be combined in a single write, possibly piggy-backing
/// data from the negotiated protocol on top.
///
/// Specifically that means:
///
///   * If a `Negotiated` is obtained by the peer with the role of the dialer in
///     the protocol negotiation, not a single negotiation message may yet have
///     been sent, if the dialer only supports a single protocol. In that case,
///     the dialer "settles" on that protocol immediately and expects it to
///     be confirmed by the remote, as it has no alternatives. Once the
///     `Negotiated` I/O resource is flushed, possibly after writing additional
///     data related to the negotiated protocol, all of the buffered frames relating to
///     protocol selection are sent together with that data. The dialer still expects
///     to receive acknowledgment of the protocol before it can continue reading data
///     from the remote related to the negotiated protocol.
///     The `Negotiated` stream may ultimately still fail protocol negotiation, if
///     the protocol that the dialer has settled on is not actually supported
///     by the listener, but having settled on that protocol the dialer has by
///     definition no more alternatives and hence such a failed negotiation is
///     usually equivalent to a failed request made using the desired protocol.
///     If an application wishes to only start using the `Negotiated` stream
///     once protocol negotiation fully completed, it may wait on completion
///     of the `Future` obtained from [`Negotiated::complete`].
///
///  * If a `Negotiated` is obtained by the peer with the role of the listener in
///    the protocol negotiation, the final confirmation message for the remote's
///    selected protocol may not yet have been sent. Once the `Negotiated` I/O
///    resource is flushed, possibly after writing additional data related to the
///    negotiated protocol, e.g. a response, the buffered frames relating to protocol
///    acknowledgement are sent together with that data.
///
pub struct Negotiated<TInner> {
    state: State<TInner>
}

/// A `Future` that waits on the completion of protocol negotiation.
pub struct NegotiatedComplete<TInner> {
    inner: Option<Negotiated<TInner>>
}

impl<TInner: AsyncRead + AsyncWrite> Future for NegotiatedComplete<TInner> {
    type Item = Negotiated<TInner>;
    type Error = NegotiationError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        try_ready!(self.inner.as_mut()
            .expect("NegotiatedFuture called after completion.")
            .poll());
        Ok(Async::Ready(self.inner.take().expect("")))
    }
}

impl<TInner> Negotiated<TInner> {
    /// Creates a `Negotiated` in state [`State::Complete`], possibly
    /// with `remaining` data to be sent.
    pub(crate) fn completed(io: TInner, remaining: BytesMut) -> Self {
        Negotiated { state: State::Completed { io, remaining } }
    }

    /// Creates a `Negotiated` in state [`State::Expecting`] that is still
    /// expecting confirmation of the given `protocol`.
    pub(crate) fn expecting(io: MessageReader<TInner>, protocol: Protocol) -> Self {
        Negotiated { state: State::Expecting { io, protocol } }
    }

    /// Polls the `Negotiated` for completion.
    fn poll(&mut self) -> Poll<(), NegotiationError>
    where
        TInner: AsyncRead + AsyncWrite
    {
        // Flush any pending negotiation data.
        match self.poll_flush() {
            Ok(Async::Ready(())) => {},
            Ok(Async::NotReady) => return Ok(Async::NotReady),
            Err(e) => {
                // If the remote closed the stream, it is important to still
                // continue reading the data that was sent, if any.
                if e.kind() != io::ErrorKind::WriteZero {
                    return Err(e.into())
                }
            }
        }

        if let State::Completed { remaining, .. } = &mut self.state {
            let _ = remaining.take(); // Drop remaining data flushed above.
            return Ok(Async::Ready(()))
        }

        // Read outstanding protocol negotiation messages.
        loop {
            match mem::replace(&mut self.state, State::Invalid) {
                State::Expecting { mut io, protocol } => {
                    let msg = match io.poll() {
                        Ok(Async::Ready(Some(msg))) => msg,
                        Ok(Async::NotReady) => {
                            self.state = State::Expecting { io, protocol };
                            return Ok(Async::NotReady)
                        }
                        Ok(Async::Ready(None)) => {
                            self.state = State::Expecting { io, protocol };
                            return Err(ProtocolError::IoError(
                                io::ErrorKind::UnexpectedEof.into()).into())
                        }
                        Err(err) => {
                            self.state = State::Expecting { io, protocol };
                            return Err(err.into())
                        }
                    };

                    if let Message::Header(Version::V1) = &msg {
                        self.state = State::Expecting { io, protocol };
                        continue
                    }

                    if let Message::Protocol(p) = &msg {
                        if p.as_ref() == protocol.as_ref() {
                            debug!("Negotiated: Received confirmation for protocol: {}", p);
                            let (io, remaining) = io.into_inner();
                            self.state = State::Completed { io, remaining };
                            return Ok(Async::Ready(()))
                        }
                    }

                    return Err(NegotiationError::Failed)
                }

                _ => panic!("Negotiated: Invalid state")
            }
        }
    }

    /// Returns a `NegotiatedComplete` future that waits for protocol
    /// negotiation to complete.
    pub fn complete(self) -> NegotiatedComplete<TInner> {
        NegotiatedComplete { inner: Some(self) }
    }
}

/// The states of a `Negotiated` I/O stream.
enum State<R> {
    /// In this state, a `Negotiated` is still expecting to
    /// receive confirmation of the protocol it as settled on.
    Expecting { io: MessageReader<R>, protocol: Protocol },

    /// In this state, a protocol has been agreed upon and may
    /// only be pending the sending of the final acknowledgement,
    /// which is prepended to / combined with the next write for
    /// efficiency.
    Completed { io: R, remaining: BytesMut },

    /// Temporary state while moving the `io` resource from
    /// `Expecting` to `Completed`.
    Invalid,
}

impl<R> io::Read for Negotiated<R>
where
    R: AsyncRead + AsyncWrite
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let State::Completed { io, remaining } = &mut self.state {
                // If protocol negotiation is complete and there is no
                // remaining data to be flushed, commence with reading.
                if remaining.is_empty() {
                    return io.read(buf)
                }
            }

            // Poll the `Negotiated`, driving protocol negotiation to completion,
            // including flushing of any remaining data.
            let result = self.poll();

            // There is still remaining data to be sent before data relating
            // to the negotiated protocol can be read.
            if let Ok(Async::NotReady) = result {
                return Err(io::ErrorKind::WouldBlock.into())
            }

            if let Err(err) = result {
                return Err(err.into())
            }
        }
    }
}

impl<TInner> AsyncRead for Negotiated<TInner>
where
    TInner: AsyncRead + AsyncWrite
{
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match &self.state {
            State::Completed { io, .. } =>
                io.prepare_uninitialized_buffer(buf),
            State::Expecting { io, .. } =>
                io.inner_ref().prepare_uninitialized_buffer(buf),
            State::Invalid => panic!("Negotiated: Invalid state")
        }
    }
}

impl<TInner> io::Write for Negotiated<TInner>
where
    TInner: AsyncWrite
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match &mut self.state {
            State::Completed { io, ref mut remaining } => {
                if !remaining.is_empty() {
                    // Try to write `buf` together with `remaining` for efficiency,
                    // regardless of whether the underlying I/O stream is buffered.
                    // Every call to `write` may imply a syscall and separate
                    // network packet.
                    let remaining_len = remaining.len();
                    remaining.extend_from_slice(buf);
                    match io.write(&remaining) {
                        Err(e) => {
                            remaining.split_off(buf.len());
                            debug_assert_eq!(remaining.len(), remaining_len);
                            Err(e)
                        }
                        Ok(n) => {
                            remaining.split_to(n);
                            if !remaining.is_empty() {
                                let written = if n < buf.len() {
                                    remaining.split_off(remaining_len);
                                    n
                                } else {
                                    buf.len()
                                };
                                debug_assert!(remaining.len() <= remaining_len);
                                Ok(written)
                            } else {
                                Ok(buf.len())
                            }
                        }
                    }
                } else {
                    io.write(buf)
                }
            },
            State::Expecting { io, .. } => io.write(buf),
            State::Invalid => panic!("Negotiated: Invalid state")
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match &mut self.state {
            State::Completed { io, ref mut remaining } => {
                while !remaining.is_empty() {
                    let n = io.write(remaining)?;
                    if n == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "Failed to write remaining buffer."))
                    }
                    remaining.split_to(n);
                }
                io.flush()
            },
            State::Expecting { io, .. } => io.flush(),
            State::Invalid => panic!("Negotiated: Invalid state")
        }
    }
}

impl<TInner> AsyncWrite for Negotiated<TInner>
where
    TInner: AsyncWrite + AsyncRead
{
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        // Ensure all data has been flushed and expected negotiation messages
        // have been received.
        try_ready!(self.poll().map_err(Into::<io::Error>::into));
        // Continue with the shutdown of the underlying I/O stream.
        match &mut self.state {
            State::Completed { io, .. } => io.shutdown(),
            State::Expecting { io, .. } => io.shutdown(),
            State::Invalid => panic!("Negotiated: Invalid state")
        }
    }
}

/// Error that can happen when negotiating a protocol with the remote.
#[derive(Debug)]
pub enum NegotiationError {
    /// A protocol error occurred during the negotiation.
    ProtocolError(ProtocolError),

    /// Protocol negotiation failed because no protocol could be agreed upon.
    Failed,
}

impl From<ProtocolError> for NegotiationError {
    fn from(err: ProtocolError) -> NegotiationError {
        NegotiationError::ProtocolError(err)
    }
}

impl From<io::Error> for NegotiationError {
    fn from(err: io::Error) -> NegotiationError {
        ProtocolError::from(err).into()
    }
}

impl Into<io::Error> for NegotiationError {
    fn into(self) -> io::Error {
        if let NegotiationError::ProtocolError(e) = self {
            return e.into()
        }
        io::Error::new(io::ErrorKind::Other, self)
    }
}

impl Error for NegotiationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            NegotiationError::ProtocolError(err) => Some(err),
            _ => None,
        }
    }
}

impl fmt::Display for NegotiationError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(fmt, "{}", Error::description(self))
    }
}


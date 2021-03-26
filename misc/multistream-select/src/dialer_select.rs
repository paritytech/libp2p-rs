// Copyright 2017 Parity Technologies (UK) Ltd.
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

//! Protocol negotiation strategies for the peer acting as the dialer.

use crate::{Negotiated, NegotiationError, Version};
use crate::protocol::{Protocol, ProtocolError, MessageIO, Message, HeaderLine};

use futures::{future::Either, prelude::*};
use std::{convert::TryFrom as _, iter, mem, pin::Pin, task::{Context, Poll}};

/// Returns a `Future` that negotiates a protocol on the given I/O stream
/// for a peer acting as the _dialer_ (or _initiator_).
///
/// This function is given an I/O stream and a list of protocols and returns a
/// computation that performs the protocol negotiation with the remote. The
/// returned `Future` resolves with the name of the negotiated protocol and
/// a [`Negotiated`] I/O stream.
///
/// The chosen message flow for protocol negotiation depends on the numbers of
/// supported protocols given. That is, this function delegates to serial or
/// parallel variant based on the number of protocols given. The number of
/// protocols is determined through the `size_hint` of the given iterator and
/// thus an inaccurate size estimate may result in a suboptimal choice.
///
/// Within the scope of this library, a dialer always commits to a specific
/// multistream-select [`Version`], whereas a listener always supports
/// all versions supported by this library. Frictionless multistream-select
/// protocol upgrades may thus proceed by deployments with updated listeners,
/// eventually followed by deployments of dialers choosing the newer protocol.
pub fn dialer_select_proto<R, I>(
    inner: R,
    protocols: I,
    version: Version
) -> DialerSelectFuture<R, I::IntoIter>
where
    R: AsyncRead + AsyncWrite,
    I: IntoIterator,
    I::Item: AsRef<[u8]>
{
    let iter = protocols.into_iter();
    // We choose between the "serial" and "parallel" strategies based on the number of protocols.
    if iter.size_hint().1.map(|n| n <= 3).unwrap_or(false) {
       Either::Left(dialer_select_proto_serial(inner, iter, version))
    } else {
        Either::Right(dialer_select_proto_parallel(inner, iter, version))
    }
}

/// Future, returned by `dialer_select_proto`, which selects a protocol and dialer
/// either trying protocols in-order, or by requesting all protocols supported
/// by the remote upfront, from which the first protocol found in the dialer's
/// list of protocols is selected.
pub type DialerSelectFuture<R, I> = Either<DialerSelectSeq<R, I>, DialerSelectPar<R, I>>;

/// Returns a `Future` that negotiates a protocol on the given I/O stream.
///
/// Just like [`dialer_select_proto`] but always using an iterative message flow,
/// trying the given list of supported protocols one-by-one.
///
/// This strategy is preferable if the dialer only supports a few protocols.
pub(crate) fn dialer_select_proto_serial<R, I>(
    inner: R,
    protocols: I,
    version: Version
) -> DialerSelectSeq<R, I::IntoIter>
where
    R: AsyncRead + AsyncWrite,
    I: IntoIterator,
    I::Item: AsRef<[u8]>
{
    let protocols = protocols.into_iter().peekable();
    DialerSelectSeq {
        version,
        protocols,
        state: SeqState::SendHeader {
            io: MessageIO::new(inner),
        }
    }
}

/// Returns a `Future` that negotiates a protocol on the given I/O stream.
///
/// Just like [`dialer_select_proto`] but always using a message flow that first
/// requests all supported protocols from the remote, selecting the first
/// protocol from the given list of supported protocols that is supported
/// by the remote.
///
/// This strategy may be beneficial if the dialer supports many protocols
/// and it is unclear whether the remote supports one of the first few.
pub(crate) fn dialer_select_proto_parallel<R, I>(
    inner: R,
    protocols: I,
    version: Version
) -> DialerSelectPar<R, I::IntoIter>
where
    R: AsyncRead + AsyncWrite,
    I: IntoIterator,
    I::Item: AsRef<[u8]>
{
    let protocols = protocols.into_iter();
    DialerSelectPar {
        version,
        protocols,
        state: ParState::SendHeader {
            io: MessageIO::new(inner)
        }
    }
}

/// A `Future` returned by [`dialer_select_proto_serial`] which negotiates
/// a protocol iteratively by considering one protocol after the other.
#[pin_project::pin_project]
pub struct DialerSelectSeq<R, I>
where
    R: AsyncRead + AsyncWrite,
    I: Iterator,
    I::Item: AsRef<[u8]>
{
    // TODO: It would be nice if eventually N = I::Item = Protocol.
    protocols: iter::Peekable<I>,
    state: SeqState<R, I::Item>,
    version: Version,
}

enum SeqState<R, N>
where
    R: AsyncRead + AsyncWrite,
    N: AsRef<[u8]>
{
    SendHeader { io: MessageIO<R>, },
    SendProtocol { io: MessageIO<R>, protocol: N },
    FlushProtocol { io: MessageIO<R>, protocol: N },
    AwaitProtocol { io: MessageIO<R>, protocol: N },
    Done
}

impl<R, I> Future for DialerSelectSeq<R, I>
where
    // The Unpin bound here is required because we produce a `Negotiated<R>` as the output.
    // It also makes the implementation considerably easier to write.
    R: AsyncRead + AsyncWrite + Unpin,
    I: Iterator,
    I::Item: AsRef<[u8]>
{
    type Output = Result<(I::Item, Negotiated<R>), NegotiationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        loop {
            match mem::replace(this.state, SeqState::Done) {
                SeqState::SendHeader { mut io } => {
                    match Pin::new(&mut io).poll_ready(cx)? {
                        Poll::Ready(()) => {},
                        Poll::Pending => {
                            *this.state = SeqState::SendHeader { io };
                            return Poll::Pending
                        },
                    }

                    let h = HeaderLine::from(*this.version);
                    if let Err(err) = Pin::new(&mut io).start_send(Message::Header(h)) {
                        return Poll::Ready(Err(From::from(err)));
                    }

                    let protocol = this.protocols.next().ok_or(NegotiationError::Failed)?;

                    // The dialer always sends the header and the first protocol
                    // proposal in one go for efficiency.
                    *this.state = SeqState::SendProtocol { io, protocol };
                }

                SeqState::SendProtocol { mut io, protocol } => {
                    match Pin::new(&mut io).poll_ready(cx)? {
                        Poll::Ready(()) => {},
                        Poll::Pending => {
                            *this.state = SeqState::SendProtocol { io, protocol };
                            return Poll::Pending
                        },
                    }

                    let p = Protocol::try_from(protocol.as_ref())?;
                    if let Err(err) = Pin::new(&mut io).start_send(Message::Protocol(p.clone())) {
                        return Poll::Ready(Err(From::from(err)));
                    }
                    log::debug!("Dialer: Proposed protocol: {}", p);

                    if this.protocols.peek().is_some() {
                        *this.state = SeqState::FlushProtocol { io, protocol }
                    } else {
                        match this.version {
                            Version::V1 => *this.state = SeqState::FlushProtocol { io, protocol },
                            // This is the only effect that `V1Lazy` has compared to `V1`:
                            // Optimistically settling on the only protocol that
                            // the dialer supports for this negotiation. Notably,
                            // the dialer expects a regular `V1` response.
                            Version::V1Lazy => {
                                log::debug!("Dialer: Expecting proposed protocol: {}", p);
                                let hl = HeaderLine::from(Version::V1Lazy);
                                let io = Negotiated::expecting(io.into_reader(), p, Some(hl));
                                return Poll::Ready(Ok((protocol, io)))
                            }
                        }
                    }
                }

                SeqState::FlushProtocol { mut io, protocol } => {
                    match Pin::new(&mut io).poll_flush(cx)? {
                        Poll::Ready(()) => *this.state = SeqState::AwaitProtocol { io, protocol },
                        Poll::Pending => {
                            *this.state = SeqState::FlushProtocol { io, protocol };
                            return Poll::Pending
                        },
                    }
                }

                SeqState::AwaitProtocol { mut io, protocol } => {
                    let msg = match Pin::new(&mut io).poll_next(cx)? {
                        Poll::Ready(Some(msg)) => msg,
                        Poll::Pending => {
                            *this.state = SeqState::AwaitProtocol { io, protocol };
                            return Poll::Pending
                        }
                        // Treat EOF error as [`NegotiationError::Failed`], not as
                        // [`NegotiationError::ProtocolError`], allowing dropping or closing an I/O
                        // stream as a permissible way to "gracefully" fail a negotiation.
                        Poll::Ready(None) => return Poll::Ready(Err(NegotiationError::Failed)),
                    };

                    match msg {
                        Message::Header(v) if v == HeaderLine::from(*this.version) => {
                            *this.state = SeqState::AwaitProtocol { io, protocol };
                        }
                        Message::Protocol(ref p) if p.as_ref() == protocol.as_ref() => {
                            log::debug!("Dialer: Received confirmation for protocol: {}", p);
                            let io = Negotiated::completed(io.into_inner());
                            return Poll::Ready(Ok((protocol, io)));
                        }
                        Message::NotAvailable => {
                            log::debug!("Dialer: Received rejection of protocol: {}",
                                String::from_utf8_lossy(protocol.as_ref()));
                            let protocol = this.protocols.next().ok_or(NegotiationError::Failed)?;
                            *this.state = SeqState::SendProtocol { io, protocol }
                        }
                        _ => return Poll::Ready(Err(ProtocolError::InvalidMessage.into()))
                    }
                }

                SeqState::Done => panic!("SeqState::poll called after completion")
            }
        }
    }
}

/// A `Future` returned by [`dialer_select_proto_parallel`] which negotiates
/// a protocol selectively by considering all supported protocols of the remote
/// "in parallel".
#[pin_project::pin_project]
pub struct DialerSelectPar<R, I>
where
    R: AsyncRead + AsyncWrite,
    I: Iterator,
    I::Item: AsRef<[u8]>
{
    protocols: I,
    state: ParState<R, I::Item>,
    version: Version,
}

enum ParState<R, N>
where
    R: AsyncRead + AsyncWrite,
    N: AsRef<[u8]>
{
    SendHeader { io: MessageIO<R> },
    SendProtocolsRequest { io: MessageIO<R> },
    Flush { io: MessageIO<R> },
    RecvProtocols { io: MessageIO<R> },
    SendProtocol { io: MessageIO<R>, protocol: N },
    Done
}

impl<R, I> Future for DialerSelectPar<R, I>
where
    // The Unpin bound here is required because we produce a `Negotiated<R>` as the output.
    // It also makes the implementation considerably easier to write.
    R: AsyncRead + AsyncWrite + Unpin,
    I: Iterator,
    I::Item: AsRef<[u8]>
{
    type Output = Result<(I::Item, Negotiated<R>), NegotiationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        loop {
            match mem::replace(this.state, ParState::Done) {
                ParState::SendHeader { mut io } => {
                    match Pin::new(&mut io).poll_ready(cx)? {
                        Poll::Ready(()) => {},
                        Poll::Pending => {
                            *this.state = ParState::SendHeader { io };
                            return Poll::Pending
                        },
                    }

                    let msg = Message::Header(HeaderLine::from(*this.version));
                    if let Err(err) = Pin::new(&mut io).start_send(msg) {
                        return Poll::Ready(Err(From::from(err)));
                    }

                    *this.state = ParState::SendProtocolsRequest { io };
                }

                ParState::SendProtocolsRequest { mut io } => {
                    match Pin::new(&mut io).poll_ready(cx)? {
                        Poll::Ready(()) => {},
                        Poll::Pending => {
                            *this.state = ParState::SendProtocolsRequest { io };
                            return Poll::Pending
                        },
                    }

                    if let Err(err) = Pin::new(&mut io).start_send(Message::ListProtocols) {
                        return Poll::Ready(Err(From::from(err)));
                    }

                    log::debug!("Dialer: Requested supported protocols.");
                    *this.state = ParState::Flush { io }
                }

                ParState::Flush { mut io } => {
                    match Pin::new(&mut io).poll_flush(cx)? {
                        Poll::Ready(()) => *this.state = ParState::RecvProtocols { io },
                        Poll::Pending => {
                            *this.state = ParState::Flush { io };
                            return Poll::Pending
                        },
                    }
                }

                ParState::RecvProtocols { mut io } => {
                    let msg = match Pin::new(&mut io).poll_next(cx)? {
                        Poll::Ready(Some(msg)) => msg,
                        Poll::Pending => {
                            *this.state = ParState::RecvProtocols { io };
                            return Poll::Pending
                        }
                        // Treat EOF error as [`NegotiationError::Failed`], not as
                        // [`NegotiationError::ProtocolError`], allowing dropping or closing an I/O
                        // stream as a permissible way to "gracefully" fail a negotiation.
                        Poll::Ready(None) => return Poll::Ready(Err(NegotiationError::Failed)),
                    };

                    match &msg {
                        Message::Header(h) if h == &HeaderLine::from(*this.version) => {
                            *this.state = ParState::RecvProtocols { io }
                        }
                        Message::Protocols(supported) => {
                            let protocol = this.protocols.by_ref()
                                .find(|p| supported.iter().any(|s|
                                    s.as_ref() == p.as_ref()))
                                .ok_or(NegotiationError::Failed)?;
                            log::debug!("Dialer: Found supported protocol: {}",
                                String::from_utf8_lossy(protocol.as_ref()));
                            *this.state = ParState::SendProtocol { io, protocol };
                        }
                        _ => return Poll::Ready(Err(ProtocolError::InvalidMessage.into())),
                    }
                }

                ParState::SendProtocol { mut io, protocol } => {
                    match Pin::new(&mut io).poll_ready(cx)? {
                        Poll::Ready(()) => {},
                        Poll::Pending => {
                            *this.state = ParState::SendProtocol { io, protocol };
                            return Poll::Pending
                        },
                    }

                    let p = Protocol::try_from(protocol.as_ref())?;
                    if let Err(err) = Pin::new(&mut io).start_send(Message::Protocol(p.clone())) {
                        return Poll::Ready(Err(From::from(err)));
                    }

                    log::debug!("Dialer: Expecting proposed protocol: {}", p);
                    let io = Negotiated::expecting(io.into_reader(), p, None);

                    return Poll::Ready(Ok((protocol, io)))
                }

                ParState::Done => panic!("ParState::poll called after completion")
            }
        }
    }
}

// Copyright 2021 COMIT Network.
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

use crate::codec::{
    Cookie, ErrorCode, Message, Namespace, NewRegistration, Registration, RendezvousCodec, Ttl,
};
use crate::handler::Error;
use crate::substream_handler::{Next, SubstreamHandler};
use asynchronous_codec::Framed;
use futures::{SinkExt, StreamExt};
use libp2p_swarm::NegotiatedSubstream;
use std::fmt;
use std::task::{Context, Poll};

/// The state of an inbound substream (i.e. the remote node opened it).
pub enum Stream {
    /// We are in the process of reading a message from the substream.
    PendingRead(Framed<NegotiatedSubstream, RendezvousCodec>),
    /// We read a message, dispatched it to the behaviour and are waiting for the response.
    PendingBehaviour(Framed<NegotiatedSubstream, RendezvousCodec>),
    /// We are in the process of sending a response.
    PendingSend(Framed<NegotiatedSubstream, RendezvousCodec>, Message),
    /// We've sent the message and are now closing down the substream.
    PendingClose(Framed<NegotiatedSubstream, RendezvousCodec>),
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Stream::PendingRead(_) => write!(f, "Inbound::PendingRead"),
            Stream::PendingBehaviour(_) => write!(f, "Inbound::PendingBehaviour"),
            Stream::PendingSend(_, _) => write!(f, "Inbound::PendingSend"),
            Stream::PendingClose(_) => write!(f, "Inbound::PendingClose"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OutEvent {
    RegistrationRequested(NewRegistration),
    UnregisterRequested(Namespace),
    DiscoverRequested {
        namespace: Option<Namespace>,
        cookie: Option<Cookie>,
        limit: Option<u64>,
    },
}

#[derive(Debug)]
pub enum InEvent {
    RegisterResponse {
        ttl: Ttl,
    },
    DeclineRegisterRequest(ErrorCode),
    DiscoverResponse {
        discovered: Vec<Registration>,
        cookie: Cookie,
    },
    DeclineDiscoverRequest(ErrorCode),
}

impl SubstreamHandler for Stream {
    type InEvent = InEvent;
    type OutEvent = OutEvent;
    type Error = Error;
    type OpenInfo = ();

    fn new(substream: NegotiatedSubstream, _: Self::OpenInfo) -> Self {
        Stream::PendingRead(Framed::new(substream, RendezvousCodec::default()))
    }

    fn inject_event(self, event: Self::InEvent) -> Self {
        match (event, self) {
            (InEvent::RegisterResponse { ttl }, Stream::PendingBehaviour(substream)) => {
                Stream::PendingSend(substream, Message::RegisterResponse(Ok(ttl)))
            }
            (InEvent::DeclineRegisterRequest(error), Stream::PendingBehaviour(substream)) => {
                Stream::PendingSend(substream, Message::RegisterResponse(Err(error)))
            }
            (
                InEvent::DiscoverResponse { discovered, cookie },
                Stream::PendingBehaviour(substream),
            ) => Stream::PendingSend(
                substream,
                Message::DiscoverResponse(Ok((discovered, cookie))),
            ),
            (InEvent::DeclineDiscoverRequest(error), Stream::PendingBehaviour(substream)) => {
                Stream::PendingSend(substream, Message::DiscoverResponse(Err(error)))
            }
            (event, inbound) => {
                debug_assert!(false, "{:?} cannot handle event {:?}", inbound, event);

                inbound
            }
        }
    }

    fn advance(self, cx: &mut Context<'_>) -> Result<Next<Self, Self::OutEvent>, Self::Error> {
        let next_state = match self {
            Stream::PendingRead(mut substream) => {
                match substream.poll_next_unpin(cx).map_err(Error::ReadMessage)? {
                    Poll::Ready(Some(msg)) => {
                        let event = match msg {
                            Message::Register(registration) => {
                                OutEvent::RegistrationRequested(registration)
                            }
                            Message::Discover {
                                cookie,
                                namespace,
                                limit,
                            } => OutEvent::DiscoverRequested {
                                cookie,
                                namespace,
                                limit,
                            },
                            Message::Unregister(namespace) => {
                                OutEvent::UnregisterRequested(namespace)
                            }
                            other => return Err(Error::BadMessage(other)),
                        };

                        Next::EmitEvent {
                            event,
                            next_state: Stream::PendingBehaviour(substream),
                        }
                    }
                    Poll::Ready(None) => return Err(Error::UnexpectedEndOfStream),
                    Poll::Pending => Next::Pending {
                        next_state: Stream::PendingRead(substream),
                    },
                }
            }
            Stream::PendingBehaviour(substream) => Next::Pending {
                next_state: Stream::PendingBehaviour(substream),
            },
            Stream::PendingSend(mut substream, message) => match substream
                .poll_ready_unpin(cx)
                .map_err(Error::WriteMessage)?
            {
                Poll::Ready(()) => {
                    substream
                        .start_send_unpin(message)
                        .map_err(Error::WriteMessage)?;

                    Next::Continue {
                        next_state: Stream::PendingClose(substream),
                    }
                }
                Poll::Pending => Next::Pending {
                    next_state: Stream::PendingSend(substream, message),
                },
            },
            Stream::PendingClose(mut substream) => match substream.poll_close_unpin(cx) {
                Poll::Ready(Ok(())) => Next::Done,
                Poll::Ready(Err(_)) => Next::Done, // there is nothing we can do about an error during close
                Poll::Pending => Next::Pending {
                    next_state: Stream::PendingClose(substream),
                },
            },
        };

        Ok(next_state)
    }
}

use crate::codec::RendezvousCodec;
use crate::codec::{Message, Registration};
use crate::protocol;
use asynchronous_codec::Framed;
use futures::SinkExt;
use libp2p_core::{InboundUpgrade, OutboundUpgrade};
use libp2p_swarm::{
    KeepAlive, NegotiatedSubstream, ProtocolsHandler, ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr, SubstreamProtocol,
};
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::{Context, Poll};
use void::Void;

pub struct RendezvousRequest;

pub struct RendezvousHandler {
    outbound_substream: OutboundSubstreamState,

    inbound_substream: InboundSubstreamState,

    keep_alive: KeepAlive,
}

impl RendezvousHandler {
    pub fn new() -> Self {
        Self {
            outbound_substream: OutboundSubstreamState::Idle,
            inbound_substream: InboundSubstreamState::Idle,
            keep_alive: KeepAlive::Yes,
        }
    }
}

pub struct HandlerEvent(pub Message);

#[derive(Debug)]
pub enum Input {
    RegisterRequest {
        namespace: String,
        ttl: Option<i64>,
        // TODO: Signed peer record field
    },
    UnregisterRequest {
        namespace: String,
        // TODO: what is the `id` field here in the PB message
    },
    DiscoverRequest {
        namespace: Option<String>,
        // TODO limit: Option<i64>
        // TODO cookie: Option<Vec<u8>
    },
    RegisterResponse {
        ttl: i64,
    },
    DiscoverResponse {
        registrations: Vec<Registration>,
    },
}

impl From<Input> for Message {
    fn from(req: Input) -> Self {
        match req {
            Input::RegisterRequest { namespace, ttl } => Message::Register(todo!()),
            Input::UnregisterRequest { namespace } => Message::Unregister { namespace },
            Input::DiscoverRequest { namespace } => Message::Discover { namespace },
            Input::RegisterResponse { ttl } => Message::SuccessfullyRegistered { ttl },
            Input::DiscoverResponse { registrations } => {
                Message::DiscoverResponse { registrations }
            }
        }
    }
}

/// State of the inbound substream, opened either by us or by the remote.
enum InboundSubstreamState {
    Idle,
    /// Waiting for behaviour to respond to the inbound substream
    WaitingToRespondToRemote(Framed<NegotiatedSubstream, RendezvousCodec>, Message),
    /// Waiting to send response to remote
    PendingSend(Framed<NegotiatedSubstream, RendezvousCodec>, Message),
    PendingFlush(Framed<NegotiatedSubstream, RendezvousCodec>),
    Closing(Framed<NegotiatedSubstream, RendezvousCodec>),
    /// An error occurred during processing.
    Poisoned,
}

/// State of the outbound substream, opened either by us or by the remote.
enum OutboundSubstreamState {
    Idle,
    WaitingUpgrade(Message),
    /// Waiting to send a message to the remote.
    PendingSend(Framed<NegotiatedSubstream, RendezvousCodec>, Message),
    /// Waiting to flush the substream so that the data arrives to the remote.
    PendingFlush(Framed<NegotiatedSubstream, RendezvousCodec>),
    /// Waiting for remote to respond on the outbound substream
    WaitingForRemoteToRespond(Framed<NegotiatedSubstream, RendezvousCodec>),
    Closing(Framed<NegotiatedSubstream, RendezvousCodec>),
    /// An error occurred during processing.
    Poisoned,
}

impl ProtocolsHandler for RendezvousHandler {
    type InEvent = Input;
    type OutEvent = HandlerEvent;
    type Error = crate::codec::Error;
    type InboundOpenInfo = Message;
    type InboundProtocol = protocol::Rendezvous;
    type OutboundOpenInfo = Message;
    type OutboundProtocol = protocol::Rendezvous;

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        let rendezvous_protocol = crate::protocol::Rendezvous::new();
        SubstreamProtocol::new(rendezvous_protocol, todo!())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        substream: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        msg: Self::InboundOpenInfo,
    ) {
        if let InboundSubstreamState::Idle = self.inbound_substream {
            self.inbound_substream =
                InboundSubstreamState::WaitingToRespondToRemote(substream, msg);
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        substream: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        msg: Self::OutboundOpenInfo,
    ) {
        if let OutboundSubstreamState::Idle = self.outbound_substream {
            self.outbound_substream = OutboundSubstreamState::PendingSend(substream, msg);
        }
    }

    // event injected from NotifyHandler
    fn inject_event(&mut self, req: Input) {
        match &req {
            Input::RegisterRequest { .. } => {
                if let OutboundSubstreamState::Idle = self.outbound_substream {
                    self.outbound_substream =
                        OutboundSubstreamState::WaitingUpgrade(Message::from(req))
                }
            }
            Input::UnregisterRequest { .. } => {
                if let OutboundSubstreamState::Idle = self.outbound_substream {
                    self.outbound_substream =
                        OutboundSubstreamState::WaitingUpgrade(Message::from(req))
                }
            }
            Input::DiscoverRequest { .. } => {
                if let OutboundSubstreamState::Idle = self.outbound_substream {
                    self.outbound_substream =
                        OutboundSubstreamState::WaitingUpgrade(Message::from(req))
                }
            }
            Input::RegisterResponse { .. } => {
                if let InboundSubstreamState::WaitingToRespondToRemote(substream, msg) =
                    std::mem::replace(&mut self.inbound_substream, InboundSubstreamState::Poisoned)
                {
                    self.inbound_substream = InboundSubstreamState::PendingSend(substream, msg)
                }
            }
            Input::DiscoverResponse { .. } => {
                if let InboundSubstreamState::WaitingToRespondToRemote(substream, msg) =
                    std::mem::replace(&mut self.inbound_substream, InboundSubstreamState::Poisoned)
                {
                    self.inbound_substream = InboundSubstreamState::PendingSend(substream, msg)
                }
            }
        }
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        _error: ProtocolsHandlerUpgrErr<Void>,
    ) {
        todo!()
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        match std::mem::replace(&mut self.inbound_substream, InboundSubstreamState::Poisoned) {
            InboundSubstreamState::PendingSend(mut substream, message) => {
                match substream.poll_ready_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        match substream.start_send_unpin(message) {
                            Ok(()) => {
                                self.inbound_substream =
                                    InboundSubstreamState::PendingFlush(substream);
                            }
                            Err(e) => {
                                return Poll::Ready(ProtocolsHandlerEvent::Close(e));
                            }
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(ProtocolsHandlerEvent::Close(e));
                    }
                    Poll::Pending => {
                        self.keep_alive = KeepAlive::Yes;
                        self.inbound_substream =
                            InboundSubstreamState::PendingSend(substream, message);
                    }
                }
            }
            InboundSubstreamState::PendingFlush(mut substream) => {
                match substream.poll_flush_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.inbound_substream = InboundSubstreamState::Idle;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(ProtocolsHandlerEvent::Close(e)),
                    Poll::Pending => {
                        self.keep_alive = KeepAlive::Yes;
                        self.inbound_substream = InboundSubstreamState::PendingFlush(substream);
                    }
                }
            }
            InboundSubstreamState::WaitingToRespondToRemote(_substream, msg) => {
                return Poll::Ready(ProtocolsHandlerEvent::Custom(HandlerEvent(msg)));
            }
            InboundSubstreamState::Closing(mut substream) => {
                match substream.poll_close_unpin(cx) {
                    Poll::Ready(..) => {
                        if let OutboundSubstreamState::Idle | OutboundSubstreamState::Poisoned =
                            self.outbound_substream
                        {
                            self.keep_alive = KeepAlive::No;
                        }
                        self.inbound_substream = InboundSubstreamState::Idle;
                    }
                    Poll::Pending => {
                        self.inbound_substream = InboundSubstreamState::Closing(substream);
                    }
                }
            }
            InboundSubstreamState::Idle => self.outbound_substream = OutboundSubstreamState::Idle,
            InboundSubstreamState::Poisoned => {
                self.outbound_substream = OutboundSubstreamState::Idle
            }
        }

        match std::mem::replace(
            &mut self.outbound_substream,
            OutboundSubstreamState::Poisoned,
        ) {
            OutboundSubstreamState::WaitingUpgrade(msg) => {
                self.outbound_substream = OutboundSubstreamState::WaitingUpgrade(msg);
            }
            OutboundSubstreamState::PendingSend(mut substream, message) => {
                match substream.poll_ready_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        match substream.start_send_unpin(message) {
                            Ok(()) => {
                                self.outbound_substream =
                                    OutboundSubstreamState::PendingFlush(substream);
                            }
                            Err(e) => {
                                return Poll::Ready(ProtocolsHandlerEvent::Close(e));
                            }
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(ProtocolsHandlerEvent::Close(e));
                    }
                    Poll::Pending => {
                        self.keep_alive = KeepAlive::Yes;
                        self.outbound_substream =
                            OutboundSubstreamState::PendingSend(substream, message);
                    }
                }
            }
            OutboundSubstreamState::PendingFlush(mut substream) => {
                match substream.poll_flush_unpin(cx) {
                    Poll::Ready(Ok(())) => {
                        self.outbound_substream =
                            OutboundSubstreamState::WaitingForRemoteToRespond(substream)
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(ProtocolsHandlerEvent::Close(e)),
                    Poll::Pending => {
                        self.keep_alive = KeepAlive::Yes;
                        self.outbound_substream = OutboundSubstreamState::PendingFlush(substream);
                    }
                }
            }
            OutboundSubstreamState::WaitingForRemoteToRespond(substream) => {
                let message: Message = todo!("Sink::read??");
                self.outbound_substream = OutboundSubstreamState::Idle;
                return Poll::Ready(ProtocolsHandlerEvent::Custom(HandlerEvent(message)));
            }
            OutboundSubstreamState::Closing(mut substream) => {
                match substream.poll_close_unpin(cx) {
                    Poll::Ready(..) => {
                        if let InboundSubstreamState::Idle | InboundSubstreamState::Poisoned =
                            self.inbound_substream
                        {
                            self.keep_alive = KeepAlive::No;
                        }
                        self.outbound_substream = OutboundSubstreamState::Idle;
                    }
                    Poll::Pending => {
                        self.outbound_substream = OutboundSubstreamState::Closing(substream);
                    }
                }
            }
            OutboundSubstreamState::Idle => self.outbound_substream = OutboundSubstreamState::Idle,
            OutboundSubstreamState::Poisoned => {
                self.outbound_substream = OutboundSubstreamState::Poisoned
            }
        }

        Poll::Pending
    }
}

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

use crate::upgrade::SendWrapper;
use crate::protocols_handler::{
    KeepAlive,
    ProtocolsHandler,
    IntoProtocolsHandler,
    ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr
};

use futures::prelude::*;
use futures::stream::FuturesUnordered;
use libp2p_core::{
    Multiaddr,
    Connected,
    connection::{
        ConnectionHandler,
        ConnectionHandlerEvent,
        IntoConnectionHandler,
        Substream,
        SubstreamEndpoint,
    },
    muxing::StreamMuxerBox,
    upgrade::{self, InboundUpgradeApply, OutboundUpgradeApply, UpgradeError}
};
use std::{error, fmt, pin::Pin, task::Context, task::Poll, time::Duration};
use wasm_timer::{Delay, Instant};

/// Prototype for a `NodeHandlerWrapper`.
pub struct NodeHandlerWrapperBuilder<TIntoProtoHandler> {
    /// The underlying handler.
    handler: TIntoProtoHandler,
    /// The substream upgrade protocol override, if any.
    substream_upgrade_protocol_override: Option<upgrade::Version>,
}

impl<TIntoProtoHandler> NodeHandlerWrapperBuilder<TIntoProtoHandler>
where
    TIntoProtoHandler: IntoProtocolsHandler
{
    /// Builds a `NodeHandlerWrapperBuilder`.
    pub(crate) fn new(handler: TIntoProtoHandler) -> Self {
        NodeHandlerWrapperBuilder {
            handler,
            substream_upgrade_protocol_override: None,
        }
    }

    pub(crate) fn with_substream_upgrade_protocol_override(
        mut self,
        version: Option<upgrade::Version>
    ) -> Self {
        self.substream_upgrade_protocol_override = version;
        self
    }
}

impl<TIntoProtoHandler, TProtoHandler> IntoConnectionHandler
    for NodeHandlerWrapperBuilder<TIntoProtoHandler>
where
    TIntoProtoHandler: IntoProtocolsHandler<Handler = TProtoHandler>,
    TProtoHandler: ProtocolsHandler,
{
    type Handler = NodeHandlerWrapper<TIntoProtoHandler::Handler>;

    fn into_handler(self, connected: &Connected) -> Self::Handler {
        NodeHandlerWrapper {
            handler: self.handler.into_handler(&connected.peer_id, &connected.endpoint),
            negotiating_in: Default::default(),
            negotiating_out: Default::default(),
            queued_dial_upgrades: Vec::new(),
            unique_dial_upgrade_id: 0,
            shutdown: Shutdown::None,
            substream_upgrade_protocol_override: self.substream_upgrade_protocol_override,
        }
    }
}

// A `ConnectionHandler` for an underlying `ProtocolsHandler`.
/// Wraps around an implementation of `ProtocolsHandler`, and implements `NodeHandler`.
// TODO: add a caching system for protocols that are supported or not
pub struct NodeHandlerWrapper<TProtoHandler>
where
    TProtoHandler: ProtocolsHandler,
{
    /// The underlying handler.
    handler: TProtoHandler,
    /// Futures that upgrade incoming substreams.
    negotiating_in: FuturesUnordered<SubstreamUpgrade<
        TProtoHandler::InboundOpenInfo,
        InboundUpgradeApply<Substream<StreamMuxerBox>, SendWrapper<TProtoHandler::InboundProtocol>>,
    >>,
    /// Futures that upgrade outgoing substreams.
    negotiating_out: FuturesUnordered<SubstreamUpgrade<
        TProtoHandler::OutboundOpenInfo,
        OutboundUpgradeApply<Substream<StreamMuxerBox>, SendWrapper<TProtoHandler::OutboundProtocol>>,
    >>,
    /// For each outbound substream request, how to upgrade it. The first element of the tuple
    /// is the unique identifier (see `unique_dial_upgrade_id`).
    queued_dial_upgrades: Vec<(u64, SendWrapper<TProtoHandler::OutboundProtocol>)>,
    /// Unique identifier assigned to each queued dial upgrade.
    unique_dial_upgrade_id: u64,
    /// The currently planned connection & handler shutdown.
    shutdown: Shutdown,
    /// The substream upgrade protocol override, if any.
    substream_upgrade_protocol_override: Option<upgrade::Version>,
}

struct SubstreamUpgrade<UserData, Upgrade> {
    user_data: Option<UserData>,
    timeout: Delay,
    upgrade: Upgrade,
}

impl<UserData, Upgrade> Unpin for SubstreamUpgrade<UserData, Upgrade> {}

impl<UserData, Upgrade, UpgradeOutput, TUpgradeError> Future for SubstreamUpgrade<UserData, Upgrade>
where
    Upgrade: Future<Output = Result<UpgradeOutput, UpgradeError<TUpgradeError>>> + Unpin,
{
    type Output = (UserData, Result<UpgradeOutput, ProtocolsHandlerUpgrErr<TUpgradeError>>);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        match self.timeout.poll_unpin(cx) {
            Poll::Ready(Ok(_)) => return Poll::Ready((
                self.user_data.take().expect("Future not to be polled again once ready."),
                Err(ProtocolsHandlerUpgrErr::Timeout)),
            ),
            Poll::Ready(Err(_)) => return Poll::Ready((
                self.user_data.take().expect("Future not to be polled again once ready."),
                Err(ProtocolsHandlerUpgrErr::Timer)),
            ),
            Poll::Pending => {},
        }

        match self.upgrade.poll_unpin(cx) {
            Poll::Ready(Ok(upgrade)) => Poll::Ready((
                self.user_data.take().expect("Future not to be polled again once ready."),
                Ok(upgrade),
            )),
            Poll::Ready(Err(err)) => Poll::Ready((
                self.user_data.take().expect("Future not to be polled again once ready."),
                Err(ProtocolsHandlerUpgrErr::Upgrade(err)),
            )),
            Poll::Pending => Poll::Pending,
        }
    }
}


/// The options for a planned connection & handler shutdown.
///
/// A shutdown is planned anew based on the the return value of
/// [`ProtocolsHandler::connection_keep_alive`] of the underlying handler
/// after every invocation of [`ProtocolsHandler::poll`].
///
/// A planned shutdown is always postponed for as long as there are ingoing
/// or outgoing substreams being negotiated, i.e. it is a graceful, "idle"
/// shutdown.
enum Shutdown {
    /// No shutdown is planned.
    None,
    /// A shut down is planned as soon as possible.
    Asap,
    /// A shut down is planned for when a `Delay` has elapsed.
    Later(Delay, Instant)
}

/// Error generated by the `NodeHandlerWrapper`.
#[derive(Debug)]
pub enum NodeHandlerWrapperError<TErr> {
    /// The connection handler encountered an error.
    Handler(TErr),
    /// The connection keep-alive timeout expired.
    KeepAliveTimeout,
}

impl<TErr> From<TErr> for NodeHandlerWrapperError<TErr> {
    fn from(err: TErr) -> NodeHandlerWrapperError<TErr> {
        NodeHandlerWrapperError::Handler(err)
    }
}

impl<TErr> fmt::Display for NodeHandlerWrapperError<TErr>
where
    TErr: fmt::Display
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeHandlerWrapperError::Handler(err) => write!(f, "{}", err),
            NodeHandlerWrapperError::KeepAliveTimeout =>
                write!(f, "Connection closed due to expired keep-alive timeout."),
        }
    }
}

impl<TErr> error::Error for NodeHandlerWrapperError<TErr>
where
    TErr: error::Error + 'static
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            NodeHandlerWrapperError::Handler(err) => Some(err),
            NodeHandlerWrapperError::KeepAliveTimeout => None,
        }
    }
}

impl<TProtoHandler> ConnectionHandler for NodeHandlerWrapper<TProtoHandler>
where
    TProtoHandler: ProtocolsHandler,
{
    type InEvent = TProtoHandler::InEvent;
    type OutEvent = TProtoHandler::OutEvent;
    type Error = NodeHandlerWrapperError<TProtoHandler::Error>;
    type Substream = Substream<StreamMuxerBox>;
    // The first element of the tuple is the unique upgrade identifier
    // (see `unique_dial_upgrade_id`).
    type OutboundOpenInfo = (u64, TProtoHandler::OutboundOpenInfo, Duration);

    fn inject_substream(
        &mut self,
        substream: Self::Substream,
        endpoint: SubstreamEndpoint<Self::OutboundOpenInfo>,
    ) {
        match endpoint {
            SubstreamEndpoint::Listener => {
                let protocol = self.handler.listen_protocol();
                let timeout = *protocol.timeout();
                let (upgrade, user_data) = protocol.into_upgrade();
                let upgrade = upgrade::apply_inbound(substream, SendWrapper(upgrade));
                let timeout = Delay::new(timeout);
                self.negotiating_in.push(SubstreamUpgrade {
                    user_data: Some(user_data),
                    timeout,
                    upgrade,
                });
            }
            SubstreamEndpoint::Dialer((upgrade_id, user_data, timeout)) => {
                let pos = match self
                    .queued_dial_upgrades
                    .iter()
                    .position(|(id, _)| id == &upgrade_id)
                {
                    Some(p) => p,
                    None => {
                        debug_assert!(false, "Received an upgrade with an invalid upgrade ID");
                        return;
                    }
                };

                let (_, upgrade) = self.queued_dial_upgrades.remove(pos);
                let mut version = upgrade::Version::default();
                if let Some(v) = self.substream_upgrade_protocol_override {
                    if v != version {
                        log::debug!("Substream upgrade protocol override: {:?} -> {:?}", version, v);
                        version = v;
                    }
                }
                let upgrade = upgrade::apply_outbound(substream, upgrade, version);
                let timeout = Delay::new(timeout);
                self.negotiating_out.push(SubstreamUpgrade {
                    user_data: Some(user_data),
                    timeout,
                    upgrade,
                });
            }
        }
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        self.handler.inject_event(event);
    }

    fn inject_address_change(&mut self, new_address: &Multiaddr) {
        self.handler.inject_address_change(new_address);
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<
        Result<ConnectionHandlerEvent<Self::OutboundOpenInfo, Self::OutEvent>, Self::Error>
    > {
        while let Poll::Ready(Some((user_data, res))) = self.negotiating_in.poll_next_unpin(cx) {
            match res {
                Ok(upgrade) => self.handler.inject_fully_negotiated_inbound(upgrade, user_data),
                Err(err) => self.handler.inject_listen_upgrade_error(user_data, err),
            }
        }

        while let Poll::Ready(Some((user_data, res))) = self.negotiating_out.poll_next_unpin(cx) {
            match res {
                Ok(upgrade) => self.handler.inject_fully_negotiated_outbound(upgrade, user_data),
                Err(err) => self.handler.inject_dial_upgrade_error(user_data, err),
            }
        }

        // Poll the handler at the end so that we see the consequences of the method
        // calls on `self.handler`.
        let poll_result = self.handler.poll(cx);

        // Ask the handler whether it wants the connection (and the handler itself)
        // to be kept alive, which determines the planned shutdown, if any.
        match (&mut self.shutdown, self.handler.connection_keep_alive()) {
            (Shutdown::Later(timer, deadline), KeepAlive::Until(t)) =>
                if *deadline != t {
                    *deadline = t;
                    timer.reset_at(t)
                },
            (_, KeepAlive::Until(t)) => self.shutdown = Shutdown::Later(Delay::new_at(t), t),
            (_, KeepAlive::No) => self.shutdown = Shutdown::Asap,
            (_, KeepAlive::Yes) => self.shutdown = Shutdown::None
        };

        match poll_result {
            Poll::Ready(ProtocolsHandlerEvent::Custom(event)) => {
                return Poll::Ready(Ok(ConnectionHandlerEvent::Custom(event)));
            }
            Poll::Ready(ProtocolsHandlerEvent::OutboundSubstreamRequest { protocol }) => {
                let id = self.unique_dial_upgrade_id;
                let timeout = *protocol.timeout();
                self.unique_dial_upgrade_id += 1;
                let (upgrade, info) = protocol.into_upgrade();
                self.queued_dial_upgrades.push((id, SendWrapper(upgrade)));
                return Poll::Ready(Ok(
                    ConnectionHandlerEvent::OutboundSubstreamRequest((id, info, timeout)),
                ));
            }
            Poll::Ready(ProtocolsHandlerEvent::Close(err)) => return Poll::Ready(Err(err.into())),
            Poll::Pending => (),
        };

        // Check if the connection (and handler) should be shut down.
        // As long as we're still negotiating substreams, shutdown is always postponed.
        if self.negotiating_in.is_empty() && self.negotiating_out.is_empty() {
            match self.shutdown {
                Shutdown::None => {},
                Shutdown::Asap => return Poll::Ready(Err(NodeHandlerWrapperError::KeepAliveTimeout)),
                Shutdown::Later(ref mut delay, _) => match Future::poll(Pin::new(delay), cx) {
                    Poll::Ready(_) => return Poll::Ready(Err(NodeHandlerWrapperError::KeepAliveTimeout)),
                    Poll::Pending => {}
                }
            }
        }

        Poll::Pending
    }
}

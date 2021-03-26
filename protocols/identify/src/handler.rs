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

use crate::protocol::{
    IdentifyProtocol,
    IdentifyPushProtocol,
    IdentifyInfo,
    InboundPush,
    OutboundPush,
    ReplySubstream
};
use futures::prelude::*;
use libp2p_core::either::{
    EitherError,
    EitherOutput,
};
use libp2p_core::upgrade::{
    EitherUpgrade,
    InboundUpgrade,
    OutboundUpgrade,
    SelectUpgrade,
    UpgradeError,
};
use libp2p_swarm::{
    NegotiatedSubstream,
    KeepAlive,
    SubstreamProtocol,
    ProtocolsHandler,
    ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr
};
use smallvec::SmallVec;
use std::{io, pin::Pin, task::Context, task::Poll, time::Duration};
use wasm_timer::Delay;

/// Protocol handler for sending and receiving identification requests.
///
/// Outbound requests are sent periodically. The handler performs expects
/// at least one identification request to be answered by the remote before
/// permitting the underlying connection to be closed.
pub struct IdentifyHandler {
    /// Pending events to yield.
    events: SmallVec<[ProtocolsHandlerEvent<
            EitherUpgrade<IdentifyProtocol, IdentifyPushProtocol<OutboundPush>>,
            (),
            IdentifyHandlerEvent,
            io::Error,
    >; 4]>,

    /// Future that fires when we need to identify the node again.
    next_id: Delay,

    /// Whether the handler should keep the connection alive.
    keep_alive: KeepAlive,

    /// The interval of `next_id`, i.e. the recurrent delay.
    interval: Duration,
}

/// Event produced by the `IdentifyHandler`.
#[derive(Debug)]
pub enum IdentifyHandlerEvent {
    /// We obtained identification information from the remote.
    Identified(IdentifyInfo),
    /// We received a request for identification.
    Identify(ReplySubstream<NegotiatedSubstream>),
    /// Failed to identify the remote.
    IdentificationError(ProtocolsHandlerUpgrErr<io::Error>),
}

/// Identifying information of the local node that is pushed to a remote.
#[derive(Debug)]
pub struct IdentifyPush(pub IdentifyInfo);

impl IdentifyHandler {
    /// Creates a new `IdentifyHandler`.
    pub fn new(initial_delay: Duration, interval: Duration) -> Self {
        IdentifyHandler {
            events: SmallVec::new(),
            next_id: Delay::new(initial_delay),
            keep_alive: KeepAlive::Yes,
            interval,
        }
    }
}

impl ProtocolsHandler for IdentifyHandler {
    type InEvent = IdentifyPush;
    type OutEvent = IdentifyHandlerEvent;
    type Error = io::Error;
    type InboundProtocol = SelectUpgrade<IdentifyProtocol, IdentifyPushProtocol<InboundPush>>;
    type OutboundProtocol = EitherUpgrade<IdentifyProtocol, IdentifyPushProtocol<OutboundPush>>;
    type OutboundOpenInfo = ();
    type InboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(
            SelectUpgrade::new(
                IdentifyProtocol,
                IdentifyPushProtocol::inbound(),
            ), ())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        output: <Self::InboundProtocol as InboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::InboundOpenInfo
    ) {
        match output {
            EitherOutput::First(substream) => {
                self.events.push(
                    ProtocolsHandlerEvent::Custom(
                        IdentifyHandlerEvent::Identify(substream)))
            }
            EitherOutput::Second(info) => {
                self.events.push(
                    ProtocolsHandlerEvent::Custom(
                        IdentifyHandlerEvent::Identified(info)))
            }
        }
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        output: <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Output,
        _: Self::OutboundOpenInfo,
    ) {
        match output {
            EitherOutput::First(remote_info) => {
                self.events.push(
                    ProtocolsHandlerEvent::Custom(
                        IdentifyHandlerEvent::Identified(remote_info)));
                self.keep_alive = KeepAlive::No;
            }
            EitherOutput::Second(()) => {}
        }
    }

    fn inject_event(&mut self, IdentifyPush(push): Self::InEvent) {
        self.events.push(ProtocolsHandlerEvent::OutboundSubstreamRequest {
            protocol: SubstreamProtocol::new(
                EitherUpgrade::B(
                    IdentifyPushProtocol::outbound(push)), ())
        });
    }

    fn inject_dial_upgrade_error(
        &mut self,
        _info: Self::OutboundOpenInfo,
        err: ProtocolsHandlerUpgrErr<
            <Self::OutboundProtocol as OutboundUpgrade<NegotiatedSubstream>>::Error
        >
    ) {
        let err = err.map_upgrade_err(|e| match e {
            UpgradeError::Select(e) => UpgradeError::Select(e),
            UpgradeError::Apply(EitherError::A(ioe)) => UpgradeError::Apply(ioe),
            UpgradeError::Apply(EitherError::B(ioe)) => UpgradeError::Apply(ioe),
        });
        self.events.push(
            ProtocolsHandlerEvent::Custom(
                IdentifyHandlerEvent::IdentificationError(err)));
        self.keep_alive = KeepAlive::No;
        self.next_id.reset(self.interval);
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.keep_alive
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            IdentifyHandlerEvent,
            Self::Error,
        >,
    > {
        if !self.events.is_empty() {
            return Poll::Ready(
                self.events.remove(0),
            );
        }

        // Poll the future that fires when we need to identify the node again.
        match Future::poll(Pin::new(&mut self.next_id), cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => {
                self.next_id.reset(self.interval);
                let ev = ProtocolsHandlerEvent::OutboundSubstreamRequest {
                    protocol: SubstreamProtocol::new(EitherUpgrade::A(IdentifyProtocol), ())
                };
                Poll::Ready(ev)
            }
            Poll::Ready(Err(err)) => Poll::Ready(ProtocolsHandlerEvent::Close(err.into()))
        }
    }
}

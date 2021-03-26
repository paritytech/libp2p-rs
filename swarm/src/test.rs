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

use crate::{
    NetworkBehaviour,
    NetworkBehaviourAction,
    ProtocolsHandler,
    IntoProtocolsHandler,
    PollParameters
};
use libp2p_core::{
    ConnectedPoint,
    PeerId,
    connection::{ConnectionId, ListenerId},
    multiaddr::Multiaddr,
};
use std::collections::HashMap;
use std::task::{Context, Poll};

/// A `MockBehaviour` is a `NetworkBehaviour` that allows for
/// the instrumentation of return values, without keeping
/// any further state.
pub struct MockBehaviour<THandler, TOutEvent>
where
    THandler: ProtocolsHandler,
{
    /// The prototype protocols handler that is cloned for every
    /// invocation of `new_handler`.
    pub handler_proto: THandler,
    /// The addresses to return from `addresses_of_peer`.
    pub addresses: HashMap<PeerId, Vec<Multiaddr>>,
    /// The next action to return from `poll`.
    ///
    /// An action is only returned once.
    pub next_action: Option<NetworkBehaviourAction<THandler::InEvent, TOutEvent>>,
}

impl<THandler, TOutEvent> MockBehaviour<THandler, TOutEvent>
where
    THandler: ProtocolsHandler
{
    pub fn new(handler_proto: THandler) -> Self {
        MockBehaviour {
            handler_proto,
            addresses: HashMap::new(),
            next_action: None,
        }
    }
}

impl<THandler, TOutEvent> NetworkBehaviour for MockBehaviour<THandler, TOutEvent>
where
    THandler: ProtocolsHandler + Clone,
    THandler::OutEvent: Clone,
    TOutEvent: Send + 'static,
{
    type ProtocolsHandler = THandler;
    type OutEvent = TOutEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.handler_proto.clone()
    }

    fn addresses_of_peer(&mut self, p: &PeerId) -> Vec<Multiaddr> {
        self.addresses.get(p).map_or(Vec::new(), |v| v.clone())
    }

    fn inject_connected(&mut self, _: &PeerId) {
    }

    fn inject_disconnected(&mut self, _: &PeerId) {
    }

    fn inject_event(&mut self, _: PeerId, _: ConnectionId, _: THandler::OutEvent) {
    }

    fn poll(&mut self, _: &mut Context, _: &mut impl PollParameters) ->
        Poll<NetworkBehaviourAction<THandler::InEvent, Self::OutEvent>>
    {
        self.next_action.take().map_or(Poll::Pending, Poll::Ready)
    }
}

/// A `CallTraceBehaviour` is a `NetworkBehaviour` that tracks
/// invocations of callback methods and their arguments, wrapping
/// around an inner behaviour.
pub struct CallTraceBehaviour<TInner>
where
    TInner: NetworkBehaviour,
{
    inner: TInner,

    pub addresses_of_peer: Vec<PeerId>,
    pub inject_connected: Vec<PeerId>,
    pub inject_disconnected: Vec<PeerId>,
    pub inject_connection_established: Vec<(PeerId, ConnectionId, ConnectedPoint)>,
    pub inject_connection_closed: Vec<(PeerId, ConnectionId, ConnectedPoint)>,
    pub inject_event: Vec<(PeerId, ConnectionId, <<TInner::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent)>,
    pub inject_addr_reach_failure: Vec<(Option<PeerId>, Multiaddr)>,
    pub inject_dial_failure: Vec<PeerId>,
    pub inject_new_listener: Vec<ListenerId>,
    pub inject_new_listen_addr: Vec<(ListenerId, Multiaddr)>,
    pub inject_new_external_addr: Vec<Multiaddr>,
    pub inject_expired_listen_addr: Vec<(ListenerId, Multiaddr)>,
    pub inject_expired_external_addr: Vec<Multiaddr>,
    pub inject_listener_error: Vec<ListenerId>,
    pub inject_listener_closed: Vec<(ListenerId, bool)>,
    pub poll: usize,
}

impl<TInner> CallTraceBehaviour<TInner>
where
    TInner: NetworkBehaviour
{
    pub fn new(inner: TInner) -> Self {
        Self {
            inner,
            addresses_of_peer: Vec::new(),
            inject_connected: Vec::new(),
            inject_disconnected: Vec::new(),
            inject_connection_established: Vec::new(),
            inject_connection_closed: Vec::new(),
            inject_event: Vec::new(),
            inject_addr_reach_failure: Vec::new(),
            inject_dial_failure: Vec::new(),
            inject_new_listener: Vec::new(),
            inject_new_listen_addr: Vec::new(),
            inject_new_external_addr: Vec::new(),
            inject_expired_listen_addr: Vec::new(),
            inject_expired_external_addr: Vec::new(),
            inject_listener_error: Vec::new(),
            inject_listener_closed: Vec::new(),
            poll: 0,
        }
    }

    pub fn reset(&mut self) {
        self.addresses_of_peer = Vec::new();
        self.inject_connected = Vec::new();
        self.inject_disconnected = Vec::new();
        self.inject_connection_established = Vec::new();
        self.inject_connection_closed = Vec::new();
        self.inject_event = Vec::new();
        self.inject_addr_reach_failure = Vec::new();
        self.inject_dial_failure = Vec::new();
        self.inject_new_listen_addr = Vec::new();
        self.inject_new_external_addr = Vec::new();
        self.inject_expired_listen_addr = Vec::new();
        self.inject_listener_error = Vec::new();
        self.inject_listener_closed = Vec::new();
        self.poll = 0;
    }
}

impl<TInner> NetworkBehaviour for CallTraceBehaviour<TInner>
where
    TInner: NetworkBehaviour,
    <<TInner::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent: Clone,
{
    type ProtocolsHandler = TInner::ProtocolsHandler;
    type OutEvent = TInner::OutEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.inner.new_handler()
    }

    fn addresses_of_peer(&mut self, p: &PeerId) -> Vec<Multiaddr> {
        self.addresses_of_peer.push(p.clone());
        self.inner.addresses_of_peer(p)
    }

    fn inject_connected(&mut self, peer: &PeerId) {
        self.inject_connected.push(peer.clone());
        self.inner.inject_connected(peer);
    }

    fn inject_connection_established(&mut self, p: &PeerId, c: &ConnectionId, e: &ConnectedPoint) {
        self.inject_connection_established.push((p.clone(), c.clone(), e.clone()));
        self.inner.inject_connection_established(p, c, e);
    }

    fn inject_disconnected(&mut self, peer: &PeerId) {
        self.inject_disconnected.push(peer.clone());
        self.inner.inject_disconnected(peer);
    }

    fn inject_connection_closed(&mut self, p: &PeerId, c: &ConnectionId, e: &ConnectedPoint) {
        self.inject_connection_closed.push((p.clone(), c.clone(), e.clone()));
        self.inner.inject_connection_closed(p, c, e);
    }

    fn inject_event(&mut self, p: PeerId, c: ConnectionId, e: <<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent) {
        self.inject_event.push((p.clone(), c.clone(), e.clone()));
        self.inner.inject_event(p, c, e);
    }

    fn inject_addr_reach_failure(&mut self, p: Option<&PeerId>, a: &Multiaddr, e: &dyn std::error::Error) {
        self.inject_addr_reach_failure.push((p.cloned(), a.clone()));
        self.inner.inject_addr_reach_failure(p, a, e);
    }

    fn inject_dial_failure(&mut self, p: &PeerId) {
        self.inject_dial_failure.push(p.clone());
        self.inner.inject_dial_failure(p);
    }

    fn inject_new_listener(&mut self, id: ListenerId) {
        self.inject_new_listener.push(id);
        self.inner.inject_new_listener(id);
    }

    fn inject_new_listen_addr(&mut self, id: ListenerId, a: &Multiaddr) {
        self.inject_new_listen_addr.push((id, a.clone()));
        self.inner.inject_new_listen_addr(id, a);
    }

    fn inject_expired_listen_addr(&mut self, id: ListenerId, a: &Multiaddr) {
        self.inject_expired_listen_addr.push((id, a.clone()));
        self.inner.inject_expired_listen_addr(id, a);
    }

    fn inject_new_external_addr(&mut self, a: &Multiaddr) {
        self.inject_new_external_addr.push(a.clone());
        self.inner.inject_new_external_addr(a);
    }

    fn inject_expired_external_addr(&mut self, a: &Multiaddr) {
        self.inject_expired_external_addr.push(a.clone());
        self.inner.inject_expired_external_addr(a);
    }

    fn inject_listener_error(&mut self, l: ListenerId, e: &(dyn std::error::Error + 'static)) {
        self.inject_listener_error.push(l.clone());
        self.inner.inject_listener_error(l, e);
    }

    fn inject_listener_closed(&mut self, l: ListenerId, r: Result<(), &std::io::Error>) {
        self.inject_listener_closed.push((l, r.is_ok()));
        self.inner.inject_listener_closed(l, r);
    }

    fn poll(&mut self, cx: &mut Context, args: &mut impl PollParameters) ->
        Poll<NetworkBehaviourAction<
            <<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent,
            Self::OutEvent
        >>
    {
        self.poll += 1;
        self.inner.poll(cx, args)
    }
}

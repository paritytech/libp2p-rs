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

//! Limit the number of requests peers can send to each other.
//!
//! Each peer is assigned a budget for sending and a budget for receiving
//! requests. Initially a peer assumes it has a send budget of 1. When its
//! budget has been used up its remote peer will send a credit message which
//! informs it how many more requests it can send before it needs to wait for
//! the next credit message. Credit messages which error or time out are
//! retried until they have reached the peer which is assumed once a
//! corresponding ack or a new request has been received from the peer.
//!
//! The `Throttled` behaviour wraps an existing `RequestResponse` behaviour
//! and uses a codec implementation that sends ordinary requests and responses
//! as well as a special credit message to which an ack message is expected
//! as a response. It does so by putting a small CBOR encoded header in front
//! of each message the inner codec produces.

mod codec;

use codec::{Codec, Message, ProtocolWrapper, Type};
use crate::handler::{RequestProtocol, RequestResponseHandler, RequestResponseHandlerEvent};
use futures::ready;
use libp2p_core::{ConnectedPoint, connection::ConnectionId, Multiaddr, PeerId};
use libp2p_swarm::{NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use lru::LruCache;
use std::{collections::{HashMap, HashSet, VecDeque}, task::{Context, Poll}};
use std::{cmp::max, num::NonZeroU16};
use super::{
    ProtocolSupport,
    RequestId,
    RequestResponse,
    RequestResponseCodec,
    RequestResponseConfig,
    RequestResponseEvent,
    RequestResponseMessage,
};

pub type ResponseChannel<R> = super::ResponseChannel<Message<R>>;

/// A wrapper around [`RequestResponse`] which adds request limits per peer.
pub struct Throttled<C>
where
    C: RequestResponseCodec + Send,
    C::Protocol: Sync
{
    /// A random id used for logging.
    id: u32,
    /// The wrapped behaviour.
    behaviour: RequestResponse<Codec<C>>,
    /// Information per peer.
    peer_info: HashMap<PeerId, PeerInfo>,
    /// Information about previously connected peers.
    offline_peer_info: LruCache<PeerId, PeerInfo>,
    /// The default limit applies to all peers unless overriden.
    default_limit: Limit,
    /// Permanent limit overrides per peer.
    limit_overrides: HashMap<PeerId, Limit>,
    /// Pending events to report in `Throttled::poll`.
    events: VecDeque<Event<C::Request, C::Response, Message<C::Response>>>,
    /// The current credit ID.
    next_grant_id: u64
}

/// Information about a credit grant that is sent to remote peers.
#[derive(Clone, Copy, Debug)]
struct Grant {
    /// The grant ID. Used to deduplicate retransmitted credit grants.
    id: GrantId,
    /// The ID of the outbound credit grant message.
    request: RequestId,
    /// The credit given in this grant, i.e. the number of additional
    /// requests the remote is allowed to send.
    credit: u16
}

/// Max. number of inbound requests that can be received.
#[derive(Clone, Copy, Debug)]
struct Limit {
    /// The current receive limit.
    max_recv: NonZeroU16,
    /// The next receive limit which becomes active after
    /// the current limit has been reached.
    next_max: NonZeroU16
}

impl Limit {
    /// Create a new limit.
    fn new(max: NonZeroU16) -> Self {
        // The max. limit provided will be effective after the initial request
        // from a peer which is always allowed has been answered. Values greater
        // than 1 would prevent sending the credit grant, leading to a stalling
        // sender so we must not use `max` right away.
        Limit {
            max_recv: NonZeroU16::new(1).expect("1 > 0"),
            next_max: max
        }
    }

    /// Set a new limit.
    ///
    /// The new limit becomes effective when all current inbound
    /// requests have been processed and replied to.
    fn set(&mut self, next: NonZeroU16) {
        self.next_max = next
    }

    /// Activate the new limit.
    fn switch(&mut self) -> u16 {
        self.max_recv = self.next_max;
        self.max_recv.get()
    }
}

type GrantId = u64;

/// Information related to the current send budget with a peer.
#[derive(Clone, Debug)]
struct SendBudget {
    /// The last received credit grant.
    grant: Option<GrantId>,
    /// The remaining credit for requests to send.
    remaining: u16,
    /// Credit grant requests received and acknowledged where the outcome
    /// of the acknowledgement (i.e. response sent) is still undetermined.
    /// Used to avoid emitting events for successful (`ResponseSent`) or failed
    /// acknowledgements.
    received: HashSet<RequestId>,
}

/// Information related to the current receive budget with a peer.
#[derive(Clone, Debug)]
struct RecvBudget {
    /// The grant currently given to the remote but yet to be acknowledged.
    ///
    /// Set to `Some` when a new grant is sent to the remote, followed
    /// by `None` when an acknowledgment or a request is received. The
    /// latter is seen as an implicit acknowledgement.
    grant: Option<Grant>,
    /// The limit for new credit grants when the `remaining` credit is
    /// exhausted.
    limit: Limit,
    /// The remaining credit for requests to receive.
    remaining: u16,
    /// Credit grants sent whose outcome is still undetermined.
    /// Used to avoid emitting events for failed credit grants.
    ///
    /// > **Note**: While receiving an inbound request is an implicit
    /// > acknowledgement for the last sent `grant`, the outcome of
    /// > the outbound request remains undetermined until a success or
    /// > failure event is received for that request or the corresponding
    /// > connection closes.
    sent: HashSet<RequestId>,
}

/// Budget information about a peer.
#[derive(Clone, Debug)]
struct PeerInfo {
    send_budget: SendBudget,
    recv_budget: RecvBudget,
}

impl PeerInfo {
    fn new(recv_limit: Limit) -> Self {
        PeerInfo {
            send_budget: SendBudget {
                grant: None,
                remaining: 1,
                received: HashSet::new(),
            },
            recv_budget: RecvBudget {
                grant: None,
                limit: recv_limit,
                remaining: 1,
                sent: HashSet::new(),
            }
        }
    }

    fn into_disconnected(mut self) -> Self {
        self.send_budget.received = HashSet::new();
        self.send_budget.remaining = 1;
        self.recv_budget.sent = HashSet::new();
        self.recv_budget.remaining = max(1, self.recv_budget.remaining);
        // Since we potentially reset the remaining receive budget,
        // we forget about the potentially still unacknowledged last grant.
        self.recv_budget.grant = None;
        self
    }
}

impl<C> Throttled<C>
where
    C: RequestResponseCodec + Send + Clone,
    C::Protocol: Sync
{
    /// Create a new throttled request-response behaviour.
    pub fn new<I>(c: C, protos: I, cfg: RequestResponseConfig) -> Self
    where
        I: IntoIterator<Item = (C::Protocol, ProtocolSupport)>,
        C: Send,
        C::Protocol: Sync
    {
        let protos = protos.into_iter().map(|(p, ps)| (ProtocolWrapper::new(b"/t/1", p), ps));
        Throttled::from(RequestResponse::new(Codec::new(c, 8192), protos, cfg))
    }

    /// Wrap an existing `RequestResponse` behaviour and apply send/recv limits.
    pub fn from(behaviour: RequestResponse<Codec<C>>) -> Self {
        Throttled {
            id: rand::random(),
            behaviour,
            peer_info: HashMap::new(),
            offline_peer_info: LruCache::new(8192),
            default_limit: Limit::new(NonZeroU16::new(1).expect("1 > 0")),
            limit_overrides: HashMap::new(),
            events: VecDeque::new(),
            next_grant_id: 0
        }
    }

    /// Set the global default receive limit per peer.
    pub fn set_receive_limit(&mut self, limit: NonZeroU16) {
        log::trace!("{:08x}: new default limit: {:?}", self.id, limit);
        self.default_limit = Limit::new(limit)
    }

    /// Override the receive limit of a single peer.
    pub fn override_receive_limit(&mut self, p: &PeerId, limit: NonZeroU16) {
        log::debug!("{:08x}: override limit for {}: {:?}", self.id, p, limit);
        if let Some(info) = self.peer_info.get_mut(p) {
            info.recv_budget.limit.set(limit)
        } else if let Some(info) = self.offline_peer_info.get_mut(p) {
            info.recv_budget.limit.set(limit)
        }
        self.limit_overrides.insert(*p, Limit::new(limit));
    }

    /// Remove any limit overrides for the given peer.
    pub fn remove_override(&mut self, p: &PeerId) {
        log::trace!("{:08x}: removing limit override for {}", self.id, p);
        self.limit_overrides.remove(p);
    }

    /// Has the limit of outbound requests been reached for the given peer?
    pub fn can_send(&mut self, p: &PeerId) -> bool {
        self.peer_info.get(p).map(|i| i.send_budget.remaining > 0).unwrap_or(true)
    }

    /// Send a request to a peer.
    ///
    /// If the limit of outbound requests has been reached, the request is
    /// returned. Sending more outbound requests should only be attempted
    /// once [`Event::ResumeSending`] has been received from [`NetworkBehaviour::poll`].
    pub fn send_request(&mut self, p: &PeerId, req: C::Request) -> Result<RequestId, C::Request> {
        let connected = &mut self.peer_info;
        let disconnected = &mut self.offline_peer_info;
        let remaining =
            if let Some(info) = connected.get_mut(p).or_else(|| disconnected.get_mut(p)) {
                if info.send_budget.remaining == 0 {
                    log::trace!("{:08x}: no more budget to send another request to {}", self.id, p);
                    return Err(req)
                }
                info.send_budget.remaining -= 1;
                info.send_budget.remaining
            } else {
                let limit = self.limit_overrides.get(p).copied().unwrap_or(self.default_limit);
                let mut info = PeerInfo::new(limit);
                info.send_budget.remaining -= 1;
                let remaining = info.send_budget.remaining;
                self.offline_peer_info.put(*p, info);
                remaining
            };

        let rid = self.behaviour.send_request(p, Message::request(req));

        log::trace! { "{:08x}: sending request {} to {} (budget remaining = {})",
            self.id,
            rid,
            p,
            remaining
        };

        Ok(rid)
    }

    /// Answer an inbound request with a response.
    ///
    /// See [`RequestResponse::send_response`] for details.
    pub fn send_response(&mut self, ch: ResponseChannel<C::Response>, res: C::Response)
        -> Result<(), C::Response>
    {
        log::trace!("{:08x}: sending response {} to peer {}", self.id, ch.request_id(), &ch.peer);
        if let Some(info) = self.peer_info.get_mut(&ch.peer) {
            if info.recv_budget.remaining == 0 { // need to send more credit to the remote peer
                let crd = info.recv_budget.limit.switch();
                info.recv_budget.remaining = info.recv_budget.limit.max_recv.get();
                self.send_credit(&ch.peer, crd);
            }
        }
        match self.behaviour.send_response(ch, Message::response(res)) {
            Ok(()) => Ok(()),
            Err(m) => Err(m.into_parts().1.expect("Missing response data.")),
        }
    }

    /// Add a known peer address.
    ///
    /// See [`RequestResponse::add_address`] for details.
    pub fn add_address(&mut self, p: &PeerId, a: Multiaddr) {
        self.behaviour.add_address(p, a)
    }

    /// Remove a previously added peer address.
    ///
    /// See [`RequestResponse::remove_address`] for details.
    pub fn remove_address(&mut self, p: &PeerId, a: &Multiaddr) {
        self.behaviour.remove_address(p, a)
    }

    /// Are we connected to the given peer?
    ///
    /// See [`RequestResponse::is_connected`] for details.
    pub fn is_connected(&self, p: &PeerId) -> bool {
        self.behaviour.is_connected(p)
    }

    /// Are we waiting for a response to the given request?
    ///
    /// See [`RequestResponse::is_pending_outbound`] for details.
    pub fn is_pending_outbound(&self, p: &PeerId, r: &RequestId) -> bool {
        self.behaviour.is_pending_outbound(p, r)
    }


    /// Is the remote waiting for the local node to respond to the given
    /// request?
    ///
    /// See [`RequestResponse::is_pending_inbound`] for details.
    pub fn is_pending_inbound(&self, p: &PeerId, r: &RequestId) -> bool {
        self.behaviour.is_pending_inbound(p, r)
    }

    /// Send a credit grant to the given peer.
    fn send_credit(&mut self, p: &PeerId, credit: u16) {
        if let Some(info) = self.peer_info.get_mut(p) {
            let cid = self.next_grant_id;
            self.next_grant_id += 1;
            let rid = self.behaviour.send_request(p, Message::credit(credit, cid));
            log::trace!("{:08x}: sending {} credit as grant {} to {}", self.id, credit, cid, p);
            let grant = Grant { id: cid, request: rid, credit };
            info.recv_budget.grant = Some(grant);
            info.recv_budget.sent.insert(rid);
        }
    }
}

/// A Wrapper around [`RequestResponseEvent`].
#[derive(Debug)]
pub enum Event<Req, Res, CRes = Res> {
    /// A regular request-response event.
    Event(RequestResponseEvent<Req, Res, CRes>),
    /// We received more inbound requests than allowed.
    TooManyInboundRequests(PeerId),
    /// When previously reaching the send limit of a peer,
    /// this event is eventually emitted when sending is
    /// allowed to resume.
    ResumeSending(PeerId)
}

impl<C> NetworkBehaviour for Throttled<C>
where
    C: RequestResponseCodec + Send + Clone + 'static,
    C::Protocol: Sync
{
    type ProtocolsHandler = RequestResponseHandler<Codec<C>>;
    type OutEvent = Event<C::Request, C::Response, Message<C::Response>>;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        self.behaviour.new_handler()
    }

    fn addresses_of_peer(&mut self, p: &PeerId) -> Vec<Multiaddr> {
        self.behaviour.addresses_of_peer(p)
    }

    fn inject_connection_established(&mut self, p: &PeerId, id: &ConnectionId, end: &ConnectedPoint) {
        self.behaviour.inject_connection_established(p, id, end)
    }

    fn inject_connection_closed(&mut self, peer: &PeerId, id: &ConnectionId, end: &ConnectedPoint) {
        self.behaviour.inject_connection_closed(peer, id, end);
        if let Some(info) = self.peer_info.get_mut(peer) {
            if let Some(grant) = &mut info.recv_budget.grant {
                log::debug! { "{:08x}: resending credit grant {} to {} after connection closed",
                    self.id,
                    grant.id,
                    peer
                };
                let msg = Message::credit(grant.credit, grant.id);
                grant.request = self.behaviour.send_request(peer, msg)
            }
        }
    }

    fn inject_connected(&mut self, p: &PeerId) {
        log::trace!("{:08x}: connected to {}", self.id, p);
        self.behaviour.inject_connected(p);
        // The limit may have been added by `Throttled::send_request` already.
        if !self.peer_info.contains_key(p) {
            if let Some(info) = self.offline_peer_info.pop(p) {
                let recv_budget = info.recv_budget.remaining;
                self.peer_info.insert(*p, info);
                if recv_budget > 1 {
                    self.send_credit(p, recv_budget - 1);
                }
            } else {
                let limit = self.limit_overrides.get(p).copied().unwrap_or(self.default_limit);
                self.peer_info.insert(*p, PeerInfo::new(limit));
            }
        }
    }

    fn inject_disconnected(&mut self, p: &PeerId) {
        log::trace!("{:08x}: disconnected from {}", self.id, p);
        if let Some(info) = self.peer_info.remove(p) {
            self.offline_peer_info.put(*p, info.into_disconnected());
        }
        self.behaviour.inject_disconnected(p)
    }

    fn inject_dial_failure(&mut self, p: &PeerId) {
        self.behaviour.inject_dial_failure(p)
    }

    fn inject_event(&mut self, p: PeerId, i: ConnectionId, e: RequestResponseHandlerEvent<Codec<C>>) {
        self.behaviour.inject_event(p, i, e)
    }

    fn poll(&mut self, cx: &mut Context<'_>, params: &mut impl PollParameters)
        -> Poll<NetworkBehaviourAction<RequestProtocol<Codec<C>>, Self::OutEvent>>
    {
        loop {
            if let Some(ev) = self.events.pop_front() {
                return Poll::Ready(NetworkBehaviourAction::GenerateEvent(ev))
            } else if self.events.capacity() > super::EMPTY_QUEUE_SHRINK_THRESHOLD {
                self.events.shrink_to_fit()
            }

            let event = match ready!(self.behaviour.poll(cx, params)) {
                | NetworkBehaviourAction::GenerateEvent(RequestResponseEvent::Message { peer, message }) => {
                    let message = match message {
                        | RequestResponseMessage::Response { request_id, response } =>
                            match &response.header().typ {
                                | Some(Type::Ack) => {
                                    if let Some(info) = self.peer_info.get_mut(&peer) {
                                        if let Some(id) = info.recv_budget.grant.as_ref().map(|c| c.id) {
                                            if Some(id) == response.header().ident {
                                                log::trace!("{:08x}: received ack {} from {}", self.id, id, peer);
                                                info.recv_budget.grant = None;
                                            }
                                        }
                                        info.recv_budget.sent.remove(&request_id);
                                    }
                                    continue
                                }
                                | Some(Type::Response) => {
                                    log::trace!("{:08x}: received response {} from {}", self.id, request_id, peer);
                                    if let Some(rs) = response.into_parts().1 {
                                        RequestResponseMessage::Response { request_id, response: rs }
                                    } else {
                                        log::error! { "{:08x}: missing data for response {} from peer {}",
                                            self.id,
                                            request_id,
                                            peer
                                        }
                                        continue
                                    }
                                }
                                | ty => {
                                    log::trace! {
                                        "{:08x}: unknown message type: {:?} from {}; expected response or credit",
                                        self.id,
                                        ty,
                                        peer
                                    };
                                    continue
                                }
                            }
                        | RequestResponseMessage::Request { request_id, request, channel } =>
                            match &request.header().typ {
                                | Some(Type::Credit) => {
                                    if let Some(info) = self.peer_info.get_mut(&peer) {
                                        let id = if let Some(n) = request.header().ident {
                                            n
                                        } else {
                                            log::warn! { "{:08x}: missing credit id in message from {}",
                                                self.id,
                                                peer
                                            }
                                            continue
                                        };
                                        let credit = request.header().credit.unwrap_or(0);
                                        log::trace! { "{:08x}: received {} additional credit {} from {}",
                                            self.id,
                                            credit,
                                            id,
                                            peer
                                        };
                                        if info.send_budget.grant < Some(id) {
                                            if info.send_budget.remaining == 0 && credit > 0 {
                                                log::trace!("{:08x}: sending to peer {} can resume", self.id, peer);
                                                self.events.push_back(Event::ResumeSending(peer))
                                            }
                                            info.send_budget.remaining += credit;
                                            info.send_budget.grant = Some(id);
                                        }
                                        // Note: Failing to send a response to a credit grant is
                                        // handled along with other inbound failures further below.
                                        let _ = self.behaviour.send_response(channel, Message::ack(id));
                                        info.send_budget.received.insert(request_id);
                                    }
                                    continue
                                }
                                | Some(Type::Request) => {
                                    if let Some(info) = self.peer_info.get_mut(&peer) {
                                        log::trace! { "{:08x}: received request {} (recv. budget = {})",
                                            self.id,
                                            request_id,
                                            info.recv_budget.remaining
                                        };
                                        if info.recv_budget.remaining == 0 {
                                            log::debug!("{:08x}: peer {} exceeds its budget", self.id, peer);
                                            self.events.push_back(Event::TooManyInboundRequests(peer));
                                            continue
                                        }
                                        info.recv_budget.remaining -= 1;
                                        // We consider a request as proof that our credit grant has
                                        // reached the peer. Usually, an ACK has already been
                                        // received.
                                        info.recv_budget.grant = None;
                                    }
                                    if let Some(rq) = request.into_parts().1 {
                                        RequestResponseMessage::Request { request_id, request: rq, channel }
                                    } else {
                                        log::error! { "{:08x}: missing data for request {} from peer {}",
                                            self.id,
                                            request_id,
                                            peer
                                        }
                                        continue
                                    }
                                }
                                | ty => {
                                    log::trace! {
                                        "{:08x}: unknown message type: {:?} from {}; expected request or ack",
                                        self.id,
                                        ty,
                                        peer
                                    };
                                    continue
                                }
                            }
                    };
                    let event = RequestResponseEvent::Message { peer, message };
                    NetworkBehaviourAction::GenerateEvent(Event::Event(event))
                }
                | NetworkBehaviourAction::GenerateEvent(RequestResponseEvent::OutboundFailure {
                    peer,
                    request_id,
                    error
                }) => {
                    if let Some(info) = self.peer_info.get_mut(&peer) {
                        if let Some(grant) = info.recv_budget.grant.as_mut() {
                            if grant.request == request_id {
                                log::debug! {
                                    "{:08x}: failed to send {} as credit {} to {}; retrying...",
                                    self.id,
                                    grant.credit,
                                    grant.id,
                                    peer
                                };
                                let msg = Message::credit(grant.credit, grant.id);
                                grant.request = self.behaviour.send_request(&peer, msg);
                            }
                        }

                        // If the outbound failure was for a credit message, don't report it on
                        // the public API and retry the sending.
                        if info.recv_budget.sent.remove(&request_id) {
                            continue
                        }
                    }
                    let event = RequestResponseEvent::OutboundFailure { peer, request_id, error };
                    NetworkBehaviourAction::GenerateEvent(Event::Event(event))
                }
                | NetworkBehaviourAction::GenerateEvent(RequestResponseEvent::InboundFailure {
                    peer,
                    request_id,
                    error
                }) => {
                    // If the inbound failure occurred in the context of responding to a
                    // credit grant, don't report it on the public API.
                    if let Some(info) = self.peer_info.get_mut(&peer) {
                        if info.send_budget.received.remove(&request_id) {
                            log::debug! {
                                "{:08}: failed to acknowledge credit grant from {}: {:?}",
                                self.id, peer, error
                            };
                            continue
                        }
                    }
                    let event = RequestResponseEvent::InboundFailure { peer, request_id, error };
                    NetworkBehaviourAction::GenerateEvent(Event::Event(event))
                }
                | NetworkBehaviourAction::GenerateEvent(RequestResponseEvent::ResponseSent {
                    peer,
                    request_id
                }) => {
                    // If this event is for an ACK response that was sent for
                    // the last received credit grant, skip it.
                    if let Some(info) = self.peer_info.get_mut(&peer) {
                        if info.send_budget.received.remove(&request_id) {
                            log::trace! {
                                "{:08}: successfully sent ACK for credit grant {:?}.",
                                self.id,
                                info.send_budget.grant,
                            }
                            continue
                        }
                    }
                    NetworkBehaviourAction::GenerateEvent(Event::Event(
                        RequestResponseEvent::ResponseSent { peer, request_id }))
                }
                | NetworkBehaviourAction::DialAddress { address } =>
                    NetworkBehaviourAction::DialAddress { address },
                | NetworkBehaviourAction::DialPeer { peer_id, condition } =>
                    NetworkBehaviourAction::DialPeer { peer_id, condition },
                | NetworkBehaviourAction::NotifyHandler { peer_id, handler, event } =>
                    NetworkBehaviourAction::NotifyHandler { peer_id, handler, event },
                | NetworkBehaviourAction::ReportObservedAddr { address, score } =>
                    NetworkBehaviourAction::ReportObservedAddr { address, score }
            };

            return Poll::Ready(event)
        }
    }
}

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

use crate::dns::{build_query, build_query_response, build_service_discovery_response};
use crate::query::MdnsPacket;
use async_io::{Async, Timer};
use futures::prelude::*;
use if_watch::{IfEvent, IfWatcher};
use lazy_static::lazy_static;
use libp2p_core::{
    address_translation, connection::ConnectionId, multiaddr::Protocol, Multiaddr, PeerId,
};
use libp2p_swarm::{
    protocols_handler::DummyProtocolsHandler, NetworkBehaviour, NetworkBehaviourAction,
    PollParameters, ProtocolsHandler,
};
use smallvec::SmallVec;
use socket2::{Domain, Socket, Type};
use std::{
    cmp,
    collections::VecDeque,
    fmt, io, iter,
    net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    pin::Pin,
    task::Context,
    task::Poll,
    time::{Duration, Instant},
};

lazy_static! {
    static ref IPV4_MDNS_MULTICAST_ADDRESS: SocketAddr =
        SocketAddr::from((Ipv4Addr::new(224, 0, 0, 251), 5353));
}

/// Configuration for mDNS.
#[derive(Clone, Debug)]
pub struct MdnsConfig {
    /// TTL to use for mdns records.
    pub ttl: Duration,
    /// Interval at which to poll the network for new peers. This isn't
    /// necessary during normal operation but avoids the case that an
    /// initial packet was lost and not discovering any peers until a new
    /// peer joins the network. Receiving an mdns packet resets the timer
    /// preventing unnecessary traffic.
    pub query_interval: Duration,
}

impl Default for MdnsConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(6 * 60),
            query_interval: Duration::from_secs(5 * 60),
        }
    }
}

/// A `NetworkBehaviour` for mDNS. Automatically discovers peers on the local network and adds
/// them to the topology.
#[derive(Debug)]
pub struct Mdns {
    /// Main socket for listening.
    recv_socket: Async<UdpSocket>,

    /// Query socket for making queries.
    send_socket: Async<UdpSocket>,

    /// Iface watcher.
    if_watch: IfWatcher,

    /// Buffer used for receiving data from the main socket.
    /// RFC6762 discourages packets larger than the interface MTU, but allows sizes of up to 9000
    /// bytes, if it can be ensured that all participating devices can handle such large packets.
    /// For computers with several interfaces and IP addresses responses can easily reach sizes in
    /// the range of 3000 bytes, so 4096 seems sensible for now. For more information see
    /// [rfc6762](https://tools.ietf.org/html/rfc6762#page-46).
    recv_buffer: [u8; 4096],

    /// Buffers pending to send on the main socket.
    send_buffer: VecDeque<Vec<u8>>,

    /// List of nodes that we have discovered, the address, and when their TTL expires.
    ///
    /// Each combination of `PeerId` and `Multiaddr` can only appear once, but the same `PeerId`
    /// can appear multiple times.
    discovered_nodes: SmallVec<[(PeerId, Multiaddr, Instant); 8]>,

    /// Future that fires when the TTL of at least one node in `discovered_nodes` expires.
    ///
    /// `None` if `discovered_nodes` is empty.
    closest_expiration: Option<Timer>,

    /// Queued events.
    events: VecDeque<MdnsEvent>,

    /// Discovery interval.
    query_interval: Duration,

    /// Record ttl.
    ttl: Duration,

    /// Discovery timer.
    timeout: Timer,
}

impl Mdns {
    /// Builds a new `Mdns` behaviour.
    pub async fn new(config: MdnsConfig) -> io::Result<Self> {
        let recv_socket = {
            let socket = Socket::new(
                Domain::IPV4,
                Type::DGRAM,
                Some(socket2::Protocol::UDP),
            )?;
            socket.set_reuse_address(true)?;
            #[cfg(unix)]
            socket.set_reuse_port(true)?;
            socket.bind(&SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 5353).into())?;
            let socket = UdpSocket::from(socket);
            socket.set_multicast_loop_v4(true)?;
            socket.set_multicast_ttl_v4(255)?;
            Async::new(socket)?
        };
        let send_socket = {
            let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0))?;
            Async::new(socket)?
        };
        let if_watch = if_watch::IfWatcher::new().await?;
        Ok(Self {
            recv_socket,
            send_socket,
            if_watch,
            recv_buffer: [0; 4096],
            send_buffer: Default::default(),
            discovered_nodes: SmallVec::new(),
            closest_expiration: None,
            events: Default::default(),
            query_interval: config.query_interval,
            ttl: config.ttl,
            timeout: Timer::interval(config.query_interval),
        })
    }

    /// Returns true if the given `PeerId` is in the list of nodes discovered through mDNS.
    pub fn has_node(&self, peer_id: &PeerId) -> bool {
        self.discovered_nodes().any(|p| p == peer_id)
    }

    /// Returns the list of nodes that we have discovered through mDNS and that are not expired.
    pub fn discovered_nodes(&self) -> impl ExactSizeIterator<Item = &PeerId> {
        self.discovered_nodes.iter().map(|(p, _, _)| p)
    }

    fn inject_mdns_packet(&mut self, packet: MdnsPacket, params: &impl PollParameters) {
        self.timeout.set_interval(self.query_interval);
        match packet {
            MdnsPacket::Query(query) => {
                for packet in build_query_response(
                    query.query_id(),
                    *params.local_peer_id(),
                    params.listened_addresses(),
                    self.ttl,
                ) {
                    self.send_buffer.push_back(packet);
                }
            }
            MdnsPacket::Response(response) => {
                // We replace the IP address with the address we observe the
                // remote as and the address they listen on.
                let obs_ip = Protocol::from(response.remote_addr().ip());
                let obs_port = Protocol::Udp(response.remote_addr().port());
                let observed: Multiaddr = iter::once(obs_ip).chain(iter::once(obs_port)).collect();

                let mut discovered: SmallVec<[_; 4]> = SmallVec::new();
                for peer in response.discovered_peers() {
                    if peer.id() == params.local_peer_id() {
                        continue;
                    }

                    let new_expiration = Instant::now() + peer.ttl();

                    let mut addrs: Vec<Multiaddr> = Vec::new();
                    for addr in peer.addresses() {
                        if let Some(new_addr) = address_translation(&addr, &observed) {
                            addrs.push(new_addr.clone())
                        }
                        addrs.push(addr.clone())
                    }

                    for addr in addrs {
                        if let Some((_, _, cur_expires)) = self
                            .discovered_nodes
                            .iter_mut()
                            .find(|(p, a, _)| p == peer.id() && *a == addr)
                        {
                            *cur_expires = cmp::max(*cur_expires, new_expiration);
                        } else {
                            self.discovered_nodes
                                .push((*peer.id(), addr.clone(), new_expiration));
                        }
                        discovered.push((*peer.id(), addr));
                    }
                }

                self.closest_expiration = self
                    .discovered_nodes
                    .iter()
                    .fold(None, |exp, &(_, _, elem_exp)| {
                        Some(exp.map(|exp| cmp::min(exp, elem_exp)).unwrap_or(elem_exp))
                    })
                    .map(Timer::at);

                self.events
                    .push_back(MdnsEvent::Discovered(DiscoveredAddrsIter {
                        inner: discovered.into_iter(),
                    }));
            }
            MdnsPacket::ServiceDiscovery(disc) => {
                let resp = build_service_discovery_response(disc.query_id(), self.ttl);
                self.send_buffer.push_back(resp);
            }
        }
    }
}

impl NetworkBehaviour for Mdns {
    type ProtocolsHandler = DummyProtocolsHandler;
    type OutEvent = MdnsEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        DummyProtocolsHandler::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        let now = Instant::now();
        self.discovered_nodes
            .iter()
            .filter(move |(p, _, expires)| p == peer_id && *expires > now)
            .map(|(_, addr, _)| addr.clone())
            .collect()
    }

    fn inject_connected(&mut self, _: &PeerId) {}

    fn inject_disconnected(&mut self, _: &PeerId) {}

    fn inject_event(
        &mut self,
        _: PeerId,
        _: ConnectionId,
        ev: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        void::unreachable(ev)
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
        params: &mut impl PollParameters,
    ) -> Poll<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        while let Poll::Ready(event) = Pin::new(&mut self.if_watch).poll(cx) {
            let multicast = From::from([224, 0, 0, 251]);
            let socket = self.recv_socket.get_ref();
            match event {
                Ok(IfEvent::Up(inet)) => {
                    if inet.addr().is_loopback() {
                        continue;
                    }
                    if let IpAddr::V4(addr) = inet.addr() {
                        log::trace!("joining multicast on iface {}", addr);
                        if let Err(err) = socket.join_multicast_v4(&multicast, &addr) {
                            log::error!("join multicast failed: {}", err);
                        } else {
                            self.send_buffer.push_back(build_query());
                        }
                    }
                }
                Ok(IfEvent::Down(inet)) => {
                    if inet.addr().is_loopback() {
                        continue;
                    }
                    if let IpAddr::V4(addr) = inet.addr() {
                        log::trace!("leaving multicast on iface {}", addr);
                        if let Err(err) = socket.leave_multicast_v4(&multicast, &addr) {
                            log::error!("leave multicast failed: {}", err);
                        }
                    }
                }
                Err(err) => log::error!("if watch returned an error: {}", err),
            }
        }
        // Poll receive socket.
        while self.recv_socket.poll_readable(cx).is_ready() {
            match self
                .recv_socket
                .recv_from(&mut self.recv_buffer)
                .now_or_never()
            {
                Some(Ok((len, from))) => {
                    if let Some(packet) = MdnsPacket::new_from_bytes(&self.recv_buffer[..len], from)
                    {
                        self.inject_mdns_packet(packet, params);
                    }
                }
                Some(Err(err)) => log::error!("Failed reading datagram: {}", err),
                _ => {}
            }
        }
        if Pin::new(&mut self.timeout).poll_next(cx).is_ready() {
            self.send_buffer.push_back(build_query());
        }
        // Send responses.
        if !self.send_buffer.is_empty() {
            while self.send_socket.poll_writable(cx).is_ready() {
                if let Some(packet) = self.send_buffer.pop_front() {
                    match self
                        .send_socket
                        .send_to(&packet, *IPV4_MDNS_MULTICAST_ADDRESS)
                        .now_or_never()
                    {
                        Some(Ok(_)) => {}
                        Some(Err(err)) => log::error!("{}", err),
                        None => self.send_buffer.push_front(packet),
                    }
                } else {
                    break;
                }
            }
        }
        // Emit discovered event.
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
        }
        // Emit expired event.
        if let Some(ref mut closest_expiration) = self.closest_expiration {
            if let Poll::Ready(now) = Pin::new(closest_expiration).poll(cx) {
                let mut expired = SmallVec::<[(PeerId, Multiaddr); 4]>::new();
                while let Some(pos) = self
                    .discovered_nodes
                    .iter()
                    .position(|(_, _, exp)| *exp < now)
                {
                    let (peer_id, addr, _) = self.discovered_nodes.remove(pos);
                    expired.push((peer_id, addr));
                }

                if !expired.is_empty() {
                    let event = MdnsEvent::Expired(ExpiredAddrsIter {
                        inner: expired.into_iter(),
                    });

                    return Poll::Ready(NetworkBehaviourAction::GenerateEvent(event));
                }
            }
        }
        Poll::Pending
    }
}

/// Event that can be produced by the `Mdns` behaviour.
#[derive(Debug)]
pub enum MdnsEvent {
    /// Discovered nodes through mDNS.
    Discovered(DiscoveredAddrsIter),

    /// The given combinations of `PeerId` and `Multiaddr` have expired.
    ///
    /// Each discovered record has a time-to-live. When this TTL expires and the address hasn't
    /// been refreshed, we remove it from the list and emit it as an `Expired` event.
    Expired(ExpiredAddrsIter),
}

/// Iterator that produces the list of addresses that have been discovered.
pub struct DiscoveredAddrsIter {
    inner: smallvec::IntoIter<[(PeerId, Multiaddr); 4]>,
}

impl Iterator for DiscoveredAddrsIter {
    type Item = (PeerId, Multiaddr);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for DiscoveredAddrsIter {}

impl fmt::Debug for DiscoveredAddrsIter {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("DiscoveredAddrsIter").finish()
    }
}

/// Iterator that produces the list of addresses that have expired.
pub struct ExpiredAddrsIter {
    inner: smallvec::IntoIter<[(PeerId, Multiaddr); 4]>,
}

impl Iterator for ExpiredAddrsIter {
    type Item = (PeerId, Multiaddr);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for ExpiredAddrsIter {}

impl fmt::Debug for ExpiredAddrsIter {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ExpiredAddrsIter").finish()
    }
}

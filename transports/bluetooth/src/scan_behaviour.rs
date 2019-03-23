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

use crate::scan::Scan;
use futures::prelude::*;
use libp2p_core::protocols_handler::{DummyProtocolsHandler, ProtocolsHandler};
use libp2p_core::swarm::{ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p_core::{Multiaddr, PeerId};
use std::{fmt, io, marker::PhantomData, time::Duration, time::Instant};
use tokio_io::{AsyncRead, AsyncWrite};

/// A network behaviour that discovers nearby libp2p-compatible Bluetooth devices.
pub struct BluetoothDiscovery<TSubstream> {
    /// The current scan, if any.
    /// We start with `None` so that creating a new `BluetoothDiscovery` always succeeds.
    current_scan: Option<Scan>,

    /// Known nearby Bluetooth nodes, and when their TTL expires.
    known_nodes: Vec<(PeerId, Multiaddr, Instant)>,

    /// Duration for addresses.
    ttl: Duration,

    /// Marker to pin the generic.
    marker: PhantomData<TSubstream>,
}

impl<TSubstream> BluetoothDiscovery<TSubstream> {
    /// Builds a new `BluetoothDiscovery`.
    pub fn new() -> io::Result<BluetoothDiscovery<TSubstream>> {
        Ok(BluetoothDiscovery {
            current_scan: None,
            known_nodes: Vec::with_capacity(16),
            ttl: Duration::from_secs(120),
            marker: PhantomData,
        })
    }

    /// Returns true if the given `PeerId` is in the list of nodes discovered through Bluetooth.
    pub fn has_node(&self, peer_id: &PeerId) -> bool {
        self.known_nodes.iter().any(|(p, _, _)| p == peer_id)
    }
}

/// Event that can be produced by the `BluetoothDiscovery`.
#[derive(Debug)]
pub enum BluetoothEvent {
    /// Discovered Bluetooth nodes.
    Discovered {
        peer_id: PeerId,
        address: Multiaddr,
    },

    /// The given combinations of `PeerId` and `Multiaddr` have expired.
    // TODO: never triggered
    Expired(ExpiredAddrsIter),
}

/// Iterator that produces the list of addresses that have been discovered.
pub struct DiscoveredAddrsIter {
    inner: smallvec::IntoIter<[(PeerId, Multiaddr); 4]>
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

impl ExactSizeIterator for DiscoveredAddrsIter {
}

impl fmt::Debug for DiscoveredAddrsIter {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("DiscoveredAddrsIter")
            .finish()
    }
}

/// Iterator that produces the list of addresses that have expired.
pub struct ExpiredAddrsIter {
    inner: smallvec::IntoIter<[(PeerId, Multiaddr); 4]>
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

impl ExactSizeIterator for ExpiredAddrsIter {
}

impl fmt::Debug for ExpiredAddrsIter {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("ExpiredAddrsIter")
            .finish()
    }
}

impl<TSubstream> NetworkBehaviour for BluetoothDiscovery<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = DummyProtocolsHandler<TSubstream>;
    type OutEvent = BluetoothEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        DummyProtocolsHandler::default()
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        let now = Instant::now();
        self.known_nodes
            .iter()
            .filter(move |(p, _, expires)| p == peer_id && *expires > now)
            .map(|(_, addr, _)| addr.clone())
            .collect()
    }

    fn inject_connected(&mut self, _: PeerId, _: ConnectedPoint) {}

    fn inject_disconnected(&mut self, _: &PeerId, _: ConnectedPoint) {}

    fn inject_node_event(
        &mut self,
        _: PeerId,
        _ev: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        void::unreachable(_ev)
    }

    fn poll(
        &mut self,
        params: &mut PollParameters<'_>,
    ) -> Async<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        /*// Remove expired peers.
        if let Some(ref mut closest_expiration) = self.closest_expiration {
            match closest_expiration.poll() {
                Ok(Async::Ready(())) => {
                    let now = Instant::now();
                    let mut expired = SmallVec::<[(PeerId, Multiaddr); 4]>::new();
                    while let Some(pos) = self.discovered_nodes.iter().position(|(_, _, exp)| *exp < now) {
                        let (peer_id, addr, _) = self.discovered_nodes.remove(pos);
                        expired.push((peer_id, addr));
                    }

                    if !expired.is_empty() {
                        let event = BluetoothEvent::Expired(ExpiredAddrsIter {
                            inner: expired.into_iter(),
                        });

                        return Async::Ready(NetworkBehaviourAction::GenerateEvent(event));
                    }
                },
                Ok(Async::NotReady) => (),
                Err(err) => warn!("tokio timer has errored: {:?}", err),
            }
        }*/

        // Creating a scan, if none is started.
        let current_scan = match self.current_scan {
            Some(ref mut s) => s,
            ref mut s @ None => {
                *s = Some(Scan::new().unwrap());
                s.as_mut().expect("We just inserted Some in the Option")
            }
        };

        // Polling the current scan.
        match current_scan.poll() {
            Async::Ready(Some((addr, peer_id))) => {
                if let Some(existing) = self.known_nodes.iter_mut().find(|(p, a, _)| *p == peer_id && *a == addr) {
                    existing.2 = Instant::now() + self.ttl;
                } else {
                    self.known_nodes.push((peer_id.clone(), addr.clone(), Instant::now() + self.ttl));
                    return Async::Ready(NetworkBehaviourAction::GenerateEvent(BluetoothEvent::Discovered {
                        peer_id,
                        address: addr,
                    }))
                }
            },
            Async::Ready(None) => self.current_scan = Some(Scan::new().unwrap()),     // TODO: don't unwrap
            Async::NotReady => (),
        }

        Async::NotReady
    }
}

impl<TSubstream> fmt::Debug for BluetoothDiscovery<TSubstream> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("BluetoothDiscovery")
            .finish()
    }
}

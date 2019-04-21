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

use crate::handler::{BrahmsHandler, BrahmsHandlerEvent, BrahmsHandlerIn};
use crate::sampler::Sampler;
use crate::view::View;
use fnv::{FnvHashMap, FnvHashSet};
use futures::prelude::*;
use libp2p_core::swarm::{
    ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters,
};
use libp2p_core::{Multiaddr, PeerId};
use rand::seq::IteratorRandom;
use smallvec::SmallVec;
use std::{cmp, iter, marker::PhantomData, mem, time::Duration, time::Instant};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::Interval;

/// Configuration for the Brahms discovery mechanism.
#[derive(Debug, Copy, Clone)]
pub struct BrahmsConfig<TInitViewIter> {
    /// Configuration for the size of the view.
    pub view_size: BrahmsViewSize,

    /// Duration of a round. Should be at least twice the maximum expected latency between two
    /// nodes, plus the time to generate `alpha` proofs of work.
    pub round_duration: Duration,

    /// Number of samplers. The more the better, but the more CPU-intensive the algorithm is.
    pub num_samplers: u32,

    /// Number of leading zero bytes that are required in the proof-of-work calculation used to
    /// prevent DDoS attacks. The higher the value, the more resistant we are, but the more
    /// CPU-intensive the code is.
    ///
    /// The correct value to use here depends on the round duration, the size of the view, and the
    /// speed of CPUs. We need to generate `alpha` proofs in less than `round_duration - latency`.
    ///
    /// As an example, on a modern machine, with a value of 16 it takes around 1ms to create a
    /// proof.
    pub difficulty: u8,

    /// Iterator containing the nodes of the initial view. Only the first `alpha + beta + gamma`
    /// elements will be taken into account.
    ///
    /// The list of nodes in the initial view is pretty important for the safety of the network,
    /// as all the nodes that are discovered by Brahms afterwards will be derived from this initial
    /// view. In other words, if all the nodes of the initial view are malicious, then Brahms may
    /// only ever discover malicious nodes. However Brahms is designed so that a single
    /// well-behaving node is enough for the entire network of well-behaving nodes to be
    /// discovered.
    pub initial_view: TInitViewIter,
}

/// Configuration for the view size of the Brahms discovery mechanism.
///
/// The maximum number of elements in the view will be `alpha + beta + gamma`.
///
/// Note that if a node receives receives more than `alpha` pushes, it will assume that the network
/// is under attack. It is therefore important that the configuration is the same throughout the
/// network, most notably the value of `alpha`.
#[derive(Debug, Copy, Clone)]
pub struct BrahmsViewSize {
    /// Number of elements in the view that are the result of pushes. Must not be 0. Typically
    /// 45% of the size of the view.
    pub alpha: u32,

    /// Number of elements in the view that are the result of pulls. Must not be 0. Typically
    /// 45% of the size of the view.
    pub beta: u32,

    /// Number of elements in the view that are the result of sampling. Typically 10% of the size
    /// of the view.
    pub gamma: u32,
}

impl<TInitViewIter> BrahmsConfig<TInitViewIter> {
    /// Builds a "default" BrahmsConfig from an initial list of nodes.
    pub fn default_from_initial_view(initial_view: TInitViewIter) -> Self {
        BrahmsConfig {
            view_size: BrahmsViewSize {
                alpha: 14,
                beta: 14,
                gamma: 4,
            },
            round_duration: Duration::from_secs(10),
            num_samplers: 32,
            difficulty: 10,
            initial_view,
        }
    }
}

impl BrahmsViewSize {
    /// Returns the total view size.
    pub fn total(&self) -> u32 {
        self.alpha.saturating_add(self.beta).saturating_add(self.gamma)
    }

    /// Builds a view size.
    pub fn from_network_size(size: u64) -> Self {
        let cubic_root = (0u64..).find(|&n| n.saturating_mul(n).saturating_mul(n) >= size)
            .expect("We always find a value whose cube power is superior of equal to size; QED");
        let alpha_beta = 45 * cubic_root / 100;
        let gamma = cubic_root / 10;

        debug_assert!(alpha_beta < u64::from(u32::max_value()));
        let alpha_beta = cmp::max(8, alpha_beta as u32);
        debug_assert!(gamma < u64::from(u32::max_value()));
        let gamma = cmp::max(1, gamma as u32);

        BrahmsViewSize {
            alpha: alpha_beta,
            beta: alpha_beta,
            gamma,
        }
    }
}

/// Brahms discovery algorithm behaviour.
pub struct Brahms<TSubstream> {
    /// Same value as in the configuration. Never modified.
    difficulty: u8,

    /// Configuration for the size of the view.
    view_size: BrahmsViewSize,
    /// Configuration to use for the next round. Copied into `config` at the beginning of each
    /// round.
    next_round_view_size: BrahmsViewSize,
    /// PeerId of the local node.
    // TODO: field can be removed after a rework of NetworkBehaviour
    local_peer_id: PeerId,

    /// The view contains our local view of the whole network. This is public over the network.
    view: View,

    /// Contains the peers we know about.
    sampler: Sampler<PeerIdAdapter>,

    /// Interval to advance round.
    // TODO: randomize the time (see https://www.net.in.tum.de/fileadmin/bibtex/publications/theses/totakura2015_brahms.pdf)
    round_advance: Interval,

    /// List of values that other nodes have spontaneously pushed to us.
    ///
    /// Contrary to the original Brahms paper, never contains twice the same `PeerId`.
    /// Never contains the local peer ID.
    push_list: FnvHashMap<PeerId, SmallVec<[Multiaddr; 8]>>,

    /// List of values that we pulled from other nodes.
    ///
    /// Contrary to the original Brahms paper, never contains twice the same `PeerId`.
    /// Never contains the local peer ID.
    pull_list: FnvHashMap<PeerId, SmallVec<[Multiaddr; 8]>>,

    /// List of all nodes we're connected to.
    connected_peers: FnvHashSet<PeerId>,
    /// Nodes we want to connect to because we want to put them in `pending_pushes`.
    push_pending_connects: SmallVec<[PeerId; 32]>,
    /// Nodes we want to connect to because we want to put them in `pending_pulls`.
    pull_pending_connects: SmallVec<[PeerId; 32]>,
    /// List of peers we want to send a push to.
    pending_pushes: SmallVec<[PeerId; 32]>,
    /// List of peers we want to send a pull request to.
    pending_pulls: SmallVec<[PeerId; 32]>,
    /// List of pull requests we received and that need to be answered.
    pull_requests_to_respond: SmallVec<[PeerId; 32]>,

    /// Marker to pin the generic.
    marker: PhantomData<TSubstream>,
}

/// Event that happened in the Brahms behaviour.
#[derive(Debug)]
pub enum BrahmsEvent {
    /// A round of Brahms has been processed.
    Round(RoundResult),
}

/// Outcome of a round.
#[derive(Debug)]
pub struct RoundResult {
    /// List of peers that have been added to the view.
    pub added: Vec<PeerId>,
    /// List of peers that have been removed from the view.
    pub removed: Vec<PeerId>,
}

/// Wraps around a `PeerId` so that it implements `AsRef<[u8]>`.
#[derive(Debug, Clone)]
struct PeerIdAdapter(PeerId);

impl AsRef<[u8]> for PeerIdAdapter {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<TSubstream> Brahms<TSubstream> {
    /// Initializes the Brahms state.
    pub fn new(config: BrahmsConfig<impl IntoIterator<Item = (PeerId, Multiaddr)>>, local_peer_id: PeerId) -> Self {
        let round_advance = Interval::new(Instant::now(), config.round_duration);

        let view = config.initial_view
            .into_iter()
            .filter(|(peer, _)| *peer != local_peer_id)
            .collect::<View>();

        let mut sampler = Sampler::with_len(config.num_samplers);
        for peer in view.peers() {
            sampler.insert(PeerIdAdapter(peer.id().clone()));
        }

        Brahms {
            difficulty: config.difficulty,
            view_size: config.view_size,
            next_round_view_size: config.view_size,
            local_peer_id,
            sampler,
            round_advance,
            view,
            push_list: FnvHashMap::with_capacity_and_hasher(config.view_size.alpha as usize, Default::default()),
            pull_list: FnvHashMap::with_capacity_and_hasher(config.view_size.beta.saturating_mul(config.view_size.total()) as usize, Default::default()),
            connected_peers: FnvHashSet::default(),
            push_pending_connects: SmallVec::new(),
            pull_pending_connects: SmallVec::new(),
            pending_pushes: SmallVec::new(),
            pending_pulls: SmallVec::new(),
            pull_requests_to_respond: SmallVec::new(),
            marker: PhantomData,
        }
    }

    /// Modifies the configuration of Brahms. Only starts applying after the next round.
    pub fn set_config(&mut self, config: BrahmsViewSize) {
        self.next_round_view_size = config;

        if self.push_list.len() < config.alpha as usize {
            self.push_list.reserve(config.alpha as usize - self.push_list.len());
        }
    }

    /// Returns the local view of the network.
    ///
    /// Contains a maximum of `alpha + beta + gamma` elements (where `alpha`, `beta` and `gamma`
    /// come from the configuration). Never contains any duplicate.
    ///
    /// Assuming that the whole network uses Brahms, there should be a direct or indirect link
    /// between every single node of the network and at least one of the nodes returned by
    /// `view()`.
    ///
    /// The list of nodes that are returned can change after you call `poll()`.
    #[inline]
    pub fn view(&self) -> impl Iterator<Item = &PeerId> {
        self.view.peers().map(|peer| peer.id())
    }

    /// Internal function. Called at a regular interval. Updates the view with the previous round's
    /// responses and advances to the next round.
    fn advance_round(&mut self, parameters: &mut PollParameters) -> RoundResult {
        // Return value of the function.
        let mut outcome = RoundResult {
            added: Vec::new(),
            removed: Vec::new(),
        };

        // Update the view if necessary.
        // TODO: figure out bootstrapping
        if self.push_list.len() <= self.view_size.alpha as usize //&&
            //(self.view.is_empty() || (!self.push_list.is_empty() && !self.pull_list.is_empty()))
        {
            // Clear `self.view` and put the former view in `old_view`.
            let old_view = {
                // We don't just copy the capacity of `self.view` because the view size can be
                // changed by the user.
                let view_size = self.view_size.total();
                mem::replace(&mut self.view, View::with_capacity(view_size as usize))
            };

            // Move elements from `push_list` to `self.view`.
            for (peer_id, addrs) in self.push_list.iter().choose_multiple(&mut rand::thread_rng(), self.view_size.alpha as usize) {
                debug_assert_ne!(peer_id, parameters.local_peer_id());
                self.view.insert(peer_id, addrs.iter().cloned())
            }

            // Move elements from `pull_list` to `self.view`.
            for (peer_id, addrs) in self.pull_list.iter().choose_multiple(&mut rand::thread_rng(), self.view_size.beta as usize) {
                if peer_id != parameters.local_peer_id() {
                    self.view.insert(peer_id, addrs.iter().cloned())
                }
            }

            // Move elements from the sampler.
            for _ in 0..self.view_size.gamma {
                if let Some(elem) = self.sampler.sample() {
                    debug_assert_ne!(elem.0, self.local_peer_id);
                    if let Some(peer) = old_view.peer(&elem.0) {
                        self.view.insert(&elem.0, peer.addrs().cloned());
                    } else {
                        self.view.insert(&elem.0, iter::empty());
                    }
                }
            }

            // TODO: for each element in `self.view` not in `old_view`, we send a "keep-alive enable"
            // message.
            /*for peer in self.view.keys() {
                if !old_view.contains_key(peer) {

                }
            }*/

            let (a, r) = self.view.diff(&old_view);
            outcome.added = a.into_iter().cloned().collect();
            outcome.removed = r.into_iter().cloned().collect();
        }

        // Update the samplers and clear `push_list`.
        for (peer_id, _) in self.push_list.drain() {
            debug_assert_ne!(peer_id, *parameters.local_peer_id());
            self.sampler.insert(PeerIdAdapter(peer_id));
        }

        // Update the samplers and clear `push_list`.
        for (peer_id, _) in self.pull_list.drain() {
            if peer_id != *parameters.local_peer_id() {
                self.sampler.insert(PeerIdAdapter(peer_id));
            }
        }

        // Reset for next round.
        self.view_size = self.next_round_view_size;

        // Send push requests.
        for peer in self.view.pick_random_peers(self.view_size.alpha as usize) {
            if self.connected_peers.contains(peer.id()) {
                if !self.pending_pushes.contains(peer.id()) {
                    self.pending_pushes.push(peer.id().clone());
                }
            } else if !self.push_pending_connects.contains(peer.id()) {
                self.push_pending_connects.push(peer.id().clone());
            }
        }

        // Send pull requests.
        for peer in self.view.pick_random_peers(self.view_size.beta as usize) {
            if self.connected_peers.contains(peer.id()) {
                if !self.pending_pulls.contains(peer.id()) {
                    self.pending_pulls.push(peer.id().clone());
                }
            } else if !self.pull_pending_connects.contains(peer.id()) {
                self.pull_pending_connects.push(peer.id().clone());
            }
        }

        outcome
    }
}

impl<TSubstream> NetworkBehaviour for Brahms<TSubstream>
where
    TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = BrahmsHandler<TSubstream>;
    type OutEvent = BrahmsEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        BrahmsHandler::new(self.local_peer_id.clone(), self.difficulty)
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        self.view.peer(peer_id).map(|peer| peer.addrs().cloned().collect::<Vec<_>>()).unwrap_or_default()
    }

    fn inject_connected(&mut self, peer_id: PeerId, _: ConnectedPoint) {
        let wasnt_in = self.connected_peers.insert(peer_id);
        debug_assert!(wasnt_in);
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, _: ConnectedPoint) {
        let was_in = self.connected_peers.remove(peer_id);
        debug_assert!(was_in);
    }

    fn inject_node_event(&mut self, peer_id: PeerId, event: BrahmsHandlerEvent) {
        match event {
            BrahmsHandlerEvent::Push { addresses, .. } => {
                debug_assert_ne!(peer_id, self.local_peer_id);
                let list = self.push_list.entry(peer_id).or_default();
                for addr in addresses {
                    if list.iter().all(|a| *a != addr) {
                        list.push(addr);
                    }
                }
            }
            BrahmsHandlerEvent::PullRequest => {
                self.pull_requests_to_respond.push(peer_id);
            }
            BrahmsHandlerEvent::PullResult { list } => {
                for (peer_id, addresses) in list {
                    if peer_id == self.local_peer_id {
                        continue;
                    }
                    let list = self.push_list.entry(peer_id).or_default();
                    for addr in addresses {
                        if list.iter().all(|a| *a != addr) {
                            list.push(addr);
                        }
                    }
                }
            }
        }
    }

    fn poll(
        &mut self,
        parameters: &mut PollParameters,
    ) -> Async<NetworkBehaviourAction<BrahmsHandlerIn, Self::OutEvent>> {
        // TODO: we need to send BrahmsHandlerIn::Enable/DisableKeepAlive

        if !self.pull_requests_to_respond.is_empty() {
            let peer_id = self.pull_requests_to_respond.remove(0);
            let local_addresses = parameters
                .listened_addresses()
                .chain(parameters.external_addresses())
                .cloned()
                .collect();
            let local_peer_id = parameters.local_peer_id().clone();
            let response = self
                .view
                .peers()
                .map(|peer| (peer.id().clone(), peer.addrs().cloned().collect()))
                .chain(iter::once((local_peer_id, local_addresses)))
                .collect();
            return Async::Ready(
                NetworkBehaviourAction::SendEvent {
                    peer_id,
                    event: BrahmsHandlerIn::Event(BrahmsHandlerEvent::PullResult { list: response }),
                },
            );
        }

        if !self.push_pending_connects.is_empty() {
            let peer_id = self.push_pending_connects.remove(0);
            self.pending_pushes.push(peer_id.clone());
            return Async::Ready(NetworkBehaviourAction::DialPeer { peer_id });
        }

        if !self.pull_pending_connects.is_empty() {
            let peer_id = self.pull_pending_connects.remove(0);
            self.pending_pulls.push(peer_id.clone());
            return Async::Ready(NetworkBehaviourAction::DialPeer { peer_id });
        }

        if let Some(pos) = self.pending_pushes.iter().position(|p| self.connected_peers.contains(p)) {
            let peer_id = self.pending_pushes.remove(pos);
            return Async::Ready(NetworkBehaviourAction::SendEvent {
                peer_id: peer_id.clone(),
                event: BrahmsHandlerIn::Event(BrahmsHandlerEvent::Push {
                    addresses: parameters
                        .listened_addresses()
                        .chain(parameters.external_addresses())
                        .cloned()
                        .collect(),
                    local_peer_id: parameters.local_peer_id().clone(),
                    remote_peer_id: peer_id.clone(),
                    pow_difficulty: self.difficulty,
                }),
            });
        }

        if let Some(pos) = self.pending_pulls.iter().position(|p| self.connected_peers.contains(p)) {
            let peer_id = self.pending_pulls.remove(pos);
            return Async::Ready(NetworkBehaviourAction::SendEvent {
                peer_id,
                event: BrahmsHandlerIn::Event(BrahmsHandlerEvent::PullRequest),
            });
        }

        match self.round_advance.poll() {
            Ok(Async::NotReady) => (),
            Ok(Async::Ready(Some(_))) => {
                let round_result = self.advance_round(parameters);
                return Async::Ready(NetworkBehaviourAction::GenerateEvent(
                    BrahmsEvent::Round(round_result),
                ));
            }
            Ok(Async::Ready(None)) | Err(_) => (),
        }

        Async::NotReady
    }
}

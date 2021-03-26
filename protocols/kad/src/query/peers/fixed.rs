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

use super::*;

use fnv::FnvHashMap;
use libp2p_core::PeerId;
use std::{vec, collections::hash_map::Entry, num::NonZeroUsize};

/// A peer iterator for a fixed set of peers.
pub struct FixedPeersIter {
    /// Ther permitted parallelism, i.e. number of pending results.
    parallelism: NonZeroUsize,

    /// The state of peers emitted by the iterator.
    peers: FnvHashMap<PeerId, PeerState>,

    /// The backlog of peers that can still be emitted.
    iter: vec::IntoIter<PeerId>,

    /// The internal state of the iterator.
    state: State,
}

#[derive(Debug, PartialEq, Eq)]
enum State {
    Waiting { num_waiting: usize },
    Finished
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum PeerState {
    /// The iterator is waiting for a result to be reported back for the peer.
    Waiting,

    /// The iterator has been informed that the attempt to contact the peer failed.
    Failed,

    /// The iterator has been informed of a successful result from the peer.
    Succeeded,
}

impl FixedPeersIter {
    pub fn new<I>(peers: I, parallelism: NonZeroUsize) -> Self
    where
        I: IntoIterator<Item = PeerId>
    {
        let peers = peers.into_iter().collect::<Vec<_>>();

        Self {
            parallelism,
            peers: FnvHashMap::default(),
            iter: peers.into_iter(),
            state: State::Waiting { num_waiting: 0 },
        }
    }

    /// Callback for delivering the result of a successful request to a peer.
    ///
    /// If the iterator is currently waiting for a result from `peer`,
    /// the iterator state is updated and `true` is returned. In that
    /// case, after calling this function, `next` should eventually be
    /// called again to obtain the new state of the iterator.
    ///
    /// If the iterator is finished, it is not currently waiting for a
    /// result from `peer`, or a result for `peer` has already been reported,
    /// calling this function has no effect and `false` is returned.
    pub fn on_success(&mut self, peer: &PeerId) -> bool {
        if let State::Waiting { num_waiting } = &mut self.state {
            if let Some(state @ PeerState::Waiting) = self.peers.get_mut(peer) {
                *state = PeerState::Succeeded;
                *num_waiting -= 1;
                return true
            }
        }
        false
    }

    /// Callback for informing the iterator about a failed request to a peer.
    ///
    /// If the iterator is currently waiting for a result from `peer`,
    /// the iterator state is updated and `true` is returned. In that
    /// case, after calling this function, `next` should eventually be
    /// called again to obtain the new state of the iterator.
    ///
    /// If the iterator is finished, it is not currently waiting for a
    /// result from `peer`, or a result for `peer` has already been reported,
    /// calling this function has no effect and `false` is returned.
    pub fn on_failure(&mut self, peer: &PeerId) -> bool {
        if let State::Waiting { num_waiting } = &mut self.state {
            if let Some(state @ PeerState::Waiting) = self.peers.get_mut(peer) {
                *state = PeerState::Failed;
                *num_waiting -= 1;
                return true
            }
        }
        false
    }

    pub fn is_waiting(&self, peer: &PeerId) -> bool {
        self.peers.get(peer) == Some(&PeerState::Waiting)
    }

    pub fn finish(&mut self) {
        if let State::Waiting { .. } = self.state {
            self.state = State::Finished
        }
    }

    /// Checks whether the iterator has finished.
    pub fn is_finished(&self) -> bool {
        self.state == State::Finished
    }

    pub fn next(&mut self) -> PeersIterState<'_> {
        match &mut self.state {
            State::Finished => PeersIterState::Finished,
            State::Waiting { num_waiting } => {
                if *num_waiting >= self.parallelism.get() {
                    return PeersIterState::WaitingAtCapacity
                }
                loop {
                    match self.iter.next() {
                        None => if *num_waiting == 0 {
                            self.state = State::Finished;
                            return PeersIterState::Finished
                        } else {
                            return PeersIterState::Waiting(None)
                        }
                        Some(p) => match self.peers.entry(p) {
                            Entry::Occupied(_) => {} // skip duplicates
                            Entry::Vacant(e) => {
                                *num_waiting += 1;
                                e.insert(PeerState::Waiting);
                                return PeersIterState::Waiting(Some(Cow::Owned(p)))
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn into_result(self) -> impl Iterator<Item = PeerId> {
        self.peers.into_iter()
            .filter_map(|(p, s)|
                if let PeerState::Succeeded = s {
                    Some(p)
                } else {
                    None
                })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn decrease_num_waiting_on_failure() {
        let mut iter = FixedPeersIter::new(
            vec![PeerId::random(), PeerId::random()],
            NonZeroUsize::new(1).unwrap(),
        );

        match iter.next() {
            PeersIterState::Waiting(Some(peer)) => {
                let peer = peer.into_owned();
                iter.on_failure(&peer);
            },
            _ => panic!("Expected iterator to yield peer."),
        }

        match iter.next() {
            PeersIterState::Waiting(Some(_)) => {},
            PeersIterState::WaitingAtCapacity => panic!(
                "Expected iterator to return another peer given that the \
                 previous `on_failure` call should have allowed another peer \
                 to be queried.",
            ),
            _ => panic!("Expected iterator to yield peer."),
        }
    }
}

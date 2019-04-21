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

use fnv::FnvHashMap;
use libp2p_core::{Multiaddr, PeerId};
use rand::seq::IteratorRandom;
use smallvec::SmallVec;
use std::{fmt, hash::Hash, iter::FromIterator};

/// Contains a view of the network.
pub struct View<TPeerId = PeerId> {
    inner: FnvHashMap<TPeerId, SmallVec<[Multiaddr; 8]>>,
}

impl<TPeerId> Default for View<TPeerId>
where
    TPeerId: Eq + Hash + Clone,
{
    #[inline]
    fn default() -> View<TPeerId> {
        View::empty()
    }
}

impl<TPeerId> View<TPeerId>
where
    TPeerId: Eq + Hash + Clone,
{
    /// Creates an empty view.
    #[inline]
    pub fn empty() -> Self {
        View {
            inner: FnvHashMap::default(),
        }
    }

    /// Creates a view with a capacity of the given number of peers.
    #[inline]
    pub fn with_capacity(num_peers: usize) -> Self {
        View {
            inner: FnvHashMap::with_capacity_and_hasher(num_peers, Default::default())
        }
    }

    /// Returns true if the view is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Inserts addresses for the given peer in the view. The peer is added even if the list of
    /// addresses is empty.
    pub fn insert(&mut self, peer_id: &TPeerId, addrs: impl IntoIterator<Item = Multiaddr>) {
        if let Some(list) = self.inner.get_mut(peer_id) {
            for addr in addrs {
                if list.iter().all(|a| *a != addr) {
                    list.push(addr);
                }
            }
        } else {
            self.inner.insert(peer_id.clone(), addrs.into_iter().collect());
        }
    }

    /// Returns a list of the peers of the view.
    pub fn peers(&self) -> impl ExactSizeIterator<Item = Peer<TPeerId>> {
        self.inner
            .iter()
            .map(|(peer, addrs)| Peer { peer, addrs })
    }

    /// Returns a peer by its ID, if it is present in the view.
    pub fn peer<'a>(&'a self, id: &'a TPeerId) -> Option<Peer<'a, TPeerId>> {
        self.inner
            .get(id)
            .map(|addrs| Peer { peer: id, addrs })
    }

    /// Randomly picks `num` elements in the list and returns them.
    ///
    /// Will return less elements if the view is to small.
    ///
    /// The order in which elements are returned is neither stable nor fully random.
    pub fn pick_random_peers(&self, num: usize) -> impl ExactSizeIterator<Item = Peer<TPeerId>> {
        self.inner
            .iter()
            .choose_multiple(&mut rand::thread_rng(), num)
            .into_iter()
            .map(|(peer, addrs)| Peer { peer, addrs })
    }

    /// Finds the elements that are in `self` but not in `others`, and vice versa.
    // TODO: better API
    pub fn diff<'a>(&'a self, other: &'a View<TPeerId>) -> (Vec<&'a TPeerId>, Vec<&'a TPeerId>) {
        let mut added = Vec::new();
        let mut removed = Vec::new();

        for peer_id in self.inner.keys() {
            if !other.inner.contains_key(peer_id) {
                added.push(peer_id);
            }
        }

        for peer_id in other.inner.keys() {
            if !self.inner.contains_key(peer_id) {
                removed.push(peer_id);
            }
        }

        (added, removed)
    }
}

impl<TPeerId> FromIterator<(TPeerId, Multiaddr)> for View<TPeerId>
where
    TPeerId: Eq + Hash + Clone,
{
    fn from_iter<TIter: IntoIterator<Item = (TPeerId, Multiaddr)>>(iter: TIter) -> Self {
        let mut view = View::empty();
        for (peer, addr) in iter {
            view.insert(&peer, Some(addr));
        }
        view
    }
}

impl<TPeerId> fmt::Debug for View<TPeerId>
where
    TPeerId: Eq + Hash + Clone + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_map()
            .entries(self.inner.iter().map(|(peer_id, addrs)| (peer_id, addrs.as_slice())))
            .finish()
    }
}

/// Access to a peer in the view.
pub struct Peer<'a, TPeerId> {
    peer: &'a TPeerId,
    addrs: &'a SmallVec<[Multiaddr; 8]>,
}

impl<'a, TPeerId> Peer<'a, TPeerId>
where
    TPeerId: Eq + Hash + Clone,
{
    /// Returns the id of the peer.
    pub fn id(&self) -> &'a TPeerId {
        self.peer
    }

    /// Returns the known addresses of that peer.
    pub fn addrs(&self) -> impl ExactSizeIterator<Item = &'a Multiaddr> {
        self.addrs.iter()
    }
}

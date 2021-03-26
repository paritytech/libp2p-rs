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

use libp2p_core::Multiaddr;
use smallvec::SmallVec;
use std::fmt;

/// A non-empty list of (unique) addresses of a peer in the routing table.
#[derive(Clone)]
pub struct Addresses {
    addrs: SmallVec<[Multiaddr; 6]>,
}

#[allow(clippy::len_without_is_empty)]
impl Addresses {
    /// Creates a new list of addresses.
    pub fn new(addr: Multiaddr) -> Addresses {
        let mut addrs = SmallVec::new();
        addrs.push(addr);
        Addresses { addrs }
    }

    /// Gets a reference to the first address in the list.
    pub fn first(&self) -> &Multiaddr {
        &self.addrs[0]
    }

    /// Returns an iterator over the addresses.
    pub fn iter(&self) -> impl Iterator<Item = &Multiaddr> {
        self.addrs.iter()
    }

    /// Returns the number of addresses in the list.
    pub fn len(&self) -> usize {
        self.addrs.len()
    }

    /// Converts the addresses into a `Vec`.
    pub fn into_vec(self) -> Vec<Multiaddr> {
        self.addrs.into_vec()
    }

    /// Removes the given address from the list.
    ///
    /// Returns `Ok(())` if the address is either not in the list or was found and
    /// removed. Returns `Err(())` if the address is the last remaining address,
    /// which cannot be removed.
    ///
    /// An address should only be removed if is determined to be invalid or
    /// otherwise unreachable.
    pub fn remove(&mut self, addr: &Multiaddr) -> Result<(),()> {
        if self.addrs.len() == 1 {
            return Err(())
        }

        if let Some(pos) = self.addrs.iter().position(|a| a == addr) {
            self.addrs.remove(pos);
            if self.addrs.len() <= self.addrs.inline_size() {
                self.addrs.shrink_to_fit();
            }
        }

        Ok(())
    }

    /// Adds a new address to the end of the list.
    ///
    /// Returns true if the address was added, false otherwise (i.e. if the
    /// address is already in the list).
    pub fn insert(&mut self, addr: Multiaddr) -> bool {
        if self.addrs.iter().all(|a| *a != addr) {
            self.addrs.push(addr);
            true
        } else {
            false
        }
    }

    /// Replaces an old address with a new address.
    ///
    /// Returns true if the previous address was found and replaced with a clone
    /// of the new address, returns false otherwise.
    pub fn replace(&mut self, old: &Multiaddr, new: &Multiaddr) -> bool {
        if let Some(a) = self.addrs.iter_mut().find(|a| *a == old) {
            *a = new.clone();
            return true
        }

        false
    }
}

impl fmt::Debug for Addresses {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.addrs.iter())
            .finish()
    }
}

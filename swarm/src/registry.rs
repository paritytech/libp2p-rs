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
use std::{collections::VecDeque, cmp::Ordering, num::NonZeroUsize};
use std::ops::{Add, Sub};

/// A ranked collection of [`Multiaddr`] values.
///
/// Every address has an associated [score](`AddressScore`) and iterating
/// over the addresses will return them in order from highest to lowest score.
///
/// In addition to the currently held addresses and their score, the collection
/// keeps track of a limited history of the most-recently added addresses.
/// This history determines how address scores are reduced over time as old
/// scores expire in the context of new addresses being added:
///
///   * An address's score is increased by a given amount whenever it is
///     [(re-)added](Addresses::add) to the collection.
///   * An address's score is decreased by the same amount used when it
///     was added when the least-recently seen addition is (as per the
///     limited history) for this address in the context of [`Addresses::add`].
///   * If an address's score reaches 0 in the context of [`Addresses::add`],
///     it is removed from the collection.
///
#[derive(Debug, Clone)]
pub struct Addresses {
    /// The ranked sequence of addresses, from highest to lowest score.
    ///
    /// By design, the number of finitely scored addresses stored here is
    /// never larger (but may be smaller) than the number of historic `reports`
    /// at any time.
    registry: SmallVec<[AddressRecord; 8]>,
    /// The configured limit of the `reports` history of added addresses,
    /// and thus also of the size of the `registry` w.r.t. finitely scored
    /// addresses.
    limit: NonZeroUsize,
    /// The limited history of added addresses. If the queue reaches the `limit`,
    /// the first record, i.e. the least-recently added, is removed in the
    /// context of [`Addresses::add`] and the corresponding record in the
    /// `registry` has its score reduced accordingly.
    reports: VecDeque<Report>,
}

/// An record in a prioritised list of addresses.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct AddressRecord {
    pub addr: Multiaddr,
    pub score: AddressScore,
}

/// A report tracked for a finitely scored address.
#[derive(Debug, Clone)]
struct Report {
    addr: Multiaddr,
    score: u32,
}

impl AddressRecord {
    fn new(addr: Multiaddr, score: AddressScore) -> Self {
        AddressRecord {
            addr, score,
        }
    }
}

/// The "score" of an address w.r.t. an ordered collection of addresses.
///
/// A score is a measure of the trusworthyness of a particular
/// observation of an address. The same address may be repeatedly
/// reported with the same or differing scores.
#[derive(PartialEq, Eq, Debug, Clone, Copy, Hash)]
pub enum AddressScore {
    /// The score is "infinite", i.e. an address with this score is never
    /// purged from the associated address records and remains sorted at
    /// the beginning (possibly with other `Infinite`ly scored addresses).
    Infinite,
    /// The score is finite, i.e. an address with this score has
    /// its score increased and decreased as per the frequency of
    /// reports (i.e. additions) of the same address relative to
    /// the reports of other addresses.
    Finite(u32),
}

impl AddressScore {
    fn is_zero(&self) -> bool {
        &AddressScore::Finite(0) == self
    }
}

impl PartialOrd for AddressScore {
    fn partial_cmp(&self, other: &AddressScore) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AddressScore {
    fn cmp(&self, other: &AddressScore) -> Ordering {
        // Semantics of cardinal numbers with a single infinite cardinal.
        match (self, other) {
            (AddressScore::Infinite, AddressScore::Infinite) =>
                Ordering::Equal,
            (AddressScore::Infinite, AddressScore::Finite(_)) =>
                Ordering::Greater,
            (AddressScore::Finite(_), AddressScore::Infinite) =>
                Ordering::Less,
            (AddressScore::Finite(a), AddressScore::Finite(b)) =>
                a.cmp(b),
        }
    }
}

impl Add for AddressScore {
    type Output = AddressScore;

    fn add(self, rhs: AddressScore) -> Self::Output {
        // Semantics of cardinal numbers with a single infinite cardinal.
        match (self, rhs) {
            (AddressScore::Infinite, AddressScore::Infinite) =>
                AddressScore::Infinite,
            (AddressScore::Infinite, AddressScore::Finite(_)) =>
                AddressScore::Infinite,
            (AddressScore::Finite(_), AddressScore::Infinite) =>
                AddressScore::Infinite,
            (AddressScore::Finite(a), AddressScore::Finite(b)) =>
                AddressScore::Finite(a.saturating_add(b))
        }
    }
}

impl Sub<u32> for AddressScore {
    type Output = AddressScore;

    fn sub(self, rhs: u32) -> Self::Output {
        // Semantics of cardinal numbers with a single infinite cardinal.
        match self {
            AddressScore::Infinite => AddressScore::Infinite,
            AddressScore::Finite(score) => AddressScore::Finite(score.saturating_sub(rhs))
        }
    }
}

impl Default for Addresses {
    fn default() -> Self {
        Addresses::new(NonZeroUsize::new(200).expect("200 > 0"))
    }
}

/// The result of adding an address to an ordered list of
/// addresses with associated scores.
pub enum AddAddressResult {
    Inserted { expired: SmallVec<[AddressRecord; 8]> },
    Updated { expired: SmallVec<[AddressRecord; 8]> },
}

impl Addresses {
    /// Create a new ranked address collection with the given size limit
    /// for [finitely scored](AddressScore::Finite) addresses.
    pub fn new(limit: NonZeroUsize) -> Self {
        Addresses {
            registry: SmallVec::new(),
            limit,
            reports: VecDeque::with_capacity(limit.get()),
        }
    }

    /// Add a [`Multiaddr`] to the collection.
    ///
    /// If the given address already exists in the collection,
    /// the given score is added to the current score of the address.
    ///
    /// If the collection has already observed the configured
    /// number of address additions, the least-recently added address
    /// as per this limited history has its score reduced by the amount
    /// used in this prior report, with removal from the collection
    /// occurring when the score drops to 0.
    pub fn add(&mut self, addr: Multiaddr, score: AddressScore) -> AddAddressResult {
        // If enough reports (i.e. address additions) occurred, reduce
        // the score of the least-recently added address.
        if self.reports.len() == self.limit.get() {
            let old_report = self.reports.pop_front().expect("len = limit > 0");
            // If the address is still in the collection, decrease its score.
            if let Some(record) = self.registry.iter_mut().find(|r| r.addr == old_report.addr) {
                record.score = record.score - old_report.score;
                isort(&mut self.registry);
            }
        }

        // Remove addresses that have a score of 0.
        let mut expired = SmallVec::new();
        while self.registry.last().map(|e| e.score.is_zero()).unwrap_or(false) {
            if let Some(addr) = self.registry.pop() {
                expired.push(addr);
            }
        }

        // If the address score is finite, remember this report.
        if let AddressScore::Finite(score) = score {
            self.reports.push_back(Report { addr: addr.clone(), score });
        }

        // If the address is already in the collection, increase its score.
        for r in &mut self.registry {
            if r.addr == addr {
                r.score = r.score + score;
                isort(&mut self.registry);
                return AddAddressResult::Updated { expired }
            }
        }

        // It is a new record.
        self.registry.push(AddressRecord::new(addr, score));
        AddAddressResult::Inserted { expired }
    }

    /// Explicitly remove an address from the collection.
    ///
    /// Returns `true` if the address existed in the collection
    /// and was thus removed, false otherwise.
    pub fn remove(&mut self, addr: &Multiaddr) -> bool {
        if let Some(pos) = self.registry.iter().position(|r| &r.addr == addr) {
            self.registry.remove(pos);
            true
        } else {
            false
        }
    }

    /// Return an iterator over all [`Multiaddr`] values.
    ///
    /// The iteration is ordered by descending score.
    pub fn iter(&self) -> AddressIter<'_> {
        AddressIter { items: &self.registry, offset: 0 }
    }

    /// Return an iterator over all [`Multiaddr`] values.
    ///
    /// The iteration is ordered by descending score.
    pub fn into_iter(self) -> AddressIntoIter {
        AddressIntoIter { items: self.registry }
    }
}

/// An iterator over [`Multiaddr`] values.
#[derive(Clone)]
pub struct AddressIter<'a> {
    items: &'a [AddressRecord],
    offset: usize
}

impl<'a> Iterator for AddressIter<'a> {
    type Item = &'a AddressRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.items.len() {
            return None
        }
        let item = &self.items[self.offset];
        self.offset += 1;
        Some(&item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.items.len() - self.offset;
        (n, Some(n))
    }
}

impl<'a> ExactSizeIterator for AddressIter<'a> {}

/// An iterator over [`Multiaddr`] values.
#[derive(Clone)]
pub struct AddressIntoIter {
    items: SmallVec<[AddressRecord; 8]>,
}

impl Iterator for AddressIntoIter {
    type Item = AddressRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.items.is_empty() {
            Some(self.items.remove(0))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let n = self.items.len();
        (n, Some(n))
    }
}

impl ExactSizeIterator for AddressIntoIter {}

// Reverse insertion sort.
fn isort(xs: &mut [AddressRecord]) {
    for i in 1 .. xs.len() {
        for j in (1 ..= i).rev() {
            if xs[j].score <= xs[j - 1].score {
                break
            }
            xs.swap(j, j - 1)
        }
    }
}

#[cfg(test)]
mod tests {
    use libp2p_core::multiaddr::{Multiaddr, Protocol};
    use quickcheck::*;
    use rand::Rng;
    use std::num::{NonZeroUsize, NonZeroU8};
    use super::*;

    impl Arbitrary for AddressScore {
        fn arbitrary<G: Gen>(g: &mut G) -> AddressScore {
            if g.gen_range(0, 10) == 0 { // ~10% "Infinitely" scored addresses
                AddressScore::Infinite
            } else {
                AddressScore::Finite(g.gen())
            }
        }
    }

    impl Arbitrary for AddressRecord {
        fn arbitrary<G: Gen>(g: &mut G) -> Self {
            let addr = Protocol::Tcp(g.gen::<u16>() % 256).into();
            let score = AddressScore::arbitrary(g);
            AddressRecord::new(addr, score)
        }
    }

    #[test]
    fn isort_sorts() {
        fn property(xs: Vec<AddressScore>) {
            let mut xs = xs.into_iter()
                .map(|score| AddressRecord::new(Multiaddr::empty(), score))
                .collect::<Vec<_>>();

            isort(&mut xs);

            for i in 1 .. xs.len() {
                assert!(xs[i - 1].score >= xs[i].score)
            }
        }

        quickcheck(property as fn(_));
    }

    #[test]
    fn score_retention() {
        fn prop(first: AddressRecord, other: AddressRecord) -> TestResult {
            if first.addr == other.addr {
                return TestResult::discard()
            }

            let mut addresses = Addresses::default();

            // Add the first address.
            addresses.add(first.addr.clone(), first.score);
            assert!(addresses.iter().any(|a| &a.addr == &first.addr));

            // Add another address so often that the initial report of
            // the first address may be purged and, since it was the
            // only report, the address removed.
            for _ in 0 .. addresses.limit.get() + 1 {
                addresses.add(other.addr.clone(), other.score);
            }

            let exists = addresses.iter().any(|a| &a.addr == &first.addr);

            match (first.score, other.score) {
                // Only finite scores push out other finite scores.
                (AddressScore::Finite(_), AddressScore::Finite(_)) => assert!(!exists),
                _ => assert!(exists),
            }

            TestResult::passed()
        }

        quickcheck(prop as fn(_,_) -> _);
    }

    #[test]
    fn finitely_scored_address_limit() {
        fn prop(reports: Vec<AddressRecord>, limit: NonZeroU8) {
            let mut addresses = Addresses::new(limit.into());

            // Add all reports.
            for r in reports {
                addresses.add(r.addr, r.score);
            }

            // Count the finitely scored addresses.
            let num_finite = addresses.iter().filter(|r| match r {
                AddressRecord { score: AddressScore::Finite(_), .. } => true,
                _ => false,
            }).count();

            // Check against the limit.
            assert!(num_finite <= limit.get() as usize);
        }

        quickcheck(prop as fn(_,_));
    }

    #[test]
    fn record_score_sum() {
        fn prop(records: Vec<AddressRecord>) -> bool {
            // Make sure the address collection can hold all reports.
            let n = std::cmp::max(records.len(), 1);
            let mut addresses = Addresses::new(NonZeroUsize::new(n).unwrap());

            // Add all address reports to the collection.
            for r in records.iter() {
                addresses.add(r.addr.clone(), r.score.clone());
            }

            // Check that each address in the registry has the expected score.
            for r in &addresses.registry {
                let expected_score = records.iter().fold(
                    None::<AddressScore>, |sum, rec|
                        if &rec.addr == &r.addr {
                            sum.map_or(Some(rec.score), |s| Some(s + rec.score))
                        } else {
                            sum
                        });

                if Some(r.score) != expected_score {
                    return false
                }
            }

            true
        }

        quickcheck(prop as fn(_) -> _)
    }
}

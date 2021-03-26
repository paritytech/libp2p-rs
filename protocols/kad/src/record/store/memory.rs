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

use crate::kbucket;
use libp2p_core::PeerId;
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::{hash_map, hash_set, HashMap, HashSet};
use std::iter;

/// In-memory implementation of a `RecordStore`.
pub struct MemoryStore {
    /// The identity of the peer owning the store.
    local_key: kbucket::Key<PeerId>,
    /// The configuration of the store.
    config: MemoryStoreConfig,
    /// The stored (regular) records.
    records: HashMap<Key, Record>,
    /// The stored provider records.
    providers: HashMap<Key, SmallVec<[ProviderRecord; K_VALUE.get()]>>,
    /// The set of all provider records for the node identified by `local_key`.
    ///
    /// Must be kept in sync with `providers`.
    provided: HashSet<ProviderRecord>,
}

/// Configuration for a `MemoryStore`.
pub struct MemoryStoreConfig {
    /// The maximum number of records.
    pub max_records: usize,
    /// The maximum size of record values, in bytes.
    pub max_value_bytes: usize,
    /// The maximum number of providers stored for a key.
    ///
    /// This should match up with the chosen replication factor.
    pub max_providers_per_key: usize,
    /// The maximum number of provider records for which the
    /// local node is the provider.
    pub max_provided_keys: usize,
}

impl Default for MemoryStoreConfig {
    fn default() -> Self {
        Self {
            max_records: 1024,
            max_value_bytes: 65 * 1024,
            max_provided_keys: 1024,
            max_providers_per_key: K_VALUE.get(),
        }
    }
}

impl MemoryStore {
    /// Creates a new `MemoryRecordStore` with a default configuration.
    pub fn new(local_id: PeerId) -> Self {
        Self::with_config(local_id, Default::default())
    }

    /// Creates a new `MemoryRecordStore` with the given configuration.
    pub fn with_config(local_id: PeerId, config: MemoryStoreConfig) -> Self {
        MemoryStore {
            local_key: kbucket::Key::from(local_id),
            config,
            records: HashMap::default(),
            provided: HashSet::default(),
            providers: HashMap::default(),
        }
    }

    /// Retains the records satisfying a predicate.
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&Key, &mut Record) -> bool
    {
        self.records.retain(f);
    }
}

impl<'a> RecordStore<'a> for MemoryStore {
    type RecordsIter = iter::Map<
        hash_map::Values<'a, Key, Record>,
        fn(&'a Record) -> Cow<'a, Record>
    >;

    type ProvidedIter = iter::Map<
        hash_set::Iter<'a, ProviderRecord>,
        fn(&'a ProviderRecord) -> Cow<'a, ProviderRecord>
    >;

    fn get(&'a self, k: &Key) -> Option<Cow<'_, Record>> {
        self.records.get(k).map(Cow::Borrowed)
    }

    fn put(&'a mut self, r: Record) -> Result<()> {
        if r.value.len() >= self.config.max_value_bytes {
            return Err(Error::ValueTooLarge)
        }

        let num_records = self.records.len();

        match self.records.entry(r.key.clone()) {
            hash_map::Entry::Occupied(mut e) => {
                e.insert(r);
            }
            hash_map::Entry::Vacant(e) => {
                if num_records >= self.config.max_records {
                    return Err(Error::MaxRecords)
                }
                e.insert(r);
            }
        }

        Ok(())
    }

    fn remove(&'a mut self, k: &Key) {
        self.records.remove(k);
    }

    fn records(&'a self) -> Self::RecordsIter {
        self.records.values().map(Cow::Borrowed)
    }

    fn add_provider(&'a mut self, record: ProviderRecord) -> Result<()> {
        let num_keys = self.providers.len();

        // Obtain the entry
        let providers = match self.providers.entry(record.key.clone()) {
            e@hash_map::Entry::Occupied(_) => e,
            e@hash_map::Entry::Vacant(_) => {
                if self.config.max_provided_keys == num_keys {
                    return Err(Error::MaxProvidedKeys)
                }
                e
            }
        }.or_insert_with(Default::default);

        if let Some(i) = providers.iter().position(|p| p.provider == record.provider) {
            // In-place update of an existing provider record.
            providers.as_mut()[i] = record;
        } else {
            // It is a new provider record for that key.
            let local_key = self.local_key.clone();
            let key = kbucket::Key::new(record.key.clone());
            let provider = kbucket::Key::from(record.provider);
            if let Some(i) = providers.iter().position(|p| {
                let pk = kbucket::Key::from(p.provider);
                provider.distance(&key) < pk.distance(&key)
            }) {
                // Insert the new provider.
                if local_key.preimage() == &record.provider {
                    self.provided.insert(record.clone());
                }
                providers.insert(i, record);
                // Remove the excess provider, if any.
                if providers.len() > self.config.max_providers_per_key {
                    if let Some(p) = providers.pop() {
                        self.provided.remove(&p);
                    }
                }
            }
            else if providers.len() < self.config.max_providers_per_key {
                // The distance of the new provider to the key is larger than
                // the distance of any existing provider, but there is still room.
                if local_key.preimage() == &record.provider {
                    self.provided.insert(record.clone());
                }
                providers.push(record);
            }
        }
        Ok(())
    }

    fn providers(&'a self, key: &Key) -> Vec<ProviderRecord> {
        self.providers.get(key).map_or_else(Vec::new, |ps| ps.clone().into_vec())
    }

    fn provided(&'a self) -> Self::ProvidedIter {
        self.provided.iter().map(Cow::Borrowed)
    }

    fn remove_provider(&'a mut self, key: &Key, provider: &PeerId) {
        if let hash_map::Entry::Occupied(mut e) = self.providers.entry(key.clone()) {
            let providers = e.get_mut();
            if let Some(i) = providers.iter().position(|p| &p.provider == provider) {
                let p = providers.remove(i);
                self.provided.remove(&p);
            }
            if providers.is_empty() {
                e.remove();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p_core::multihash::{Code, Multihash};
    use quickcheck::*;
    use rand::Rng;

    fn random_multihash() -> Multihash {
        Multihash::wrap(Code::Sha2_256.into(), &rand::thread_rng().gen::<[u8; 32]>()).unwrap()
    }

    fn distance(r: &ProviderRecord) -> kbucket::Distance {
        kbucket::Key::new(r.key.clone())
            .distance(&kbucket::Key::from(r.provider))
    }

    #[test]
    fn put_get_remove_record() {
        fn prop(r: Record) {
            let mut store = MemoryStore::new(PeerId::random());
            assert!(store.put(r.clone()).is_ok());
            assert_eq!(Some(Cow::Borrowed(&r)), store.get(&r.key));
            store.remove(&r.key);
            assert!(store.get(&r.key).is_none());
        }
        quickcheck(prop as fn(_))
    }

    #[test]
    fn add_get_remove_provider() {
        fn prop(r: ProviderRecord) {
            let mut store = MemoryStore::new(PeerId::random());
            assert!(store.add_provider(r.clone()).is_ok());
            assert!(store.providers(&r.key).contains(&r));
            store.remove_provider(&r.key, &r.provider);
            assert!(!store.providers(&r.key).contains(&r));
        }
        quickcheck(prop as fn(_))
    }

    #[test]
    fn providers_ordered_by_distance_to_key() {
        fn prop(providers: Vec<kbucket::Key<PeerId>>) -> bool {
            let mut store = MemoryStore::new(PeerId::random());
            let key = Key::from(random_multihash());

            let mut records = providers.into_iter().map(|p| {
                ProviderRecord::new(key.clone(), p.into_preimage(), Vec::new())
            }).collect::<Vec<_>>();

            for r in &records {
                assert!(store.add_provider(r.clone()).is_ok());
            }

            records.sort_by(|r1, r2| distance(r1).cmp(&distance(r2)));
            records.truncate(store.config.max_providers_per_key);

            records == store.providers(&key).to_vec()
        }

        quickcheck(prop as fn(_) -> _)
    }

    #[test]
    fn provided() {
        let id = PeerId::random();
        let mut store = MemoryStore::new(id.clone());
        let key = random_multihash();
        let rec = ProviderRecord::new(key, id.clone(), Vec::new());
        assert!(store.add_provider(rec.clone()).is_ok());
        assert_eq!(vec![Cow::Borrowed(&rec)], store.provided().collect::<Vec<_>>());
        store.remove_provider(&rec.key, &id);
        assert_eq!(store.provided().count(), 0);
    }

    #[test]
    fn update_provider() {
        let mut store = MemoryStore::new(PeerId::random());
        let key = random_multihash();
        let prv = PeerId::random();
        let mut rec = ProviderRecord::new(key, prv, Vec::new());
        assert!(store.add_provider(rec.clone()).is_ok());
        assert_eq!(vec![rec.clone()], store.providers(&rec.key).to_vec());
        rec.expires = Some(Instant::now());
        assert!(store.add_provider(rec.clone()).is_ok());
        assert_eq!(vec![rec.clone()], store.providers(&rec.key).to_vec());
    }

    #[test]
    fn max_provided_keys() {
        let mut store = MemoryStore::new(PeerId::random());
        for _ in 0 .. store.config.max_provided_keys {
            let key = random_multihash();
            let prv = PeerId::random();
            let rec = ProviderRecord::new(key, prv, Vec::new());
            let _ = store.add_provider(rec);
        }
        let key = random_multihash();
        let prv = PeerId::random();
        let rec = ProviderRecord::new(key, prv, Vec::new());
        match store.add_provider(rec) {
            Err(Error::MaxProvidedKeys) => {}
            _ => panic!("Unexpected result"),
        }
    }
}

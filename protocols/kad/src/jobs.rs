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

//! Periodic (background) jobs.
//!
//! ## Record Persistence & Expiry
//!
//! To ensure persistence of records in the DHT, a Kademlia node
//! must periodically (re-)publish and (re-)replicate its records:
//!
//!   1. (Re-)publishing: The original publisher or provider of a record
//!      must regularly re-publish in order to prolong the expiration.
//!
//!   2. (Re-)replication: Every node storing a replica of a record must
//!      regularly re-replicate it to the closest nodes to the key in
//!      order to ensure the record is present at these nodes.
//!
//! Re-publishing primarily ensures persistence of the record beyond its
//! initial TTL, for as long as the publisher stores (or provides) the record,
//! whilst (re-)replication primarily ensures persistence for the duration
//! of the TTL in the light of topology changes. Consequently, replication
//! intervals should be shorter than publication intervals and
//! publication intervals should be shorter than the TTL.
//!
//! This module implements two periodic jobs:
//!
//!   * [`PutRecordJob`]: For (re-)publication and (re-)replication of
//!     regular (value-)records.
//!
//!   * [`AddProviderJob`]: For (re-)publication of provider records.
//!     Provider records currently have no separate replication mechanism.
//!
//! A periodic job is driven like a `Future` or `Stream` by `poll`ing it.
//! Once a job starts running it emits records to send to the `k` closest
//! nodes to the key, where `k` is the replication factor.
//!
//! Furthermore, these jobs perform double-duty by removing expired records
//! from the `RecordStore` on every run. Expired records are never emitted
//! by the jobs.
//!
//! > **Note**: The current implementation takes a snapshot of the records
//! > to replicate from the `RecordStore` when it starts and thus, to account
//! > for the worst case, it temporarily requires additional memory proportional
//! > to the size of all stored records. As a job runs, the records are moved
//! > out of the job to the consumer, where they can be dropped after being sent.

use crate::record::{self, Record, ProviderRecord, store::RecordStore};
use libp2p_core::PeerId;
use futures::prelude::*;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use std::vec;
use wasm_timer::{Instant, Delay};

/// The maximum number of queries towards which background jobs
/// are allowed to start new queries on an invocation of
/// `Kademlia::poll`.
pub const JOBS_MAX_QUERIES: usize = 100;

/// The maximum number of new queries started by a background job
/// per invocation of `Kademlia::poll`.
pub const JOBS_MAX_NEW_QUERIES: usize = 10;

/// A background job run periodically.
#[derive(Debug)]
struct PeriodicJob<T> {
    interval: Duration,
    state: PeriodicJobState<T>,
}

impl<T> PeriodicJob<T> {
    fn is_running(&self) -> bool {
        match self.state {
            PeriodicJobState::Running(..) => true,
            PeriodicJobState::Waiting(..) => false,
        }
    }

    /// Cuts short the remaining delay, if the job is currently waiting
    /// for the delay to expire.
    fn asap(&mut self) {
        if let PeriodicJobState::Waiting(delay, deadline) = &mut self.state {
            let new_deadline = Instant::now() - Duration::from_secs(1);
            *deadline = new_deadline;
            delay.reset_at(new_deadline);
        }
    }

    /// Returns `true` if the job is currently not running but ready
    /// to be run, `false` otherwise.
    fn is_ready(&mut self, cx: &mut Context<'_>, now: Instant) -> bool {
        if let PeriodicJobState::Waiting(delay, deadline) = &mut self.state {
            if now >= *deadline || !Future::poll(Pin::new(delay), cx).is_pending() {
                return true
            }
        }
        false
    }
}

/// The state of a background job run periodically.
#[derive(Debug)]
enum PeriodicJobState<T> {
    Running(T),
    Waiting(Delay, Instant)
}

//////////////////////////////////////////////////////////////////////////////
// PutRecordJob

/// Periodic job for replicating / publishing records.
pub struct PutRecordJob {
    local_id: PeerId,
    next_publish: Option<Instant>,
    publish_interval: Option<Duration>,
    record_ttl: Option<Duration>,
    skipped: HashSet<record::Key>,
    inner: PeriodicJob<vec::IntoIter<Record>>,
}

impl PutRecordJob {
    /// Creates a new periodic job for replicating and re-publishing
    /// locally stored records.
    pub fn new(
        local_id: PeerId,
        replicate_interval: Duration,
        publish_interval: Option<Duration>,
        record_ttl: Option<Duration>,
    ) -> Self {
        let now = Instant::now();
        let deadline = now + replicate_interval;
        let delay = Delay::new_at(deadline);
        let next_publish = publish_interval.map(|i| now + i);
        Self {
            local_id,
            next_publish,
            publish_interval,
            record_ttl,
            skipped: HashSet::new(),
            inner: PeriodicJob {
                interval: replicate_interval,
                state: PeriodicJobState::Waiting(delay, deadline)
            }
        }
    }

    /// Adds the key of a record that is ignored on the current or
    /// next run of the job.
    pub fn skip(&mut self, key: record::Key) {
        self.skipped.insert(key);
    }

    /// Checks whether the job is currently running.
    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    /// Cuts short the remaining delay, if the job is currently waiting
    /// for the delay to expire.
    ///
    /// The job is guaranteed to run on the next invocation of `poll`.
    pub fn asap(&mut self, publish: bool) {
        if publish {
            self.next_publish = Some(Instant::now() - Duration::from_secs(1))
        }
        self.inner.asap()
    }

    /// Polls the job for records to replicate.
    ///
    /// Must be called in the context of a task. When `NotReady` is returned,
    /// the current task is registered to be notified when the job is ready
    /// to be run.
    pub fn poll<T>(&mut self, cx: &mut Context<'_>, store: &mut T, now: Instant) -> Poll<Record>
    where
        for<'a> T: RecordStore<'a>
    {
        if self.inner.is_ready(cx, now) {
            let publish = self.next_publish.map_or(false, |t_pub| now >= t_pub);
            let records = store.records()
                .filter_map(|r| {
                    let is_publisher = r.publisher.as_ref() == Some(&self.local_id);
                    if self.skipped.contains(&r.key) || (!publish && is_publisher) {
                        None
                    } else {
                        let mut record = r.into_owned();
                        if publish && is_publisher {
                            record.expires = record.expires.or_else(||
                                self.record_ttl.map(|ttl| now + ttl));
                        }
                        Some(record)
                    }
                })
                .collect::<Vec<_>>()
                .into_iter();

            // Schedule the next publishing run.
            if publish {
                self.next_publish = self.publish_interval.map(|i| now + i);
            }

            self.skipped.clear();

            self.inner.state = PeriodicJobState::Running(records);
        }

        if let PeriodicJobState::Running(records) = &mut self.inner.state {
            for r in records {
                if r.is_expired(now) {
                    store.remove(&r.key)
                } else {
                    return Poll::Ready(r)
                }
            }

            // Wait for the next run.
            let deadline = now + self.inner.interval;
            let delay = Delay::new_at(deadline);
            self.inner.state = PeriodicJobState::Waiting(delay, deadline);
            assert!(!self.inner.is_ready(cx, now));
        }

        Poll::Pending
    }
}

//////////////////////////////////////////////////////////////////////////////
// AddProviderJob

/// Periodic job for replicating provider records.
pub struct AddProviderJob {
    inner: PeriodicJob<vec::IntoIter<ProviderRecord>>
}

impl AddProviderJob {
    /// Creates a new periodic job for provider announcements.
    pub fn new(interval: Duration) -> Self {
        let now = Instant::now();
        Self {
            inner: PeriodicJob {
                interval,
                state: {
                    let deadline = now + interval;
                    PeriodicJobState::Waiting(Delay::new_at(deadline), deadline)
                }
            }
        }
    }

    /// Checks whether the job is currently running.
    pub fn is_running(&self) -> bool {
        self.inner.is_running()
    }

    /// Cuts short the remaining delay, if the job is currently waiting
    /// for the delay to expire.
    ///
    /// The job is guaranteed to run on the next invocation of `poll`.
    pub fn asap(&mut self) {
        self.inner.asap()
    }

    /// Polls the job for provider records to replicate.
    ///
    /// Must be called in the context of a task. When `NotReady` is returned,
    /// the current task is registered to be notified when the job is ready
    /// to be run.
    pub fn poll<T>(&mut self, cx: &mut Context<'_>, store: &mut T, now: Instant) -> Poll<ProviderRecord>
    where
        for<'a> T: RecordStore<'a>
    {
        if self.inner.is_ready(cx, now) {
            let records = store.provided()
                .map(|r| r.into_owned())
                .collect::<Vec<_>>()
                .into_iter();
            self.inner.state = PeriodicJobState::Running(records);
        }

        if let PeriodicJobState::Running(keys) = &mut self.inner.state {
            for r in keys {
                if r.is_expired(now) {
                    store.remove_provider(&r.key, &r.provider)
                } else {
                    return Poll::Ready(r)
                }
            }

            let deadline = now + self.inner.interval;
            let delay = Delay::new_at(deadline);
            self.inner.state = PeriodicJobState::Waiting(delay, deadline);
            assert!(!self.inner.is_ready(cx, now));
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use crate::record::store::MemoryStore;
    use futures::{executor::block_on, future::poll_fn};
    use quickcheck::*;
    use rand::Rng;
    use super::*;

    fn rand_put_record_job() -> PutRecordJob {
        let mut rng = rand::thread_rng();
        let id = PeerId::random();
        let replicate_interval = Duration::from_secs(rng.gen_range(1, 60));
        let publish_interval = Some(replicate_interval * rng.gen_range(1, 10));
        let record_ttl = Some(Duration::from_secs(rng.gen_range(1, 600)));
        PutRecordJob::new(id.clone(), replicate_interval, publish_interval, record_ttl)
    }

    fn rand_add_provider_job() -> AddProviderJob {
        let mut rng = rand::thread_rng();
        let interval = Duration::from_secs(rng.gen_range(1, 60));
        AddProviderJob::new(interval)
    }

    #[test]
    fn new_job_not_running() {
        let job = rand_put_record_job();
        assert!(!job.is_running());
        let job = rand_add_provider_job();
        assert!(!job.is_running());
    }

    #[test]
    fn run_put_record_job() {
        fn prop(records: Vec<Record>) {
            let mut job = rand_put_record_job();
            // Fill a record store.
            let mut store = MemoryStore::new(job.local_id.clone());
            for r in records {
                let _ = store.put(r);
            }

            block_on(poll_fn(|ctx| {
                let now = Instant::now() + job.inner.interval;
                // All (non-expired) records in the store must be yielded by the job.
                for r in store.records().map(|r| r.into_owned()).collect::<Vec<_>>() {
                    if !r.is_expired(now) {
                        assert_eq!(job.poll(ctx, &mut store, now), Poll::Ready(r));
                        assert!(job.is_running());
                    }
                }
                assert_eq!(job.poll(ctx, &mut store, now), Poll::Pending);
                assert!(!job.is_running());
                Poll::Ready(())
            }));
        }

        quickcheck(prop as fn(_))
    }

    #[test]
    fn run_add_provider_job() {
        fn prop(records: Vec<ProviderRecord>) {
            let mut job = rand_add_provider_job();
            let id = PeerId::random();
            // Fill a record store.
            let mut store = MemoryStore::new(id.clone());
            for mut r in records {
                r.provider = id.clone();
                let _ = store.add_provider(r);
            }

            block_on(poll_fn(|ctx| {
                let now = Instant::now() + job.inner.interval;
                // All (non-expired) records in the store must be yielded by the job.
                for r in store.provided().map(|r| r.into_owned()).collect::<Vec<_>>() {
                    if !r.is_expired(now) {
                        assert_eq!(job.poll(ctx, &mut store, now), Poll::Ready(r));
                        assert!(job.is_running());
                    }
                }
                assert_eq!(job.poll(ctx, &mut store, now), Poll::Pending);
                assert!(!job.is_running());
                Poll::Ready(())
            }));
        }

        quickcheck(prop as fn(_))
    }
}

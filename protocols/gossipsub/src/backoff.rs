// Copyright 2020 Sigma Prime Pty Ltd.
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

//! Data structure for efficiently storing known back-off's when pruning peers.
use crate::topic::TopicHash;
use libp2p_core::PeerId;
use std::collections::{
    hash_map::{Entry, HashMap},
    HashSet,
};
use std::time::Duration;
use wasm_timer::Instant;

#[derive(Copy, Clone)]
struct HeartbeatIndex(usize);

/// Stores backoffs in an efficient manner.
pub struct BackoffStorage {
    /// Stores backoffs and the index in backoffs_by_heartbeat per peer per topic.
    backoffs: HashMap<TopicHash, HashMap<PeerId, (Instant, HeartbeatIndex)>>,
    /// Stores peer topic pairs per heartbeat (this is cyclic the current index is
    /// heartbeat_index).
    backoffs_by_heartbeat: Vec<HashSet<(TopicHash, PeerId)>>,
    /// The index in the backoffs_by_heartbeat vector corresponding to the current heartbeat.
    heartbeat_index: HeartbeatIndex,
    /// The heartbeat interval duration from the config.
    heartbeat_interval: Duration,
    /// Backoff slack from the config.
    backoff_slack: u32,
}

impl BackoffStorage {
    fn heartbeats(d: &Duration, heartbeat_interval: &Duration) -> usize {
        ((d.as_nanos() + heartbeat_interval.as_nanos() - 1) / heartbeat_interval.as_nanos())
            as usize
    }

    pub fn new(
        prune_backoff: &Duration,
        heartbeat_interval: Duration,
        backoff_slack: u32,
    ) -> BackoffStorage {
        // We add one additional slot for partial heartbeat
        let max_heartbeats =
            Self::heartbeats(prune_backoff, &heartbeat_interval) + backoff_slack as usize + 1;
        BackoffStorage {
            backoffs: HashMap::new(),
            backoffs_by_heartbeat: vec![HashSet::new(); max_heartbeats],
            heartbeat_index: HeartbeatIndex(0),
            heartbeat_interval,
            backoff_slack,
        }
    }

    /// Updates the backoff for a peer (if there is already a more restrictive backoff then this call
    /// doesn't change anything).
    pub fn update_backoff(&mut self, topic: &TopicHash, peer: &PeerId, time: Duration) {
        let instant = Instant::now() + time;
        let insert_into_backoffs_by_heartbeat =
            |heartbeat_index: HeartbeatIndex,
             backoffs_by_heartbeat: &mut Vec<HashSet<_>>,
             heartbeat_interval,
             backoff_slack| {
                let pair = (topic.clone(), *peer);
                let index = (heartbeat_index.0
                    + Self::heartbeats(&time, heartbeat_interval)
                    + backoff_slack as usize)
                    % backoffs_by_heartbeat.len();
                backoffs_by_heartbeat[index].insert(pair);
                HeartbeatIndex(index)
            };
        match self
            .backoffs
            .entry(topic.clone())
            .or_insert_with(HashMap::new)
            .entry(*peer)
        {
            Entry::Occupied(mut o) => {
                let (backoff, index) = o.get();
                if backoff < &instant {
                    let pair = (topic.clone(), *peer);
                    if let Some(s) = self.backoffs_by_heartbeat.get_mut(index.0) {
                        s.remove(&pair);
                    }
                    let index = insert_into_backoffs_by_heartbeat(
                        self.heartbeat_index,
                        &mut self.backoffs_by_heartbeat,
                        &self.heartbeat_interval,
                        self.backoff_slack,
                    );
                    o.insert((instant, index));
                }
            }
            Entry::Vacant(v) => {
                let index = insert_into_backoffs_by_heartbeat(
                    self.heartbeat_index,
                    &mut self.backoffs_by_heartbeat,
                    &self.heartbeat_interval,
                    self.backoff_slack,
                );
                v.insert((instant, index));
            }
        };
    }

    /// Checks if a given peer is backoffed for the given topic. This method respects the
    /// configured BACKOFF_SLACK and may return true even if the backup is already over.
    /// It is guaranteed to return false if the backoff is not over and eventually if enough time
    /// passed true if the backoff is over.
    ///
    /// This method should be used for deciding if we can already send a GRAFT to a previously
    /// backoffed peer.
    pub fn is_backoff_with_slack(&self, topic: &TopicHash, peer: &PeerId) -> bool {
        self.backoffs
            .get(topic)
            .map_or(false, |m| m.contains_key(peer))
    }

    pub fn get_backoff_time(&self, topic: &TopicHash, peer: &PeerId) -> Option<Instant> {
        Self::get_backoff_time_from_backoffs(&self.backoffs, topic, peer)
    }

    fn get_backoff_time_from_backoffs(
        backoffs: &HashMap<TopicHash, HashMap<PeerId, (Instant, HeartbeatIndex)>>,
        topic: &TopicHash,
        peer: &PeerId,
    ) -> Option<Instant> {
        backoffs
            .get(topic)
            .and_then(|m| m.get(peer).map(|(i, _)| *i))
    }

    /// Applies a heartbeat. That should be called regularly in intervals of length
    /// `heartbeat_interval`.
    pub fn heartbeat(&mut self) {
        // Clean up backoffs_by_heartbeat
        if let Some(s) = self.backoffs_by_heartbeat.get_mut(self.heartbeat_index.0) {
            let backoffs = &mut self.backoffs;
            let slack = self.heartbeat_interval * self.backoff_slack;
            let now = Instant::now();
            s.retain(|(topic, peer)| {
                let keep = match Self::get_backoff_time_from_backoffs(backoffs, topic, peer) {
                    Some(backoff_time) => backoff_time + slack > now,
                    None => false,
                };
                if !keep {
                    //remove from backoffs
                    if let Entry::Occupied(mut m) = backoffs.entry(topic.clone()) {
                        if m.get_mut().remove(peer).is_some() && m.get().is_empty() {
                            m.remove();
                        }
                    }
                }

                keep
            });
        }

        // Increase heartbeat index
        self.heartbeat_index =
            HeartbeatIndex((self.heartbeat_index.0 + 1) % self.backoffs_by_heartbeat.len());
    }
}

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

use crate::error::ValidationError;
use crate::peer_score::RejectReason;
use crate::MessageId;
use libp2p_core::PeerId;
use log::debug;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use wasm_timer::Instant;

/// Tracks recently sent `IWANT` messages and checks if peers respond to them
/// for each `IWANT` message we track one random requested message id.
#[derive(Default)]
pub(crate) struct GossipPromises {
    /// Stores for each tracked message id and peer the instant when this promise expires.
    ///
    /// If the peer didn't respond until then we consider the promise as broken and penalize the
    /// peer.
    promises: HashMap<MessageId, HashMap<PeerId, Instant>>,
}

impl GossipPromises {
    /// Track a promise to deliver a message from a list of [`MessageId`]s we are requesting.
    pub fn add_promise(&mut self, peer: PeerId, messages: &[MessageId], expires: Instant) {
        // Randomly select a message id
        let mut rng = thread_rng();
        if let Some(message_id) = messages.choose(&mut rng) {
            // If a promise for this message id and peer already exists we don't update expires!
            self.promises
                .entry(message_id.clone())
                .or_insert_with(HashMap::new)
                .entry(peer)
                .or_insert(expires);
        }
    }

    pub fn message_delivered(&mut self, message_id: &MessageId) {
        // Someone delivered a message, we can stop tracking all promises for it.
        self.promises.remove(message_id);
    }

    pub fn reject_message(&mut self, message_id: &MessageId, reason: &RejectReason) {
        // A message got rejected, so we can stop tracking promises and let the score penalty apply
        // from invalid message delivery.
        // We do take exception and apply promise penalty regardless in the following cases, where
        // the peer delivered an obviously invalid message.
        match reason {
            RejectReason::ValidationError(ValidationError::InvalidSignature) => (),
            RejectReason::SelfOrigin => (),
            _ => {
                self.promises.remove(message_id);
            }
        };
    }

    /// Returns the number of broken promises for each peer who didn't follow up on an IWANT
    /// request.
    /// This should be called not too often relative to the expire times, since it iterates over
    /// the whole stored data.
    pub fn get_broken_promises(&mut self) -> HashMap<PeerId, usize> {
        let now = Instant::now();
        let mut result = HashMap::new();
        self.promises.retain(|msg, peers| {
            peers.retain(|peer_id, expires| {
                if *expires < now {
                    let count = result.entry(*peer_id).or_insert(0);
                    *count += 1;
                    debug!(
                        "The peer {} broke the promise to deliver message {} in time!",
                        peer_id, msg
                    );
                    false
                } else {
                    true
                }
            });
            !peers.is_empty()
        });
        result
    }
}

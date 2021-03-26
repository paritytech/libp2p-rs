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

use crate::topic::TopicHash;
use crate::types::{MessageId, RawGossipsubMessage};
use libp2p_core::PeerId;
use log::debug;
use std::fmt::Debug;
use std::{collections::HashMap, fmt};

/// CacheEntry stored in the history.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CacheEntry {
    mid: MessageId,
    topic: TopicHash,
}

/// MessageCache struct holding history of messages.
#[derive(Clone)]
pub struct MessageCache {
    msgs: HashMap<MessageId, RawGossipsubMessage>,
    /// For every message and peer the number of times this peer asked for the message
    iwant_counts: HashMap<MessageId, HashMap<PeerId, u32>>,
    history: Vec<Vec<CacheEntry>>,
    /// The number of indices in the cache history used for gossipping. That means that a message
    /// won't get gossipped anymore when shift got called `gossip` many times after inserting the
    /// message in the cache.
    gossip: usize,
}

impl fmt::Debug for MessageCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageCache")
            .field("msgs", &self.msgs)
            .field("history", &self.history)
            .field("gossip", &self.gossip)
            .finish()
    }
}

/// Implementation of the MessageCache.
impl MessageCache {
    pub fn new(gossip: usize, history_capacity: usize) -> Self {
        MessageCache {
            gossip,
            msgs: HashMap::default(),
            iwant_counts: HashMap::default(),
            history: vec![Vec::new(); history_capacity],
        }
    }

    /// Put a message into the memory cache.
    ///
    /// Returns the message if it already exists.
    pub fn put(
        &mut self,
        message_id: &MessageId,
        msg: RawGossipsubMessage,
    ) -> Option<RawGossipsubMessage> {
        debug!("Put message {:?} in mcache", message_id);
        let cache_entry = CacheEntry {
            mid: message_id.clone(),
            topic: msg.topic.clone(),
        };

        let seen_message = self.msgs.insert(message_id.clone(), msg);
        if seen_message.is_none() {
            // Don't add duplicate entries to the cache.
            self.history[0].push(cache_entry);
        }
        seen_message
    }

    /// Get a message with `message_id`
    #[cfg(test)]
    pub fn get(&self, message_id: &MessageId) -> Option<&RawGossipsubMessage> {
        self.msgs.get(message_id)
    }

    /// Increases the iwant count for the given message by one and returns the message together
    /// with the iwant if the message exists.
    pub fn get_with_iwant_counts(
        &mut self,
        message_id: &MessageId,
        peer: &PeerId,
    ) -> Option<(&RawGossipsubMessage, u32)> {
        let iwant_counts = &mut self.iwant_counts;
        self.msgs.get(message_id).and_then(|message| {
            if !message.validated {
                None
            } else {
                Some((message, {
                    let count = iwant_counts
                        .entry(message_id.clone())
                        .or_default()
                        .entry(*peer)
                        .or_default();
                    *count += 1;
                    *count
                }))
            }
        })
    }

    /// Gets a message with [`MessageId`] and tags it as validated.
    pub fn validate(&mut self, message_id: &MessageId) -> Option<&RawGossipsubMessage> {
        self.msgs.get_mut(message_id).map(|message| {
            message.validated = true;
            &*message
        })
    }

    /// Get a list of [`MessageId`]s for a given topic.
    pub fn get_gossip_message_ids(&self, topic: &TopicHash) -> Vec<MessageId> {
        self.history[..self.gossip]
            .iter()
            .fold(vec![], |mut current_entries, entries| {
                // search for entries with desired topic
                let mut found_entries: Vec<MessageId> = entries
                    .iter()
                    .filter_map(|entry| {
                        if &entry.topic == topic {
                            let mid = &entry.mid;
                            // Only gossip validated messages
                            if let Some(true) = self.msgs.get(mid).map(|msg| msg.validated) {
                                Some(mid.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect();

                // generate the list
                current_entries.append(&mut found_entries);
                current_entries
            })
    }

    /// Shift the history array down one and delete messages associated with the
    /// last entry.
    pub fn shift(&mut self) {
        for entry in self.history.pop().expect("history is always > 1") {
            if let Some(msg) = self.msgs.remove(&entry.mid) {
                if !msg.validated {
                    // If GossipsubConfig::validate_messages is true, the implementing
                    // application has to ensure that Gossipsub::validate_message gets called for
                    // each received message within the cache timeout time."
                    debug!(
                        "The message with id {} got removed from the cache without being validated.",
                        &entry.mid
                    );
                }
            }
            debug!("Remove message from the cache: {}", &entry.mid);

            self.iwant_counts.remove(&entry.mid);
        }

        // Insert an empty vec in position 0
        self.history.insert(0, Vec::new());
    }

    /// Removes a message from the cache and returns it if existent
    pub fn remove(&mut self, message_id: &MessageId) -> Option<RawGossipsubMessage> {
        //We only remove the message from msgs and iwant_count and keep the message_id in the
        // history vector. Zhe id in the history vector will simply be ignored on popping.

        self.iwant_counts.remove(message_id);
        self.msgs.remove(message_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RawGossipsubMessage;
    use crate::{IdentTopic as Topic, TopicHash};
    use libp2p_core::PeerId;

    fn gen_testm(x: u64, topic: TopicHash) -> (MessageId, RawGossipsubMessage) {
        let default_id = |message: &RawGossipsubMessage| {
            // default message id is: source + sequence number
            let mut source_string = message.source.as_ref().unwrap().to_base58();
            source_string.push_str(&message.sequence_number.unwrap().to_string());
            MessageId::from(source_string)
        };
        let u8x: u8 = x as u8;
        let source = Some(PeerId::random());
        let data: Vec<u8> = vec![u8x];
        let sequence_number = Some(x);

        let m = RawGossipsubMessage {
            source,
            data,
            sequence_number,
            topic,
            signature: None,
            key: None,
            validated: false,
        };

        let id = default_id(&m);
        (id, m)
    }

    fn new_cache(gossip_size: usize, history: usize) -> MessageCache {
        MessageCache::new(gossip_size, history)
    }

    #[test]
    /// Test that the message cache can be created.
    fn test_new_cache() {
        let x: usize = 3;
        let mc = new_cache(x, 5);

        assert_eq!(mc.gossip, x);
    }

    #[test]
    /// Test you can put one message and get one.
    fn test_put_get_one() {
        let mut mc = new_cache(10, 15);

        let topic1_hash = Topic::new("topic1").hash().clone();
        let (id, m) = gen_testm(10, topic1_hash);

        mc.put(&id, m.clone());

        assert!(mc.history[0].len() == 1);

        let fetched = mc.get(&id);

        assert_eq!(fetched.is_none(), false);
        assert_eq!(fetched.is_some(), true);

        // Make sure it is the same fetched message
        match fetched {
            Some(x) => assert_eq!(*x, m),
            _ => assert!(false),
        }
    }

    #[test]
    /// Test attempting to 'get' with a wrong id.
    fn test_get_wrong() {
        let mut mc = new_cache(10, 15);

        let topic1_hash = Topic::new("topic1").hash().clone();
        let (id, m) = gen_testm(10, topic1_hash);

        mc.put(&id, m.clone());

        // Try to get an incorrect ID
        let wrong_id = MessageId::new(b"wrongid");
        let fetched = mc.get(&wrong_id);
        assert_eq!(fetched.is_none(), true);
    }

    #[test]
    /// Test attempting to 'get' empty message cache.
    fn test_get_empty() {
        let mc = new_cache(10, 15);

        // Try to get an incorrect ID
        let wrong_string = MessageId::new(b"imempty");
        let fetched = mc.get(&wrong_string);
        assert_eq!(fetched.is_none(), true);
    }

    #[test]
    /// Test shift mechanism.
    fn test_shift() {
        let mut mc = new_cache(1, 5);

        let topic1_hash = Topic::new("topic1").hash().clone();

        // Build the message
        for i in 0..10 {
            let (id, m) = gen_testm(i, topic1_hash.clone());
            mc.put(&id, m.clone());
        }

        mc.shift();

        // Ensure the shift occurred
        assert!(mc.history[0].len() == 0);
        assert!(mc.history[1].len() == 10);

        // Make sure no messages deleted
        assert!(mc.msgs.len() == 10);
    }

    #[test]
    /// Test Shift with no additions.
    fn test_empty_shift() {
        let mut mc = new_cache(1, 5);

        let topic1_hash = Topic::new("topic1").hash().clone();

        // Build the message
        for i in 0..10 {
            let (id, m) = gen_testm(i, topic1_hash.clone());
            mc.put(&id, m.clone());
        }

        mc.shift();

        // Ensure the shift occurred
        assert!(mc.history[0].len() == 0);
        assert!(mc.history[1].len() == 10);

        mc.shift();

        assert!(mc.history[2].len() == 10);
        assert!(mc.history[1].len() == 0);
        assert!(mc.history[0].len() == 0);
    }

    #[test]
    /// Test shift to see if the last history messages are removed.
    fn test_remove_last_from_shift() {
        let mut mc = new_cache(4, 5);

        let topic1_hash = Topic::new("topic1").hash().clone();

        // Build the message
        for i in 0..10 {
            let (id, m) = gen_testm(i, topic1_hash.clone());
            mc.put(&id, m.clone());
        }

        // Shift right until deleting messages
        mc.shift();
        mc.shift();
        mc.shift();
        mc.shift();

        assert_eq!(mc.history[mc.history.len() - 1].len(), 10);

        // Shift and delete the messages
        mc.shift();
        assert_eq!(mc.history[mc.history.len() - 1].len(), 0);
        assert_eq!(mc.history[0].len(), 0);
        assert_eq!(mc.msgs.len(), 0);
    }
}

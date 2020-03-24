/*
 * Copyright 2019 Fluence Labs Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::kbucket::{AppliedPending, InsertResult, Node, NodeStatus, PendingNode, SubBucket};
use std::time::Instant;

pub struct Swamp<TKey, TVal> {
    bucket: SubBucket<Node<TKey, TVal>>,
    pending: Option<PendingNode<TKey, TVal>>,
}

impl<TKey, TVal> Swamp<TKey, TVal> {
    pub fn new() -> Self {
        Self {
            bucket: SubBucket::new(),
            pending: None,
        }
    }

    pub fn exists_active_pending(&self) -> bool {
        self.pending.is_some() // TODO: check replace timeout
    }

    pub fn set_pending(&mut self, node: PendingNode<TKey, TVal>) {
        self.pending = Some(node)
    }

    pub fn remove_pending(&mut self) {
        self.pending = None
    }

    pub fn pending_ready(&self) -> bool {
        self.pending
            .as_ref()
            .map_or(false, |pending| pending.replace <= Instant::now())
    }

    pub fn insert(&mut self, node: Node<TKey, TVal>, status: NodeStatus) -> InsertResult<TKey> {
        match status {
            NodeStatus::Connected => {
                if self.bucket.is_full() {
                    if self.bucket.all_nodes_connected() || self.exists_active_pending() {
                        // TODO: check pending.replace in exists_active_pending & call apply_pending?
                        return InsertResult::Full;
                    } else {
                        self.set_pending(PendingNode {
                            node,
                            status: NodeStatus::Connected,
                            replace: Instant::now() + self.pending_timeout,
                        });
                        return InsertResult::Pending {
                            // Schedule a dial-up to check if the node is reachable
                            // NOTE: nodes[0] is disconnected (see all_nodes_connected check above)
                            //  and the least recently connected
                            disconnected: self.nodes[0].key.clone(),
                        };
                    }
                }
                self.bucket.append_connected_node(node);
                InsertResult::Inserted
            }
            NodeStatus::Disconnected => {
                if self.bucket.is_full() {
                    return InsertResult::Full;
                }
                self.bucket.insert_disconnected_node(node);
                InsertResult::Inserted
            }
        }
    }

    pub fn apply_pending(&mut self) -> Option<AppliedPending<TKey, TVal>> {
        if !self.pending_ready() {
            return None;
        }

        self.swamp_pending
            .take()
            .map(|PendingNode { node, status, .. }| {
                let evicted = if self.bucket.is_full() {
                    self.bucket.pop_node()
                } else {
                    None
                };

                if let InsertResult::Inserted = self.insert(node.clone(), status) {
                    AppliedPending {
                        inserted: node,
                        evicted,
                    }
                } else {
                    unreachable!("Bucket is not full, we just evicted a node.")
                }
            })
    }

    pub fn update(&mut self, key: &TKey, new_status: NodeStatus) {
        // Remove the node from its current position and then reinsert it
        // with the desired status, which puts it at the end of either the
        // prefix list of disconnected nodes or the suffix list of connected
        // nodes (i.e. most-recently disconnected or most-recently connected,
        // respectively).
        if let Some(pos) = self.bucket.position(key) {
            // Remove the node from its current position.
            let node = self
                .bucket
                .evict_node(pos)
                .expect("position MUST have been correct");
            // If the least-recently connected node re-establishes its
            // connected status, drop the pending node.
            if self.bucket.is_least_recently_connected(pos) && new_status == NodeStatus::Connected {
                self.remove_pending();
            }
            // Reinsert the node with the desired status.
            match self.insert(node, new_status) {
                InsertResult::Inserted => {}
                _ => unreachable!("The node is removed before being (re)inserted."),
            }
        }
    }
}

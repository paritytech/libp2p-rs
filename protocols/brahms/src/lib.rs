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

//! Implementation of the Brahms (Byzantine Resilient Random Membership Sampling) protocol.
//!
//! Reference paper: https://gnunet.org/sites/default/files/Brahms-Comnet-Mar09.pdf
//!
//! Brahms handles two things:
//!
//! - It is a peer discovery protocol that will fill the swarm's topology with the nodes that it
//!   discovers. See the `BrahmsTopology` trait.
//! - It provides a view (named *V* in the paper) that is a partial representation of the network
//!   topology. This view changes by itself over time. By randomness, each node's view should
//!   contain a direct or indirect link to every single other node on the network.
//!
//! Brahms can be used for pubsub protocols by letting it run and sending messages to nodes on the
//! view. Keep in mind that the view changes over time. Don't forget that this only works if nodes
//! relay all the messages they receive to every other nodes they are connected to.
//!
//! The `Brahms` network behaviour provided by this crate will connect and try stay connected to
//! all the nodes in its view. Therefore, if you build for example a pubsub protocol that
//! dispatches messages to the nodes of the view, then you won't have to pay the price of
//! connecting to the nodes to send the messages to.
//!
//! # How it works
//!
//! Brahms works by round delimited in time. During each round:
//!
//! - We send *push* messages to a random subset of the nodes of our view, containing our identity.
//! - We query a random subset of nodes of our view for their view.
//!
//! At the end of each round, we update the view by randomly combining the results.
//!
//! A few other mechanisms, namely a sampling system and a proof-of-work system, exist in order to
//! guarantee the strength of the system.
//!
//! # View size
//!
//! The Brahms algorithm does not define how large the size of the view as to be, but indicates
//! that it should be around `∛n`, where `n` is the number of nodes connected to the network.
//!
//! Creating a `Brahms` struct requires passing and then updating a `BrahmsConfig` that contains
//! the size of the view.
//!
//! In situation where you don't know in advance the size of the network (which is usually the
//! case), or if this size is very flexible, then you should use a mechanism that yields an
//! estimate of the size and regularly adjust the size of Brahm's view in order to reflect this
//! size. The network size estimation mechanism is out of scope of this crate.

pub use self::behaviour::{Brahms, BrahmsConfig, BrahmsEvent, BrahmsViewSize};

pub mod handler;
pub mod protocol;

mod behaviour;
mod codec;
mod view;

// These modules shouldn't be public, but we want to access their content from the `benches`
// directory. Ideally the benchmarks of `pow` and `sampler` would be inside `pow` and `sampler`
// themselves, but that isn't possible on the stable Rust channel as of January 2019.
#[doc(hidden)]
pub mod pow;
#[doc(hidden)]
pub mod sampler;

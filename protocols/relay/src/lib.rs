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

//! Implementation of the [libp2p circuit relay
//! specification](https://github.com/libp2p/specs/tree/master/relay).
//!
//! ## Example
//!
//! ```rust
//! # use libp2p_core::transport::memory::MemoryTransport;
//! # use libp2p_relay::{RelayConfig, new_transport_and_behaviour};
//! # use libp2p_swarm::Swarm;
//! # use libp2p_core::{identity, Multiaddr, multiaddr::Protocol, PeerId, upgrade, Transport};
//! # use libp2p_yamux::YamuxConfig;
//! # use libp2p_plaintext::PlainText2Config;
//! # use std::convert::TryInto;
//! # use std::str::FromStr;
//! #
//! # let local_key = identity::Keypair::generate_ed25519();
//! # let local_public_key = local_key.public();
//! # let local_peer_id = local_public_key.clone().into_peer_id();
//! # let plaintext = PlainText2Config {
//! #     local_public_key: local_public_key.clone(),
//! # };
//! #
//! let (relay_transport, relay_behaviour) = new_transport_and_behaviour(
//!     RelayConfig::default(),
//!     MemoryTransport::default(),
//! );
//!
//! let transport = relay_transport
//!     .upgrade(upgrade::Version::V1)
//!     .authenticate(plaintext)
//!     .multiplex(YamuxConfig::default())
//!     .boxed();
//!
//! let mut swarm = Swarm::new(transport, relay_behaviour, local_peer_id);
//!
//! let relay_addr = Multiaddr::from_str("/memory/1234").unwrap()
//!     .with(Protocol::P2p(PeerId::random().into()))
//!     .with(Protocol::P2pCircuit);
//! let dst_addr = relay_addr.clone().with(Protocol::Memory(5678));
//!
//! // Listen for incoming connections via relay node (1234).
//! swarm.listen_on(relay_addr).unwrap();
//!
//! // Dial node (5678) via relay node (1234).
//! swarm.dial_addr(dst_addr).unwrap();
//! ```
//!
//! ## Terminology
//!
//! ### Entities
//!
//! - **Source**: The node initiating a connection via a *relay* to a *destination*.
//!
//! - **Relay**: The node being asked by a *source* to relay to a *destination*.
//!
//! - **Destination**: The node contacted by the *source* via the *relay*.
//!
//! ### Messages
//!
//! - **Outgoing relay request**: The request sent by a *source* to a *relay*.
//!
//! - **Incoming relay request**: The request received by a *relay* from a *source*.
//!
//! - **Outgoing destination request**: The request sent by a *relay* to a *destination*.
//!
//! - **Incoming destination request**: The request received by a *destination* from a *relay*.

mod behaviour;

mod message_proto {
    include!(concat!(env!("OUT_DIR"), "/message.pb.rs"));
}

mod handler;
mod protocol;
mod transport;

pub use behaviour::{Relay, RelayConfig};
pub use transport::{RelayError, RelayTransport};

use libp2p_core::Transport;

/// Create both a [`RelayTransport`] wrapping the provided [`Transport`]
/// as well as a [`Relay`] [`NetworkBehaviour`](libp2p_swarm::NetworkBehaviour).
///
/// Interconnects the returned [`RelayTransport`] and [`Relay`].
pub fn new_transport_and_behaviour<T: Transport + Clone>(
    config: RelayConfig,
    transport: T,
) -> (RelayTransport<T>, Relay) {
    let (transport, from_transport) = RelayTransport::new(transport);
    let behaviour = Relay::new(config, from_transport);
    (transport, behaviour)
}

/// The ID of an outgoing / incoming, relay / destination request.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl RequestId {
    fn new() -> RequestId {
        RequestId(rand::random())
    }
}

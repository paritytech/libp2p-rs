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
//! - **Outgoing relay request**: The request send by a *source* to a *relay*.
//!
//! - **Incoming relay request**: The request received by a *relay* from a *source*.
//!
//! - **Outgoing destination request**: The request send by a *relay* to a *destination*.
//!
//! - **Incoming destination request**: The request received by a *destination* from a *relay*.
//!
//! - **Outgoing listen request**: The request send by a *destination* to a *relay* asking the
//!   *relay* to listen for incoming connections on the behalf of the *destination*.
//!
//! - **Incoming listen request**: The request received by a *relay* from a *destination* asking the
//!   *relay* to listen for incoming connections on the behalf of the *destination*.

mod behaviour;

mod message_proto {
    include!(concat!(env!("OUT_DIR"), "/message.pb.rs"));
}

mod handler;
mod protocol;
mod transport;

pub use behaviour::{Relay, RelayConfig};
use libp2p_core::Transport;
pub use transport::{RelayError, RelayTransportWrapper};

/// Create both a [`RelayTransportWrapper`] wrapping the provided [`Transport`]
/// as well as a [`Relay`] [`NetworkBehaviour`](libp2p_swarm::NetworkBehaviour).
pub fn new_transport_and_behaviour<T: Transport + Clone>(
    config: RelayConfig,
    transport: T,
) -> (RelayTransportWrapper<T>, Relay) {
    let (transport, (to_transport, from_transport)) =
        RelayTransportWrapper::new(transport);
    let behaviour = Relay::new(config, to_transport, from_transport);
    (transport, behaviour)
}

// TODO: No need to be public.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct RequestId(u64);

impl RequestId {
    fn new() -> RequestId {
        RequestId(rand::random())
    }
}

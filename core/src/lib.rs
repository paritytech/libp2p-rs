// Copyright 2017-2018 Parity Technologies (UK) Ltd.
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

//! Transports, upgrades, multiplexing and node handling of *libp2p*.
//!
//! The main concepts of libp2p-core are:
//!
//! - A [`PeerId`] is a unique global identifier for a node on the network.
//!   Each node must have a different [`PeerId`]. Normally, a [`PeerId`] is the
//!   hash of the public key used to negotiate encryption on the
//!   communication channel, thereby guaranteeing that they cannot be spoofed.
//! - The [`Transport`] trait defines how to reach a remote node or listen for
//!   incoming remote connections. See the [`transport`] module.
//! - The [`StreamMuxer`] trait is implemented on structs that hold a connection
//!   to a remote and can subdivide this connection into multiple substreams.
//!   See the [`muxing`] module.
//! - The [`UpgradeInfo`], [`InboundUpgrade`] and [`OutboundUpgrade`] traits
//!   define how to upgrade each individual substream to use a protocol.
//!   See the `upgrade` module.

mod keys_proto {
    include!(concat!(env!("OUT_DIR"), "/keys_proto.rs"));
}

mod envelope_proto {
    include!(concat!(env!("OUT_DIR"), "/envelope_proto.rs"));
}

mod peer_record_proto {
    include!(concat!(env!("OUT_DIR"), "/peer_record_proto.rs"));
}

/// Multi-address re-export.
pub use multiaddr;
pub type Negotiated<T> = multistream_select::Negotiated<T>;

mod peer_id;
mod translation;

pub mod connection;
pub mod either;
pub mod identity;
pub mod muxing;
pub mod network;
pub mod transport;
pub mod upgrade;
pub mod signed_envelope;
pub mod peer_record;

pub use multiaddr::Multiaddr;
pub use multihash;
pub use muxing::StreamMuxer;
pub use peer_id::PeerId;
pub use identity::PublicKey;
pub use transport::Transport;
pub use translation::address_translation;
pub use upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeInfo, UpgradeError, ProtocolName};
pub use connection::{Connected, Endpoint, ConnectedPoint};
pub use network::Network;
pub use signed_envelope::SignedEnvelope;
pub use peer_record::PeerRecord;

use std::{future::Future, pin::Pin};

/// Implemented on objects that can run a `Future` in the background.
///
/// > **Note**: While it may be tempting to implement this trait on types such as
/// >           [`futures::stream::FuturesUnordered`], please note that passing an `Executor` is
/// >           optional, and that `FuturesUnordered` (or a similar struct) will automatically
/// >           be used as fallback by libp2p. The `Executor` trait should therefore only be
/// >           about running `Future`s in the background.
pub trait Executor {
    /// Run the given future in the background until it ends.
    fn exec(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>);
}

impl<F: Fn(Pin<Box<dyn Future<Output = ()> + Send>>)> Executor for F {
    fn exec(&self, f: Pin<Box<dyn Future<Output = ()> + Send>>) {
        self(f)
    }
}

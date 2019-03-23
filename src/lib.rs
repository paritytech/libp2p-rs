// Copyright 2018 Parity Technologies (UK) Ltd.
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

//! Libp2p is a peer-to-peer framework.
//!
//! # Major libp2p concepts
//!
//! Here is a list of all the major concepts of libp2p.
//!
//! ## Multiaddr
//!
//! A [`Multiaddr`] is a self-describing network address and protocol stack
//! that is used to establish connections to peers. Some examples:
//!
//! * `/ip4/80.123.90.4/tcp/5432`
//! * `/ip6/[::1]/udp/10560/quic`
//! * `/unix//path/to/socket`
//!
//! ## Transport
//!
//! [`Transport`] is a trait for types that provide connection-oriented communication channels
//! based on dialing to or listening on a [`Multiaddr`]. To that end a transport
//! produces as output a type of data stream that varies depending on the concrete type of
//! transport.
//!
//! An implementation of transport typically supports only certain multi-addresses.
//! For example, the [`TcpConfig`] only supports multi-addresses of the format
//! `/ip4/.../tcp/...`.
//!
//! Example (Dialing a TCP/IP multi-address):
//!
//! ```rust
//! use libp2p::{Multiaddr, Transport, tcp::TcpConfig};
//! let tcp = TcpConfig::new();
//! let addr: Multiaddr = "/ip4/98.97.96.95/tcp/20500".parse().expect("invalid multiaddr");
//! let _conn = tcp.dial(addr);
//! ```
//! In the above example, `_conn` is a [`Future`] that needs to be polled in order for
//! the dialing to take place and eventually resolve to a connection. Polling
//! futures is typically done through a [tokio] runtime.
//!
//! The easiest way to create a transport is to use [`build_development_transport`].
//! This function provides support for the most common protocols but it is also
//! subject to change over time and should thus not be used in production
//! configurations.
//!
//! Example (Creating a development transport):
//!
//! ```rust
//! let keypair = libp2p::identity::Keypair::generate_ed25519();
//! let _transport = libp2p::build_development_transport(keypair);
//! // _transport.dial(...);
//! ```
//!
//! The keypair that is passed as an argument in the above example is used
//! to set up transport-layer encryption using a newly generated long-term
//! identity keypair. The public key of this keypair uniquely identifies
//! the node in the network in the form of a [`PeerId`].
//!
//! See the documentation of the [`Transport`] trait for more details.
//!
//! ### Connection Upgrades
//!
//! Once a connection has been established with a remote through a [`Transport`], it can be
//! *upgraded*. Upgrading a transport is the process of negotiating an additional protocol
//! with the remote, mediated through a negotiation protocol called [`multistream-select`].
//!
//! Example ([`secio`] Protocol Upgrade):
//!
//! ```rust
//! # #[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), feature = "libp2p-secio"))] {
//! use libp2p::{Transport, tcp::TcpConfig, secio::SecioConfig, identity::Keypair};
//! let tcp = TcpConfig::new();
//! let secio_upgrade = SecioConfig::new(Keypair::generate_ed25519());
//! let tcp_secio = tcp.with_upgrade(secio_upgrade);
//! // let _ = tcp_secio.dial(...);
//! # }
//! ```
//! In this example, `tcp_secio` is a new [`Transport`] that negotiates the secio protocol
//! on all connections.
//!
//! ## Network Behaviour
//!
//! The [`NetworkBehaviour`] trait is implemented on types that provide some capability to the
//! network. Examples of network behaviours include:
//!
//!   * Periodically pinging other nodes on established connections.
//!   * Periodically asking for information from other nodes.
//!   * Querying information from a DHT and propagating it to other nodes.
//!
//! ## Swarm
//!
//! A [`Swarm`] manages a pool of connections established through a [`Transport`]
//! and drives a [`NetworkBehaviour`] through emitting events triggered by activity
//! on the managed connections. Creating a [`Swarm`] thus involves combining a
//! [`Transport`] with a [`NetworkBehaviour`].
//!
//! See the documentation of the [`core`] module for more details about swarms.
//!
//! # Using libp2p
//!
//! The easiest way to get started with libp2p involves the following steps:
//!
//!   1. Creating an identity [`Keypair`] for the local node, obtaining the local
//!      [`PeerId`] from the [`PublicKey`].
//!   2. Creating an instance of a base [`Transport`], e.g. [`TcpConfig`], upgrading it with
//!      all the desired protocols, such as for transport security and multiplexing.
//!      In order to be usable with a [`Swarm`] later, the [`Output`](Transport::Output)
//!      of the final transport must be a tuple of a [`PeerId`] and a value whose type
//!      implements [`StreamMuxer`] (e.g. [`Yamux`]). The peer ID must be the
//!      identity of the remote peer of the established connection, which is
//!      usually obtained through a transport encryption protocol such as
//!      [`secio`] that authenticates the peer. See the implementation of
//!      [`build_development_transport`] for an example.
//!   3. Creating a struct that implements the [`NetworkBehaviour`] trait and combines all the
//!      desired network behaviours, implementing the event handlers as per the
//!      desired application's networking logic.
//!   4. Instantiating a [`Swarm`] with the transport, the network behaviour and the
//!      local peer ID from the previous steps.
//!
//! The swarm instance can then be polled with the [tokio] library, in order to
//! continuously drive the network activity of the program.
//!
//! [`Keypair`]: identity::Keypair
//! [`PublicKey`]: identity::PublicKey
//! [`Future`]: futures::Future
//! [`TcpConfig`]: tcp::TcpConfig
//! [`NetworkBehaviour`]: core::swarm::NetworkBehaviour
//! [`StreamMuxer`]: core::muxing::StreamMuxer
//! [`Yamux`]: yamux::Yamux
//!
//! [tokio]: https://tokio.rs
//! [`multistream-select`]: https://github.com/multiformats/multistream-select

#![doc(html_logo_url = "https://libp2p.io/img/logo_small.png")]
#![doc(html_favicon_url = "https://libp2p.io/img/favicon.png")]

pub use bytes;
pub use futures;
#[doc(inline)]
pub use multiaddr;
#[doc(inline)]
pub use multihash;
pub use tokio_io;
pub use tokio_codec;

#[doc(inline)]
pub use libp2p_bluetooth as bluetooth;
#[doc(inline)]
pub use libp2p_core as core;
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_dns as dns;
#[doc(inline)]
pub use libp2p_identify as identify;
#[doc(inline)]
pub use libp2p_kad as kad;
#[doc(inline)]
pub use libp2p_floodsub as floodsub;
#[doc(inline)]
pub use libp2p_mplex as mplex;
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_mdns as mdns;
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_noise as noise;
#[doc(inline)]
pub use libp2p_ping as ping;
#[doc(inline)]
pub use libp2p_plaintext as plaintext;
#[doc(inline)]
pub use libp2p_ratelimit as ratelimit;
#[doc(inline)]
pub use libp2p_secio as secio;
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_tcp as tcp;
#[doc(inline)]
pub use libp2p_uds as uds;
#[cfg(feature = "libp2p-websocket")]
#[doc(inline)]
pub use libp2p_websocket as websocket;
#[doc(inline)]
pub use libp2p_yamux as yamux;

mod transport_ext;

pub mod bandwidth;
pub mod simple;

pub use self::core::{
    identity,
    Transport, PeerId, Swarm,
    transport::TransportError,
    upgrade::{InboundUpgrade, InboundUpgradeExt, OutboundUpgrade, OutboundUpgradeExt}
};
pub use libp2p_core_derive::NetworkBehaviour;
pub use self::multiaddr::{Multiaddr, multiaddr as build_multiaddr};
pub use self::simple::SimpleProtocol;
pub use self::transport_ext::TransportExt;

use futures::prelude::*;
use std::{error, time::Duration};

/// Builds a `Transport` that supports the most commonly-used protocols that libp2p supports.
///
/// > **Note**: This `Transport` is not suitable for production usage, as its implementation
/// >           reserves the right to support additional protocols or remove deprecated protocols.
#[inline]
pub fn build_development_transport(keypair: identity::Keypair)
    -> impl Transport<Output = (PeerId, impl core::muxing::StreamMuxer<OutboundSubstream = impl Send, Substream = impl Send> + Send + Sync), Error = impl error::Error + Send, Listener = impl Send, Dial = impl Send, ListenerUpgrade = impl Send> + Clone
{
     build_tcp_ws_secio_mplex_yamux(keypair)
}

/// Builds an implementation of `Transport` that is suitable for usage with the `Swarm`.
///
/// The implementation supports TCP/IP, WebSockets over TCP/IP, secio as the encryption layer,
/// and mplex or yamux as the multiplexing layer.
///
/// > **Note**: If you ever need to express the type of this `Transport`.
pub fn build_tcp_ws_secio_mplex_yamux(keypair: identity::Keypair)
    -> impl Transport<Output = (PeerId, impl core::muxing::StreamMuxer<OutboundSubstream = impl Send, Substream = impl Send> + Send + Sync), Error = impl error::Error + Send, Listener = impl Send, Dial = impl Send, ListenerUpgrade = impl Send> + Clone
{
    CommonTransport::new()
        .with_upgrade(secio::SecioConfig::new(keypair))
        .and_then(move |out, endpoint| {
            let peer_id = PeerId::from(out.remote_key);
            let peer_id2 = peer_id.clone();
            let upgrade = core::upgrade::SelectUpgrade::new(yamux::Config::default(), mplex::MplexConfig::new())
                // TODO: use a single `.map` instead of two maps
                .map_inbound(move |muxer| (peer_id, muxer))
                .map_outbound(move |muxer| (peer_id2, muxer));

            core::upgrade::apply(out.stream, upgrade, endpoint)
                .map(|(id, muxer)| (id, core::muxing::StreamMuxerBox::new(muxer)))
        })
        .with_timeout(Duration::from_secs(20))
}

/// Implementation of `Transport` that supports the most common protocols.
///
/// The list currently is TCP/IP, DNS, and WebSockets. However this list could change in the
/// future to get new transports.
#[derive(Debug, Clone)]
struct CommonTransport {
    // The actual implementation of everything.
    inner: CommonTransportInner
}

#[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), feature = "libp2p-websocket"))]
type InnerImplementation = core::transport::OrTransport<dns::DnsConfig<tcp::TcpConfig>, websocket::WsConfig<dns::DnsConfig<tcp::TcpConfig>>>;
#[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), not(feature = "libp2p-websocket")))]
type InnerImplementation = dns::DnsConfig<tcp::TcpConfig>;
#[cfg(all(any(target_os = "emscripten", target_os = "unknown"), feature = "libp2p-websocket"))]
type InnerImplementation = websocket::BrowserWsConfig;
#[cfg(all(any(target_os = "emscripten", target_os = "unknown"), not(feature = "libp2p-websocket")))]
type InnerImplementation = core::transport::dummy::DummyTransport;

#[derive(Debug, Clone)]
struct CommonTransportInner {
    inner: InnerImplementation,
}

impl CommonTransport {
    /// Initializes the `CommonTransport`.
    #[inline]
    #[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
    pub fn new() -> CommonTransport {
        let transport = tcp::TcpConfig::new();
        let transport = dns::DnsConfig::new(transport);
        #[cfg(feature = "libp2p-websocket")]
        let transport = {
            let trans_clone = transport.clone();
            transport.or_transport(websocket::WsConfig::new(trans_clone))
        };

        CommonTransport {
            inner: CommonTransportInner { inner: transport }
        }
    }

    /// Initializes the `CommonTransport`.
    #[inline]
    #[cfg(all(any(target_os = "emscripten", target_os = "unknown"), feature = "libp2p-websocket"))]
    pub fn new() -> CommonTransport {
        let inner = websocket::BrowserWsConfig::new();
        CommonTransport {
            inner: CommonTransportInner { inner }
        }
    }

    /// Initializes the `CommonTransport`.
    #[inline]
    #[cfg(all(any(target_os = "emscripten", target_os = "unknown"), not(feature = "libp2p-websocket")))]
    pub fn new() -> CommonTransport {
        let inner = core::transport::dummy::DummyTransport::new();
        CommonTransport {
            inner: CommonTransportInner { inner }
        }
    }
}

impl Transport for CommonTransport {
    type Output = <InnerImplementation as Transport>::Output;
    type Error = <InnerImplementation as Transport>::Error;
    type Listener = <InnerImplementation as Transport>::Listener;
    type ListenerUpgrade = <InnerImplementation as Transport>::ListenerUpgrade;
    type Dial = <InnerImplementation as Transport>::Dial;

    #[inline]
    fn listen_on(self, addr: Multiaddr) -> Result<(Self::Listener, Multiaddr), TransportError<Self::Error>> {
        self.inner.inner.listen_on(addr)
    }

    #[inline]
    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>> {
        self.inner.inner.dial(addr)
    }

    #[inline]
    fn nat_traversal(&self, server: &Multiaddr, observed: &Multiaddr) -> Option<Multiaddr> {
        self.inner.inner.nat_traversal(server, observed)
    }
}

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
//! Example ([`secio`] + [`yamux`] Protocol Upgrade):
//!
//! ```rust
//! # #[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), feature = "tcp-async-std", feature = "secio", feature = "yamux"))] {
//! use libp2p::{Transport, core::upgrade, tcp::TcpConfig, secio::SecioConfig, identity::Keypair, yamux};
//! let tcp = TcpConfig::new();
//! let secio = SecioConfig::new(Keypair::generate_ed25519());
//! let yamux = yamux::Config::default();
//! let transport = tcp.upgrade(upgrade::Version::V1).authenticate(secio).multiplex(yamux);
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
//! [`NetworkBehaviour`]: swarm::NetworkBehaviour
//! [`StreamMuxer`]: core::muxing::StreamMuxer
//! [`Yamux`]: yamux::Yamux
//!
//! [tokio]: https://tokio.rs
//! [`multistream-select`]: https://github.com/multiformats/multistream-select

#![doc(html_logo_url = "https://libp2p.io/img/logo_small.png")]
#![doc(html_favicon_url = "https://libp2p.io/img/favicon.png")]

#[cfg(feature = "pnet")]
use libp2p_pnet::{PnetConfig, PreSharedKey};

pub use bytes;
pub use futures;
#[doc(inline)]
pub use multiaddr;
#[doc(inline)]
pub use multihash;

#[doc(inline)]
pub use libp2p_core as core;
#[cfg(feature = "deflate")]
#[cfg_attr(docsrs, doc(cfg(feature = "deflate")))]
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_deflate as deflate;
#[cfg(feature = "dns")]
#[cfg_attr(docsrs, doc(cfg(feature = "dns")))]
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_dns as dns;
#[cfg(feature = "identify")]
#[cfg_attr(docsrs, doc(cfg(feature = "identify")))]
#[doc(inline)]
pub use libp2p_identify as identify;
#[cfg(feature = "kad")]
#[cfg_attr(docsrs, doc(cfg(feature = "kad")))]
#[doc(inline)]
pub use libp2p_kad as kad;
#[cfg(feature = "floodsub")]
#[cfg_attr(docsrs, doc(cfg(feature = "floodsub")))]
#[doc(inline)]
pub use libp2p_floodsub as floodsub;
#[cfg(feature = "gossipsub")]
#[cfg_attr(docsrs, doc(cfg(feature = "gossipsub")))]
#[doc(inline)]
pub use libp2p_gossipsub as gossipsub;
#[cfg(feature = "mplex")]
#[cfg_attr(docsrs, doc(cfg(feature = "mplex")))]
#[doc(inline)]
pub use libp2p_mplex as mplex;
#[cfg(feature = "mdns")]
#[cfg_attr(docsrs, doc(cfg(feature = "mdns")))]
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_mdns as mdns;
#[cfg(feature = "noise")]
#[cfg_attr(docsrs, doc(cfg(feature = "noise")))]
#[doc(inline)]
pub use libp2p_noise as noise;
#[cfg(feature = "ping")]
#[cfg_attr(docsrs, doc(cfg(feature = "ping")))]
#[doc(inline)]
pub use libp2p_ping as ping;
#[cfg(feature = "plaintext")]
#[cfg_attr(docsrs, doc(cfg(feature = "plaintext")))]
#[doc(inline)]
pub use libp2p_plaintext as plaintext;
#[cfg(all(feature = "quic", not(any(target_os = "emscripten", target_os = "unknown"))))]
#[doc(inline)]
pub use libp2p_quic as quic;
#[cfg(feature = "secio")]
#[cfg_attr(docsrs, doc(cfg(feature = "secio")))]
#[doc(inline)]
pub use libp2p_secio as secio;
#[doc(inline)]
pub use libp2p_swarm as swarm;
#[cfg(any(feature = "tcp-async-std", feature = "tcp-tokio"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "tcp-async-std", feature = "tcp-tokio"))))]
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_tcp as tcp;
#[cfg(feature = "uds")]
#[cfg_attr(docsrs, doc(cfg(feature = "uds")))]
#[doc(inline)]
pub use libp2p_uds as uds;
#[cfg(feature = "wasm-ext")]
#[cfg_attr(docsrs, doc(cfg(feature = "wasm-ext")))]
#[doc(inline)]
pub use libp2p_wasm_ext as wasm_ext;
#[cfg(feature = "websocket")]
#[cfg_attr(docsrs, doc(cfg(feature = "websocket")))]
#[cfg(not(any(target_os = "emscripten", target_os = "unknown")))]
#[doc(inline)]
pub use libp2p_websocket as websocket;
#[cfg(feature = "yamux")]
#[cfg_attr(docsrs, doc(cfg(feature = "yamux")))]
#[doc(inline)]
pub use libp2p_yamux as yamux;
#[cfg(feature = "pnet")]
#[cfg_attr(docsrs, doc(cfg(feature = "pnet")))]
#[doc(inline)]
pub use libp2p_pnet as pnet;

mod transport_ext;

pub mod bandwidth;
pub mod simple;

pub use self::core::{
    identity,
    PeerId,
    Transport,
    transport::TransportError,
    upgrade::{InboundUpgrade, InboundUpgradeExt, OutboundUpgrade, OutboundUpgradeExt}
};
pub use libp2p_core_derive::NetworkBehaviour;
pub use self::multiaddr::{Multiaddr, multiaddr as build_multiaddr};
pub use self::simple::SimpleProtocol;
pub use self::swarm::Swarm;
pub use self::transport_ext::TransportExt;

/// Builds a `Transport` that supports the most commonly-used protocols that libp2p supports.
///
/// > **Note**: This `Transport` is not suitable for production usage, as its implementation
/// >           reserves the right to support additional protocols or remove deprecated protocols.
#[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), any(feature = "tcp-async-std", feature = "tcp-tokio"), feature = "websocket", feature = "secio", feature = "mplex", feature = "yamux"))]
#[cfg_attr(docsrs, doc(cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), any(feature = "tcp-async-std", feature = "tcp-tokio"), feature = "websocket", feature = "secio", feature = "mplex", feature = "yamux"))))]
pub fn build_development_transport(keypair: identity::Keypair)
    -> std::io::Result<impl Transport<Output = (PeerId, impl core::muxing::StreamMuxer<OutboundSubstream = impl Send, Substream = impl Send, Error = impl Into<std::io::Error>> + Send + Sync), Error = impl std::error::Error + Send, Listener = impl Send, Dial = impl Send, ListenerUpgrade = impl Send> + Clone>
{
     build_tcp_ws_secio_mplex_yamux(keypair)
}

/// Builds an implementation of `Transport` that is suitable for usage with the `Swarm`.
///
/// The implementation supports TCP/IP, WebSockets over TCP/IP, secio as the encryption layer,
/// and mplex or yamux as the multiplexing layer.
///
/// > **Note**: If you ever need to express the type of this `Transport`.
#[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), any(feature = "tcp-async-std", feature = "tcp-tokio"), feature = "websocket", feature = "secio", feature = "mplex", feature = "yamux"))]
#[cfg_attr(docsrs, doc(cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), any(feature = "tcp-async-std", feature = "tcp-tokio"), feature = "websocket", feature = "secio", feature = "mplex", feature = "yamux"))))]
pub fn build_tcp_ws_secio_mplex_yamux(keypair: identity::Keypair)
    -> std::io::Result<impl Transport<Output = (PeerId, impl core::muxing::StreamMuxer<OutboundSubstream = impl Send, Substream = impl Send, Error = impl Into<std::io::Error>> + Send + Sync), Error = impl std::error::Error + Send, Listener = impl Send, Dial = impl Send, ListenerUpgrade = impl Send> + Clone>
{
    let transport = {
        #[cfg(feature = "tcp-async-std")]
        let tcp = tcp::TcpConfig::new().nodelay(true);
        #[cfg(feature = "tcp-tokio")]
        let tcp = tcp::TokioTcpConfig::new().nodelay(true);
        let transport = dns::DnsConfig::new(tcp)?;
        let trans_clone = transport.clone();
        transport.or_transport(websocket::WsConfig::new(trans_clone))
    };

    Ok(transport
        .upgrade(core::upgrade::Version::V1)
        .authenticate(secio::SecioConfig::new(keypair))
        .multiplex(core::upgrade::SelectUpgrade::new(yamux::Config::default(), mplex::MplexConfig::new()))
        .map(|(peer, muxer), _| (peer, core::muxing::StreamMuxerBox::new(muxer)))
        .timeout(std::time::Duration::from_secs(20)))
}

/// Builds an implementation of `Transport` that is suitable for usage with the `Swarm`.
///
/// The implementation supports TCP/IP, WebSockets over TCP/IP, secio as the encryption layer,
/// and mplex or yamux as the multiplexing layer.
///
/// > **Note**: If you ever need to express the type of this `Transport`.
#[cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), any(feature = "tcp-async-std", feature = "tcp-tokio"), feature = "websocket", feature = "secio", feature = "mplex", feature = "yamux", feature = "pnet"))]
#[cfg_attr(docsrs, doc(cfg(all(not(any(target_os = "emscripten", target_os = "unknown")), any(feature = "tcp-async-std", feature = "tcp-tokio"), feature = "websocket", feature = "secio", feature = "mplex", feature = "yamux", feature = "pnet"))))]
pub fn build_tcp_ws_pnet_secio_mplex_yamux(keypair: identity::Keypair, psk: PreSharedKey)
    -> std::io::Result<impl Transport<Output = (PeerId, impl core::muxing::StreamMuxer<OutboundSubstream = impl Send, Substream = impl Send, Error = impl Into<std::io::Error>> + Send + Sync), Error = impl std::error::Error + Send, Listener = impl Send, Dial = impl Send, ListenerUpgrade = impl Send> + Clone>
{
    let transport = {
        #[cfg(feature = "tcp-async-std")]
        let tcp = tcp::TcpConfig::new().nodelay(true);
        #[cfg(feature = "tcp-tokio")]
        let tcp = tcp::TokioTcpConfig::new().nodelay(true);
        let transport = dns::DnsConfig::new(tcp)?;
        let trans_clone = transport.clone();
        transport.or_transport(websocket::WsConfig::new(trans_clone))
    };

    Ok(transport
        .and_then(move |socket, _| PnetConfig::new(psk).handshake(socket))
        .upgrade(core::upgrade::Version::V1)
        .authenticate(secio::SecioConfig::new(keypair))
        .multiplex(core::upgrade::SelectUpgrade::new(yamux::Config::default(), mplex::MplexConfig::new()))
        .map(|(peer, muxer), _| (peer, core::muxing::StreamMuxerBox::new(muxer)))
        .timeout(std::time::Duration::from_secs(20)))
}

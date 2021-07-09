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

//! [Noise protocol framework][noise] support for libp2p.
//!
//! > **Note**: This crate is still experimental and subject to major breaking changes
//! >           both on the API and the wire protocol.
//!
//! This crate provides `libp2p_core::InboundUpgrade` and `libp2p_core::OutboundUpgrade`
//! implementations for various noise handshake patterns (currently `IK`, `IX`, and `XX`)
//! over a particular choice of Diffie–Hellman key agreement (currently only X25519).
//!
//! > **Note**: Only the `XX` handshake pattern is currently guaranteed to provide
//! >           interoperability with other libp2p implementations.
//!
//! All upgrades produce as output a pair, consisting of the remote's static public key
//! and a `NoiseOutput` which represents the established cryptographic session with the
//! remote, implementing `futures::io::AsyncRead` and `futures::io::AsyncWrite`.
//!
//! # Usage
//!
//! Example:
//!
//! ```
//! use libp2p_core::{identity, Transport, upgrade};
//! use libp2p_tcp::TcpConfig;
//! use libp2p_noise::{Keypair, X25519Spec, NoiseConfig};
//!
//! # fn main() {
//! let id_keys = identity::Keypair::generate_ed25519();
//! let dh_keys = Keypair::<X25519Spec>::new().into_authentic(&id_keys).unwrap();
//! let noise = NoiseConfig::xx(dh_keys).into_authenticated();
//! let builder = TcpConfig::new().upgrade().authenticate(noise);
//! // let transport = builder.multiplex(...);
//! # }
//! ```
//!
//! [noise]: http://noiseprotocol.org/

mod error;
mod io;
mod protocol;

pub use error::NoiseError;
pub use io::NoiseOutput;
pub use io::handshake;
pub use io::handshake::{Handshake, RemoteIdentity, IdentityExchange};
pub use protocol::{Keypair, AuthenticKeypair, KeypairIdentity, PublicKey, SecretKey};
pub use protocol::{Protocol, ProtocolParams, IX, IK, XX};
pub use protocol::{x25519::X25519, x25519_spec::X25519Spec};

use futures::prelude::*;
use libp2p_core::{identity, PeerId, UpgradeInfo, InboundUpgrade, OutboundUpgrade};
use std::pin::Pin;
use zeroize::Zeroize;

/// The protocol upgrade configuration.
#[derive(Clone)]
pub struct NoiseConfig<P, C: Zeroize, R = ()> {
    dh_keys: AuthenticKeypair<C>,
    params: ProtocolParams,
    legacy: LegacyConfig,
    remote: R,
    _marker: std::marker::PhantomData<P>
}

impl<H, C: Zeroize, R> NoiseConfig<H, C, R> {
    /// Turn the `NoiseConfig` into an authenticated upgrade for use
    /// with a [`Network`](libp2p_core::Network).
    pub fn into_authenticated(self) -> NoiseAuthenticated<H, C, R> {
        NoiseAuthenticated { config: self }
    }

    /// Sets the legacy configuration options to use, if any.
    pub fn set_legacy_config(&mut self, cfg: LegacyConfig) -> &mut Self {
        self.legacy = cfg;
        self
    }
}

impl<C> NoiseConfig<IX, C>
where
    C: Protocol<C> + Zeroize
{
    /// Create a new `NoiseConfig` for the `IX` handshake pattern.
    pub fn ix(dh_keys: AuthenticKeypair<C>) -> Self {
        NoiseConfig {
            dh_keys,
            params: C::params_ix(),
            legacy: LegacyConfig::default(),
            remote: (),
            _marker: std::marker::PhantomData
        }
    }
}

impl<C> NoiseConfig<XX, C>
where
    C: Protocol<C> + Zeroize
{
    /// Create a new `NoiseConfig` for the `XX` handshake pattern.
    pub fn xx(dh_keys: AuthenticKeypair<C>) -> Self {
        NoiseConfig {
            dh_keys,
            params: C::params_xx(),
            legacy: LegacyConfig::default(),
            remote: (),
            _marker: std::marker::PhantomData
        }
    }
}

impl<C> NoiseConfig<IK, C>
where
    C: Protocol<C> + Zeroize
{
    /// Create a new `NoiseConfig` for the `IK` handshake pattern (recipient side).
    ///
    /// Since the identity of the local node is known to the remote, this configuration
    /// does not transmit a static DH public key or public identity key to the remote.
    pub fn ik_listener(dh_keys: AuthenticKeypair<C>) -> Self {
        NoiseConfig {
            dh_keys,
            params: C::params_ik(),
            legacy: LegacyConfig::default(),
            remote: (),
            _marker: std::marker::PhantomData
        }
    }
}

impl<C> NoiseConfig<IK, C, (PublicKey<C>, identity::PublicKey)>
where
    C: Protocol<C> + Zeroize
{
    /// Create a new `NoiseConfig` for the `IK` handshake pattern (initiator side).
    ///
    /// In this configuration, the remote identity is known to the local node,
    /// but the local node still needs to transmit its own public identity.
    pub fn ik_dialer(
        dh_keys: AuthenticKeypair<C>,
        remote_id: identity::PublicKey,
        remote_dh: PublicKey<C>
    ) -> Self {
        NoiseConfig {
            dh_keys,
            params: C::params_ik(),
            legacy: LegacyConfig::default(),
            remote: (remote_dh, remote_id),
            _marker: std::marker::PhantomData
        }
    }
}

// Handshake pattern IX /////////////////////////////////////////////////////

impl<T, C> InboundUpgrade<T> for NoiseConfig<IX, C>
where
    NoiseConfig<IX, C>: UpgradeInfo,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (RemoteIdentity<C>, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Handshake<T, C>;

    fn upgrade_inbound(self, socket: T, _: Self::Info) -> Self::Future {
        let session = self.params.into_builder()
            .local_private_key(self.dh_keys.secret().as_ref())
            .build_responder()
            .map_err(NoiseError::from);
        handshake::rt1_responder(socket, session,
            self.dh_keys.into_identity(),
            IdentityExchange::Mutual,
            self.legacy)
    }
}

impl<T, C> OutboundUpgrade<T> for NoiseConfig<IX, C>
where
    NoiseConfig<IX, C>: UpgradeInfo,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (RemoteIdentity<C>, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Handshake<T, C>;

    fn upgrade_outbound(self, socket: T, _: Self::Info) -> Self::Future {
        let session = self.params.into_builder()
            .local_private_key(self.dh_keys.secret().as_ref())
            .build_initiator()
            .map_err(NoiseError::from);
        handshake::rt1_initiator(socket, session,
                                 self.dh_keys.into_identity(),
                                 IdentityExchange::Mutual,
                                 self.legacy)
    }
}

// Handshake pattern XX /////////////////////////////////////////////////////

impl<T, C> InboundUpgrade<T> for NoiseConfig<XX, C>
where
    NoiseConfig<XX, C>: UpgradeInfo,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (RemoteIdentity<C>, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Handshake<T, C>;

    fn upgrade_inbound(self, socket: T, _: Self::Info) -> Self::Future {
        let session = self.params.into_builder()
            .local_private_key(self.dh_keys.secret().as_ref())
            .build_responder()
            .map_err(NoiseError::from);
        handshake::rt15_responder(socket, session,
            self.dh_keys.into_identity(),
            IdentityExchange::Mutual,
            self.legacy)
    }
}

impl<T, C> OutboundUpgrade<T> for NoiseConfig<XX, C>
where
    NoiseConfig<XX, C>: UpgradeInfo,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (RemoteIdentity<C>, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Handshake<T, C>;

    fn upgrade_outbound(self, socket: T, _: Self::Info) -> Self::Future {
        let session = self.params.into_builder()
            .local_private_key(self.dh_keys.secret().as_ref())
            .build_initiator()
            .map_err(NoiseError::from);
        handshake::rt15_initiator(socket, session,
            self.dh_keys.into_identity(),
            IdentityExchange::Mutual,
            self.legacy)
    }
}

// Handshake pattern IK /////////////////////////////////////////////////////

impl<T, C, R> InboundUpgrade<T> for NoiseConfig<IK, C, R>
where
    NoiseConfig<IK, C, R>: UpgradeInfo,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (RemoteIdentity<C>, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Handshake<T, C>;

    fn upgrade_inbound(self, socket: T, _: Self::Info) -> Self::Future {
        let session = self.params.into_builder()
            .local_private_key(self.dh_keys.secret().as_ref())
            .build_responder()
            .map_err(NoiseError::from);
        handshake::rt1_responder(socket, session,
            self.dh_keys.into_identity(),
            IdentityExchange::Receive,
            self.legacy)
    }
}

impl<T, C> OutboundUpgrade<T> for NoiseConfig<IK, C, (PublicKey<C>, identity::PublicKey)>
where
    NoiseConfig<IK, C, (PublicKey<C>, identity::PublicKey)>: UpgradeInfo,
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (RemoteIdentity<C>, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Handshake<T, C>;

    fn upgrade_outbound(self, socket: T, _: Self::Info) -> Self::Future {
        let session = self.params.into_builder()
            .local_private_key(self.dh_keys.secret().as_ref())
            .remote_public_key(self.remote.0.as_ref())
            .build_initiator()
            .map_err(NoiseError::from);
        handshake::rt1_initiator(socket, session,
            self.dh_keys.into_identity(),
            IdentityExchange::Send { remote: self.remote.1 },
            self.legacy)
    }
}

// Authenticated Upgrades /////////////////////////////////////////////////////

/// A `NoiseAuthenticated` transport upgrade that wraps around any
/// `NoiseConfig` handshake and verifies that the remote identified with a
/// [`RemoteIdentity::IdentityKey`], aborting otherwise.
///
/// See [`NoiseConfig::into_authenticated`].
///
/// On success, the upgrade yields the [`PeerId`] obtained from the
/// `RemoteIdentity`. The output of this upgrade is thus directly suitable for
/// creating an
/// [`authenticated`](libp2p_core::transport::upgrade::Builder::authenticate)
/// transport for use with a [`Network`](libp2p_core::Network).
#[derive(Clone)]
pub struct NoiseAuthenticated<P, C: Zeroize, R> {
    config: NoiseConfig<P, C, R>
}

impl<P, C: Zeroize, R> UpgradeInfo for NoiseAuthenticated<P, C, R>
where
    NoiseConfig<P, C, R>: UpgradeInfo
{
    type Info = <NoiseConfig<P, C, R> as UpgradeInfo>::Info;
    type InfoIter = <NoiseConfig<P, C, R> as UpgradeInfo>::InfoIter;

    fn protocol_info(&self) -> Self::InfoIter {
        self.config.protocol_info()
    }
}

impl<T, P, C, R> InboundUpgrade<T> for NoiseAuthenticated<P, C, R>
where
    NoiseConfig<P, C, R>: UpgradeInfo + InboundUpgrade<T,
        Output = (RemoteIdentity<C>, NoiseOutput<T>),
        Error = NoiseError
    > + 'static,
    <NoiseConfig<P, C, R> as InboundUpgrade<T>>::Future: Send,
    T: AsyncRead + AsyncWrite + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (PeerId, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, socket: T, info: Self::Info) -> Self::Future {
        Box::pin(self.config.upgrade_inbound(socket, info)
            .and_then(|(remote, io)| match remote {
                RemoteIdentity::IdentityKey(pk) => future::ok((pk.into_peer_id(), io)),
                _ => future::err(NoiseError::AuthenticationFailed)
            }))
    }
}

impl<T, P, C, R> OutboundUpgrade<T> for NoiseAuthenticated<P, C, R>
where
    NoiseConfig<P, C, R>: UpgradeInfo + OutboundUpgrade<T,
        Output = (RemoteIdentity<C>, NoiseOutput<T>),
        Error = NoiseError
    > + 'static,
    <NoiseConfig<P, C, R> as OutboundUpgrade<T>>::Future: Send,
    T: AsyncRead + AsyncWrite + Send + 'static,
    C: Protocol<C> + AsRef<[u8]> + Zeroize + Send + 'static,
{
    type Output = (PeerId, NoiseOutput<T>);
    type Error = NoiseError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, socket: T, info: Self::Info) -> Self::Future {
        Box::pin(self.config.upgrade_outbound(socket, info)
            .and_then(|(remote, io)| match remote {
                RemoteIdentity::IdentityKey(pk) => future::ok((pk.into_peer_id(), io)),
                _ => future::err(NoiseError::AuthenticationFailed)
            }))
    }
}

/// Legacy configuration options.
#[derive(Clone)]
pub struct LegacyConfig {
    /// Whether to continue sending legacy handshake payloads,
    /// i.e. length-prefixed protobuf payloads inside a length-prefixed
    /// noise frame. These payloads are not interoperable with other
    /// libp2p implementations.
    pub send_legacy_handshake: bool,
    /// Whether to support receiving legacy handshake payloads,
    /// i.e. length-prefixed protobuf payloads inside a length-prefixed
    /// noise frame. These payloads are not interoperable with other
    /// libp2p implementations.
    pub recv_legacy_handshake: bool,
}

impl Default for LegacyConfig {
    fn default() -> Self {
        Self {
            send_legacy_handshake: false,
            recv_legacy_handshake: false,
        }
    }
}

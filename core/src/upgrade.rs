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

//! Contains everything related to upgrading a connection or a substream to use a protocol.
//!
//! After a connection with a remote has been successfully established or a substream successfully
//! opened, the next step is to *upgrade* this connection or substream to use a protocol.
//!
//! This is where the `UpgradeInfo`, `InboundUpgrade` and `OutboundUpgrade` traits come into play.
//! The `InboundUpgrade` and `OutboundUpgrade` traits are implemented on types that represent a
//! collection of one or more possible protocols for respectively an ingoing or outgoing
//! connection or substream.
//!
//! > **Note**: Multiple versions of the same protocol are treated as different protocols.
//! >           For example, `/foo/1.0.0` and `/foo/1.1.0` are totally unrelated as far as
//! >           upgrading is concerned.
//!
//! # Upgrade process
//!
//! An upgrade is performed in two steps:
//!
//! - A protocol negotiation step. The `UpgradeInfo::protocol_info` method is called to determine
//!   which protocols are supported by the trait implementation. The `multistream-select` protocol
//!   is used in order to agree on which protocol to use amongst the ones supported.
//!
//! - A handshake. After a successful negotiation, the `InboundUpgrade::upgrade_inbound` or
//!   `OutboundUpgrade::upgrade_outbound` method is called. This method will return a `Future` that
//!   performs a handshake. This handshake is considered mandatory, however in practice it is
//!   possible for the trait implementation to return a dummy `Future` that doesn't perform any
//!   action and immediately succeeds.
//!
//! After an upgrade is successful, an object of type `InboundUpgrade::Output` or
//! `OutboundUpgrade::Output` is returned. The actual object depends on the implementation and
//! there is no constraint on the traits that it should implement, however it is expected that it
//! can be used by the user to control the behaviour of the protocol.
//!
//! > **Note**: You can use the `apply_inbound` or `apply_outbound` methods to try upgrade a
//!             connection or substream. However if you use the recommended `Swarm` or
//!             `ProtocolsHandler` APIs, the upgrade is automatically handled for you and you don't
//!             need to use these methods.
//!

mod apply;
mod denied;
mod either;
mod error;
mod from_fn;
mod map;
mod optional;
mod select;
mod transfer;

use futures::future::Future;

pub use crate::Negotiated;
pub use multistream_select::{NegotiatedComplete, NegotiationError, ProtocolError, Role};
pub use self::{
    apply::{
        apply, apply_authentication, apply_inbound, apply_outbound, InboundUpgradeApply,
        OutboundUpgradeApply, AuthenticationUpgradeApply, Version, AuthenticationVersion,
    },
    denied::DeniedUpgrade,
    either::EitherUpgrade,
    error::UpgradeError,
    from_fn::{from_fn, FromFnUpgrade},
    map::{MapInboundUpgrade, MapOutboundUpgrade, MapInboundUpgradeErr, MapOutboundUpgradeErr},
    optional::OptionalUpgrade,
    select::SelectUpgrade,
    transfer::{write_length_prefixed, write_varint, read_length_prefixed, read_varint},
};
#[allow(deprecated)]
pub use self::transfer::ReadOneError;

/// Types serving as protocol names.
///
/// # Context
///
/// In situations where we provide a list of protocols that we support,
/// the elements of that list are required to implement the [`ProtocolName`] trait.
///
/// Libp2p will call [`ProtocolName::protocol_name`] on each element of that list, and transmit the
/// returned value on the network. If the remote accepts a given protocol, the element
/// serves as the return value of the function that performed the negotiation.
///
/// # Example
///
/// ```
/// use libp2p_core::ProtocolName;
///
/// enum MyProtocolName {
///     Version1,
///     Version2,
///     Version3,
/// }
///
/// impl ProtocolName for MyProtocolName {
///     fn protocol_name(&self) -> &[u8] {
///         match *self {
///             MyProtocolName::Version1 => b"/myproto/1.0",
///             MyProtocolName::Version2 => b"/myproto/2.0",
///             MyProtocolName::Version3 => b"/myproto/3.0",
///         }
///     }
/// }
/// ```
///
pub trait ProtocolName {
    /// The protocol name as bytes. Transmitted on the network.
    ///
    /// **Note:** Valid protocol names must start with `/` and
    /// not exceed 140 bytes in length.
    fn protocol_name(&self) -> &[u8];
}

impl<T: AsRef<[u8]>> ProtocolName for T {
    fn protocol_name(&self) -> &[u8] {
        self.as_ref()
    }
}

/// Common trait for upgrades that can be applied on inbound substreams, outbound substreams,
/// or both.
pub trait UpgradeInfo {
    /// Opaque type representing a negotiable protocol.
    type Info: ProtocolName + Clone;
    /// Iterator returned by `protocol_info`.
    type InfoIter: IntoIterator<Item = Self::Info>;

    /// Returns the list of protocols that are supported. Used during the negotiation process.
    fn protocol_info(&self) -> Self::InfoIter;
}

/// Possible upgrade on an inbound connection or substream.
pub trait InboundUpgrade<C>: UpgradeInfo {
    /// Output after the upgrade has been successfully negotiated and the handshake performed.
    type Output;
    /// Possible error during the handshake.
    type Error;
    /// Future that performs the handshake with the remote.
    type Future: Future<Output = Result<Self::Output, Self::Error>>;

    /// After we have determined that the remote supports one of the protocols we support, this
    /// method is called to start the handshake.
    ///
    /// The `info` is the identifier of the protocol, as produced by `protocol_info`.
    fn upgrade_inbound(self, socket: C, info: Self::Info) -> Self::Future;
}

/// Extension trait for `InboundUpgrade`. Automatically implemented on all types that implement
/// `InboundUpgrade`.
pub trait InboundUpgradeExt<C>: InboundUpgrade<C> {
    /// Returns a new object that wraps around `Self` and applies a closure to the `Output`.
    fn map_inbound<F, T>(self, f: F) -> MapInboundUpgrade<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Output) -> T
    {
        MapInboundUpgrade::new(self, f)
    }

    /// Returns a new object that wraps around `Self` and applies a closure to the `Error`.
    fn map_inbound_err<F, T>(self, f: F) -> MapInboundUpgradeErr<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Error) -> T
    {
        MapInboundUpgradeErr::new(self, f)
    }
}

impl<C, U: InboundUpgrade<C>> InboundUpgradeExt<C> for U {}

/// Possible upgrade on an outbound connection or substream.
pub trait OutboundUpgrade<C>: UpgradeInfo {
    /// Output after the upgrade has been successfully negotiated and the handshake performed.
    type Output;
    /// Possible error during the handshake.
    type Error;
    /// Future that performs the handshake with the remote.
    type Future: Future<Output = Result<Self::Output, Self::Error>>;

    /// After we have determined that the remote supports one of the protocols we support, this
    /// method is called to start the handshake.
    ///
    /// The `info` is the identifier of the protocol, as produced by `protocol_info`.
    fn upgrade_outbound(self, socket: C, info: Self::Info) -> Self::Future;
}

/// Extention trait for `OutboundUpgrade`. Automatically implemented on all types that implement
/// `OutboundUpgrade`.
pub trait OutboundUpgradeExt<C>: OutboundUpgrade<C> {
    /// Returns a new object that wraps around `Self` and applies a closure to the `Output`.
    fn map_outbound<F, T>(self, f: F) -> MapOutboundUpgrade<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Output) -> T
    {
        MapOutboundUpgrade::new(self, f)
    }

    /// Returns a new object that wraps around `Self` and applies a closure to the `Error`.
    fn map_outbound_err<F, T>(self, f: F) -> MapOutboundUpgradeErr<Self, F>
    where
        Self: Sized,
        F: FnOnce(Self::Error) -> T
    {
        MapOutboundUpgradeErr::new(self, f)
    }
}

impl<C, U: OutboundUpgrade<C>> OutboundUpgradeExt<C> for U {}

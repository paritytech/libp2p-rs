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

use crate::message_proto::{CircuitRelay, circuit_relay};
use crate::protocol::{send_read, SendReadError, SendReadFuture};
use libp2p_core::{upgrade, Multiaddr, PeerId};
use std::iter;
use futures::prelude::*;
use prost::Message;

/// Ask the remote to become a destination. The upgrade succeeds if the remote accepts, and fails
/// if the remote refuses.
///
/// If we take a situation where a *source* wants to talk to a *destination* through a *relay*,
/// this struct is the message that the *relay* sends to the *destination* at initialization. The
/// parameters passed to `RelayTargetOpen::new()` are the information of the *source* and the
/// *destination* (not the information of the *relay*).
///
/// The upgrade should be performed on a substream to the *destination*.
///
/// If the upgrade succeeds, the substream is returned and we must link it with the data sent from
/// the source.
#[derive(Debug, Clone)] // TODO: better Debug
pub struct RelayTargetOpen<TUserData> {
    /// The message to send to the destination. Pre-computed.
    message: Vec<u8>,
    /// User data, passed back on success or error.
    user_data: TUserData,
}

impl<TUserData> RelayTargetOpen<TUserData> {
    /// Creates a `RelayTargetOpen`. Must pass the parameters of the message.
    ///
    /// The `user_data` is passed back in the result.
    // TODO: change parameters?
    pub(crate) fn new(
        src_id: PeerId,
        src_addresses: impl IntoIterator<Item = Multiaddr>,
        user_data: TUserData,
    ) -> Self {
        let message = CircuitRelay {
            r#type:  Some(circuit_relay::Type::Stop.into()),
            src_peer: Some(circuit_relay::Peer {
                id: src_id.as_bytes().to_vec(),
                addrs: src_addresses.into_iter().map(|a| a.to_vec()).collect(),
            }),
            dst_peer: None,
            code: None,
        };
        let mut encoded_msg = Vec::new();
        // TODO: handle error?
        message.encode(&mut encoded_msg).expect("all the mandatory fields are always filled; QED");


        RelayTargetOpen {
            message: encoded_msg,
            user_data,
        }
    }
}

impl<TUserData> upgrade::UpgradeInfo for RelayTargetOpen<TUserData> {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/libp2p/relay/circuit/0.1.0")
    }
}

impl<TSubstream, TUserData> upgrade::OutboundUpgrade<TSubstream> for RelayTargetOpen<TUserData>
where
    TSubstream: AsyncRead + AsyncWrite + Unpin,
{
    type Output = (TSubstream, TUserData);
    type Error = SendReadError;
    type Future = SendReadFuture<TSubstream, TUserData>;

    fn upgrade_outbound(
        self,
        substream: TSubstream,
        _: Self::Info,
    ) -> Self::Future {
        send_read(substream, self.message, self.user_data)
    }
}
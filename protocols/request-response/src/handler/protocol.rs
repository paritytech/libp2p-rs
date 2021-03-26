// Copyright 2020 Parity Technologies (UK) Ltd.
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

//! The definition of a request/response protocol via inbound
//! and outbound substream upgrades. The inbound upgrade
//! receives a request and sends a response, whereas the
//! outbound upgrade send a request and receives a response.

use crate::RequestId;
use crate::codec::RequestResponseCodec;

use futures::{channel::oneshot, future::BoxFuture, prelude::*};
use libp2p_core::upgrade::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p_swarm::NegotiatedSubstream;
use smallvec::SmallVec;
use std::io;

/// The level of support for a particular protocol.
#[derive(Debug, Clone)]
pub enum ProtocolSupport {
    /// The protocol is only supported for inbound requests.
    Inbound,
    /// The protocol is only supported for outbound requests.
    Outbound,
    /// The protocol is supported for inbound and outbound requests.
    Full
}

impl ProtocolSupport {
    /// Whether inbound requests are supported.
    pub fn inbound(&self) -> bool {
        match self {
            ProtocolSupport::Inbound | ProtocolSupport::Full => true,
            ProtocolSupport::Outbound => false,
        }
    }

    /// Whether outbound requests are supported.
    pub fn outbound(&self) -> bool {
        match self {
            ProtocolSupport::Outbound | ProtocolSupport::Full => true,
            ProtocolSupport::Inbound => false,
        }
    }
}

/// Response substream upgrade protocol.
///
/// Receives a request and sends a response.
#[derive(Debug)]
pub struct ResponseProtocol<TCodec>
where
    TCodec: RequestResponseCodec
{
    pub(crate) codec: TCodec,
    pub(crate) protocols: SmallVec<[TCodec::Protocol; 2]>,
    pub(crate) request_sender: oneshot::Sender<(RequestId, TCodec::Request)>,
    pub(crate) response_receiver: oneshot::Receiver<TCodec::Response>,
    pub(crate) request_id: RequestId

}

impl<TCodec> UpgradeInfo for ResponseProtocol<TCodec>
where
    TCodec: RequestResponseCodec
{
    type Info = TCodec::Protocol;
    type InfoIter = smallvec::IntoIter<[Self::Info; 2]>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocols.clone().into_iter()
    }
}

impl<TCodec> InboundUpgrade<NegotiatedSubstream> for ResponseProtocol<TCodec>
where
    TCodec: RequestResponseCodec + Send + 'static,
{
    type Output = bool;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(mut self, mut io: NegotiatedSubstream, protocol: Self::Info) -> Self::Future {
        async move {
            let read = self.codec.read_request(&protocol, &mut io);
            let request = read.await?;
            match self.request_sender.send((self.request_id, request)) {
                Ok(()) => {},
                Err(_) => panic!(
                    "Expect request receiver to be alive i.e. protocol handler to be alive.",
                ),
            }

            if let Ok(response) = self.response_receiver.await {
                let write = self.codec.write_response(&protocol, &mut io, response);
                write.await?;

                io.close().await?;
                // Response was sent. Indicate to handler to emit a `ResponseSent` event.
                Ok(true)
            } else {
                io.close().await?;
                // No response was sent. Indicate to handler to emit a `ResponseOmission` event.
                Ok(false)
            }
        }.boxed()
    }
}

/// Request substream upgrade protocol.
///
/// Sends a request and receives a response.
#[derive(Debug)]
pub struct RequestProtocol<TCodec>
where
    TCodec: RequestResponseCodec
{
    pub(crate) codec: TCodec,
    pub(crate) protocols: SmallVec<[TCodec::Protocol; 2]>,
    pub(crate) request_id: RequestId,
    pub(crate) request: TCodec::Request,
}

impl<TCodec> UpgradeInfo for RequestProtocol<TCodec>
where
    TCodec: RequestResponseCodec
{
    type Info = TCodec::Protocol;
    type InfoIter = smallvec::IntoIter<[Self::Info; 2]>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.protocols.clone().into_iter()
    }
}

impl<TCodec> OutboundUpgrade<NegotiatedSubstream> for RequestProtocol<TCodec>
where
    TCodec: RequestResponseCodec + Send + 'static,
{
    type Output = TCodec::Response;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(mut self, mut io: NegotiatedSubstream, protocol: Self::Info) -> Self::Future {
        async move {
            let write = self.codec.write_request(&protocol, &mut io, self.request);
            write.await?;
            io.close().await?;
            let read = self.codec.read_response(&protocol, &mut io);
            let response = read.await?;
            Ok(response)
        }.boxed()
    }
}

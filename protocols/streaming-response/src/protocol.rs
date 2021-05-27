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

use derive_more::{Add, Deref, Display, Sub};
use futures::{
    future::BoxFuture,
    io::{AsyncRead, AsyncWrite},
};
use libp2p_core::{upgrade, InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    io::{self, Error, ErrorKind, Result},
    marker::PhantomData,
};

/// A [`Codec`] defines the request and response types for a [`StreamingResponse`]
/// protocol. Request and responses are encoded / decoded using `serde_cbor`, so
/// `Serialize` and `Deserialize` impls have to be provided. Implement this trait
/// to specialize the [`StreamingResponse`].
pub trait Codec {
    type Request: Send + Serialize + DeserializeOwned;
    type Response: Send + Serialize + DeserializeOwned;

    fn protocol_info() -> &'static [u8];
}

/// Local requestId
#[derive(Debug, Default, Copy, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct RequestId(pub(crate) u64);

#[derive(
    Debug,
    Serialize,
    Deserialize,
    Default,
    Copy,
    Clone,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Display,
    Add,
    Sub,
    Deref,
)]
// SequenceNo for responses
pub struct SequenceNo(pub(crate) u64);
impl SequenceNo {
    pub fn increment(&mut self) {
        self.0 += 1
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StreamingResponseMessage<TCodec: Codec> {
    /// Initiate a request
    Request {
        id: RequestId,
        payload: TCodec::Request,
    },
    /// Cancel an ongoing request
    CancelRequest { id: RequestId },
    /// Single response frame
    Response {
        id: RequestId,
        seq_no: SequenceNo,
        payload: TCodec::Response,
    },
    /// Response ended
    ResponseEnd { id: RequestId, seq_no: SequenceNo },
}

#[derive(Clone, Debug)]
pub struct StreamingResponseConfig<TCodec: Codec> {
    /// Maximum size in bytes accepted for incoming requests
    max_buf_size: usize,
    /// Serializes all outgoing responses, effectively making the stream FIFO
    pub(crate) ordered_outgoing: bool,
    _c: PhantomData<TCodec>,
}

impl<TCodec> Default for StreamingResponseConfig<TCodec>
where
    TCodec: Codec,
{
    fn default() -> Self {
        Self {
            max_buf_size: 1024 * 1024 * 4,
            ordered_outgoing: true,
            _c: PhantomData,
        }
    }
}

impl<TCodec> UpgradeInfo for StreamingResponseConfig<TCodec>
where
    TCodec: Codec,
{
    type Info = &'static [u8];
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(TCodec::protocol_info())
    }
}

impl<TSocket, TCodec> InboundUpgrade<TSocket> for StreamingResponseConfig<TCodec>
where
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    TCodec: Codec + Send + 'static,
{
    type Output = StreamingResponseMessage<TCodec>;
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Output>>;

    fn upgrade_inbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let packet = upgrade::read_one(&mut socket, self.max_buf_size)
                .await
                .map_err(|err| {
                    use upgrade::ReadOneError::*;
                    match err {
                        Io(err) => err,
                        TooLarge { .. } => Error::new(ErrorKind::InvalidData, format!("{}", err)),
                    }
                })?;
            let request = serde_cbor::from_slice(&packet)
                .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;

            Ok(request)
        })
    }
}

impl<TCodec> UpgradeInfo for StreamingResponseMessage<TCodec>
where
    TCodec: Codec,
{
    type Info = &'static [u8];
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(TCodec::protocol_info())
    }
}

impl<TSocket, TCodec> OutboundUpgrade<TSocket> for StreamingResponseMessage<TCodec>
where
    TCodec: Codec + 'static,
    TSocket: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Output = ();
    type Error = Error;
    type Future = BoxFuture<'static, Result<Self::Output>>;

    fn upgrade_outbound(self, mut socket: TSocket, _info: Self::Info) -> Self::Future {
        Box::pin(async move {
            let bytes = serde_cbor::to_vec(&self)
                .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
            upgrade::write_one(&mut socket, bytes).await?;
            Ok(())
        })
    }
}

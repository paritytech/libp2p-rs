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

use crate::error::PlainTextError;

use bytes::Bytes;
use futures::future::{self, Ready};
use futures::prelude::*;
use futures::future::BoxFuture;
use libp2p_core::{
    identity,
    InboundUpgrade,
    OutboundUpgrade,
    UpgradeInfo,
    PeerId,
    PublicKey,
};
use log::debug;
use std::{io, iter, pin::Pin, task::{Context, Poll}};
use void::Void;

mod error;
mod handshake;
mod structs_proto {
    include!(concat!(env!("OUT_DIR"), "/structs.rs"));
}


/// `PlainText1Config` is an insecure connection handshake for testing purposes only.
///
/// > **Note**: Given that `PlainText1Config` has no notion of exchanging peer identity information it is not compatible
/// > with the `libp2p_core::transport::upgrade::Builder` pattern. See
/// > [`PlainText2Config`](struct.PlainText2Config.html) if compatibility is needed. Even though not compatible with the
/// > Builder pattern one can still do an upgrade *manually*:
///
/// ```
/// # use libp2p_core::transport::{ Transport, memory::MemoryTransport };
/// # use libp2p_plaintext::PlainText1Config;
/// #
/// MemoryTransport::default()
///   .and_then(move |io, endpoint| {
///     libp2p_core::upgrade::apply(
///       io,
///       PlainText1Config{},
///       endpoint,
///       libp2p_core::upgrade::Version::V1,
///     )
///   })
///   .map(|plaintext, _endpoint| {
///     unimplemented!();
///     // let peer_id = somehow_derive_peer_id();
///     // return (peer_id, plaintext);
///   });
/// ```
#[derive(Debug, Copy, Clone)]
pub struct PlainText1Config;

impl UpgradeInfo for PlainText1Config {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/plaintext/1.0.0")
    }
}

impl<C> InboundUpgrade<C> for PlainText1Config {
    type Output = C;
    type Error = Void;
    type Future = Ready<Result<C, Self::Error>>;

    fn upgrade_inbound(self, i: C, _: Self::Info) -> Self::Future {
        future::ready(Ok(i))
    }
}

impl<C> OutboundUpgrade<C> for PlainText1Config {
    type Output = C;
    type Error = Void;
    type Future = Ready<Result<C, Self::Error>>;

    fn upgrade_outbound(self, i: C, _: Self::Info) -> Self::Future {
        future::ready(Ok(i))
    }
}

/// `PlainText2Config` is an insecure connection handshake for testing purposes only, implementing
/// the libp2p plaintext connection handshake specification.
#[derive(Clone)]
pub struct PlainText2Config {
    pub local_public_key: identity::PublicKey,
}

impl UpgradeInfo for PlainText2Config {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/plaintext/2.0.0")
    }
}

impl<C> InboundUpgrade<C> for PlainText2Config
where
    C: AsyncRead + AsyncWrite + Send + Unpin + 'static
{
    type Output = (PeerId, PlainTextOutput<C>);
    type Error = PlainTextError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, socket: C, _: Self::Info) -> Self::Future {
        Box::pin(self.handshake(socket))
    }
}

impl<C> OutboundUpgrade<C> for PlainText2Config
where
    C: AsyncRead + AsyncWrite + Send + Unpin + 'static
{
    type Output = (PeerId, PlainTextOutput<C>);
    type Error = PlainTextError;
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        Box::pin(self.handshake(socket))
    }
}

impl PlainText2Config {
    async fn handshake<T>(self, socket: T) -> Result<(PeerId, PlainTextOutput<T>), PlainTextError>
    where
        T: AsyncRead + AsyncWrite + Send + Unpin + 'static
    {
        debug!("Starting plaintext handshake.");
        let (socket, remote, read_buffer) = handshake::handshake(socket, self).await?;
        debug!("Finished plaintext handshake.");

        Ok((
            remote.peer_id,
            PlainTextOutput {
                socket,
                remote_key: remote.public_key,
                read_buffer,
            }
        ))
    }
}

/// Output of the plaintext protocol.
pub struct PlainTextOutput<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// The plaintext stream.
    pub socket: S,
    /// The public key of the remote.
    pub remote_key: PublicKey,
    /// Remaining bytes that have been already buffered
    /// during the handshake but are not part of the
    /// handshake. These must be consumed first by `poll_read`.
    read_buffer: Bytes,
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for PlainTextOutput<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8])
        -> Poll<Result<usize, io::Error>>
    {
        if !self.read_buffer.is_empty() {
            let n = std::cmp::min(buf.len(), self.read_buffer.len());
            let b = self.read_buffer.split_to(n);
            buf[..n].copy_from_slice(&b[..]);
            return Poll::Ready(Ok(n))
        }
        AsyncRead::poll_read(Pin::new(&mut self.socket), cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for PlainTextOutput<S> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8])
        -> Poll<Result<usize, io::Error>>
    {
        AsyncWrite::poll_write(Pin::new(&mut self.socket), cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), io::Error>>
    {
        AsyncWrite::poll_flush(Pin::new(&mut self.socket), cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>)
        -> Poll<Result<(), io::Error>>
    {
        AsyncWrite::poll_close(Pin::new(&mut self.socket), cx)
    }
}

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

use super::copy_future::CopyFuture;
use crate::message_proto::{circuit_relay, circuit_relay::Status, CircuitRelay};
use crate::protocol::Peer;

use asynchronous_codec::{Framed, FramedParts};
use bytes::{BytesMut, Bytes};
use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::prelude::*;
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::time::Duration;
use unsigned_varint::codec::UviBytes;

/// Request from a remote for us to relay communications to another node.
///
/// If we take a situation where a *source* wants to talk to a *destination* through a *relay*, and
/// we are the *relay*, this struct is a message that the *source* sent to us. The parameters
/// passed to `IncomingRelayReq::new()` are the information of the *destination*.
///
/// If the upgrade succeeds, the substream is returned and we will receive data sent from the
/// source on it. This data must be transmitted to the destination.
#[must_use = "An incoming relay request should be either accepted or denied."]
pub struct IncomingRelayReq {
    /// The stream to the source.
    stream: Framed<NegotiatedSubstream, UviBytes>,
    /// Target of the request.
    dest: Peer,

    _notifier: oneshot::Sender<()>,
}

impl IncomingRelayReq
{
    /// Creates a [`IncomingRelayReq`] as well as a Future that resolves once the
    /// [`IncomingRelayReq`] is dropped.
    pub(crate) fn new(
        stream: Framed<NegotiatedSubstream, UviBytes>,
        dest: Peer,
    ) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (
            IncomingRelayReq {
                stream,
                dest,
                _notifier: tx,
            },
            rx,
        )
    }

    /// Peer id of the node we should relay communications to.
    pub(crate) fn dst_peer(&self) -> &Peer {
        &self.dest
    }

    /// Accepts the request by providing a stream to the destination.
    pub fn fulfill<TDestSubstream>(
        mut self,
        dst_stream: TDestSubstream,
        dst_read_buffer: Bytes,
    ) -> BoxFuture<'static, Result<(), IncomingRelayReqError>>
    where
        TDestSubstream: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let msg = CircuitRelay {
            r#type: Some(circuit_relay::Type::Status.into()),
            src_peer: None,
            dst_peer: None,
            code: Some(circuit_relay::Status::Success.into()),
        };
        let mut msg_bytes = BytesMut::new();
        msg.encode(&mut msg_bytes)
            .expect("all the mandatory fields are always filled; QED");

        async move {
            self.stream.send(msg_bytes.freeze()).await?;

            let FramedParts {
                mut io,
                read_buffer,
                write_buffer,
                ..
            } = self.stream.into_parts();
            assert!(
                read_buffer.is_empty(),
                "Expect a Framed, that was never actively read from, not to read."
            );
            assert!(
                write_buffer.is_empty(),
                "Expect a flushed Framed to have empty write buffer."
            );

            if !dst_read_buffer.is_empty() {
                io.write_all(&dst_read_buffer).await?;
            }

            let copy_future = CopyFuture::new(io, dst_stream, Duration::from_secs(5));

            copy_future.await.map_err(Into::into)
        }
        .boxed()
    }

    /// Refuses the request.
    ///
    /// The returned `Future` gracefully shuts down the request.
    pub fn deny(mut self, err_code: Status) -> BoxFuture<'static, Result<(), std::io::Error>> {
        let msg = CircuitRelay {
            r#type: Some(circuit_relay::Type::Status.into()),
            code: Some(err_code.into()),
            src_peer: None,
            dst_peer: None,
        };
        let mut msg_bytes = BytesMut::new();
        msg.encode(&mut msg_bytes)
            .expect("all the mandatory fields are always filled; QED");

        async move {
            self.stream.send(msg_bytes.freeze()).await?;
            Ok(())
        }
        .boxed()
    }
}

#[derive(Debug)]
pub enum IncomingRelayReqError {
    Io(std::io::Error),
}

impl From<std::io::Error> for IncomingRelayReqError {
    fn from(e: std::io::Error) -> Self {
        IncomingRelayReqError::Io(e)
    }
}

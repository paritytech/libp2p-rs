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

use crate::message_proto::{circuit_relay, CircuitRelay};
use crate::protocol::Peer;

use asynchronous_codec::{Framed, FramedParts};
use bytes::BytesMut;
use futures::{future::BoxFuture, prelude::*};
use futures::channel::oneshot;
use libp2p_core::{Multiaddr, PeerId};
use libp2p_swarm::NegotiatedSubstream;
use prost::Message;
use std::io;
use unsigned_varint::codec::UviBytes;

/// Request from a remote for us to become a destination.
///
/// If we take a situation where a *source* wants to talk to a *destination* through a *relay*, and
/// we are the *destination*, this struct is a message that the *relay* sent to us. The
/// parameters passed to `IncomingDstReq::new()` are the information of the *source*.
///
/// If the upgrade succeeds, the substream is returned and we will receive data sent from the
/// source on it.
#[must_use = "An incoming destination request should be either accepted or denied"]
pub struct IncomingDstReq {
    /// The stream to the source.
    stream: Framed<NegotiatedSubstream, UviBytes>,
    /// Source of the request.
    src: Peer,
}

impl IncomingDstReq
{
    /// Creates a `IncomingDstReq`.
    pub(crate) fn new(stream: Framed<NegotiatedSubstream, UviBytes>, src: Peer) -> Self {
        IncomingDstReq {
            stream: stream,
            src,
        }
    }

    /// Returns the peer id of the source that is being relayed.
    pub fn src_id(&self) -> &PeerId {
        &self.src.peer_id
    }

    /// Returns the addresses of the source that is being relayed.
    pub fn src_addrs(&self) -> impl Iterator<Item = &Multiaddr> {
        self.src.addrs.iter()
    }

    /// Accepts the request.
    ///
    /// The returned `Future` sends back a success message then returns the raw stream. This raw
    /// stream then points to the source (as retreived with `src_id()` and `src_addrs()`).
    pub fn accept(
        self,
    ) -> BoxFuture<'static, Result<(PeerId, super::Connection, oneshot::Receiver<()>), IncomingDstReqError>> {
        let IncomingDstReq { mut stream, src } = self;
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
            stream.send(msg_bytes.freeze()).await?;

            let FramedParts {
                io,
                read_buffer,
                write_buffer,
                ..
            } = stream.into_parts();
            assert!(
                write_buffer.is_empty(),
                "Expect a flushed Framed to have empty write buffer."
            );

            let (tx, rx) = oneshot::channel();

            Ok((src.peer_id, super::Connection::new(read_buffer.freeze(), io, tx), rx))
        }
        .boxed()
    }

    /// Refuses the request.
    ///
    /// The returned `Future` gracefully shuts down the request.
    pub fn deny(mut self) -> BoxFuture<'static, Result<(), io::Error>> {
        let msg = CircuitRelay {
            r#type: Some(circuit_relay::Type::Status.into()),
            src_peer: None,
            dst_peer: None,
            code: Some(circuit_relay::Status::StopRelayRefused.into()),
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
pub enum IncomingDstReqError {
    Io(std::io::Error),
}

impl From<std::io::Error> for IncomingDstReqError {
    fn from(e: std::io::Error) -> Self {
        IncomingDstReqError::Io(e)
    }
}

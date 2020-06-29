// Copyright 2017-2020 Parity Technologies (UK) Ltd.
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

//! Future that drives a QUIC connection until is has performed its TLS handshake.

use crate::{
    connection::{Connection, ConnectionEvent},
    muxer::QuicMuxer,
    transport, x509,
};

use futures::prelude::*;
use libp2p_core::PeerId;
use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
};

/// A QUIC connection currently being negotiated.
pub struct Upgrade {
    connection: Option<Connection>,
}

impl Upgrade {
    /// Builds an [`Upgrade`] that wraps around a [`Connection`].
    pub(crate) fn from_connection(connection: Connection) -> Self {
        Upgrade {
            connection: Some(connection),
        }
    }
}

impl Future for Upgrade {
    type Output = Result<(PeerId, QuicMuxer), transport::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let connection = match self.connection.as_mut() {
            Some(c) => c,
            None => panic!("Future polled after it has ended"),
        };

        loop {
            if !connection.is_handshaking() {
                let mut certificates = match connection.peer_certificates() {
                    Some(certificates) => certificates,
                    None => continue,
                };
                let peer_id = x509::extract_peerid_or_panic(certificates.next().unwrap().as_der()); // TODO: bad API
                let muxer = QuicMuxer::from_connection(self.connection.take().unwrap());
                return Poll::Ready(Ok((peer_id, muxer)));
            }

            match Connection::poll_event(connection, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(ConnectionEvent::Connected) => {
                    // `is_handshaking()` will return `false` at the next loop iteration.
                    continue;
                }
                Poll::Ready(ConnectionEvent::ConnectionLost(err)) => {
                    return Poll::Ready(Err(transport::Error::Established(err)));
                }
                Poll::Ready(ConnectionEvent::StreamOpened)
                | Poll::Ready(ConnectionEvent::StreamReadable(_)) => continue,
                // TODO: enumerate the items and explain how they can't happen
                Poll::Ready(e) => unreachable!("{:?}", e),
            }
        }
    }
}

impl fmt::Debug for Upgrade {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&self.connection, f)
    }
}

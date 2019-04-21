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

use serde_derive::{Serialize, Deserialize};

/// Message that can be transmitted over a stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RawMessage {
    /// Pushes the sender to a remote. Contains the addresses it's listening on, and a nonce.
    ///
    /// The message is valid only if
    /// `leading_zero_bits(sha256(sender_id | receiver_id | nonce)) < difficulty`, where
    /// `difficulty` is a parameter in the protocol configuration.
    Push(Vec<Vec<u8>>, u32),

    /// Sender requests the remote to send back their view of the network.
    PullRequest,

    /// Response to a `PullRequest`. Contains the peers and addresses how to reach them.
    PullResponse(Vec<(Vec<u8>, Vec<Vec<u8>>)>),
}

impl RawMessage {
    /// Turns this message into raw bytes that can be sent over the network.
    #[inline]
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        serde_cbor::to_vec(&self).expect("encoding with serde_cbor cannot fail; QED")
    }

    /// Parses bytes into a `RawMessages`.
    #[inline]
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        serde_cbor::from_slice(bytes).unwrap()  // TODO: don't panic
    }
}

#[cfg(test)]
mod tests {
    use super::RawMessage;

    #[test]
    fn push_ser_de() {
        let message = {
            let data = (0..rand::random::<usize>() % 30)
                .map(|_| {
                    (0..rand::random::<usize>() % 30).map(|_| rand::random::<u8>()).collect()
                })
                .collect::<Vec<_>>();
            RawMessage::Push(data, rand::random())
        };

        assert_eq!(RawMessage::from_bytes(&message.clone().into_bytes()), message);
    }

    #[test]
    fn pull_rq_ser_de() {
        let message = RawMessage::PullRequest;
        assert_eq!(RawMessage::from_bytes(&message.clone().into_bytes()), message);
    }
}

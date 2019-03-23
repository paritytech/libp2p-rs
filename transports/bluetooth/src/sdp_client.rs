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

//! Client for the SDP (Service Discovery Protocol) protocol.
//!
//! Allows connecting to a client and querying which services are provided.
//!
//! Documented in the Bluetooth core specifications, Volume 3 part B.

use super::l2cap;
use crate::Addr;
use byteorder::{WriteBytesExt, BigEndian, LittleEndian};
use futures::prelude::*;
use smallvec::SmallVec;
use std::{io, mem, os::raw::c_int, os::raw::c_void};
use tokio_io::AsyncWrite;

const PUBLIC_BROWSE_GROUP: u16 = 0x1002;
//#define SDP_RESPONSE_TIMEOUT	20
const SDP_REQ_BUFFER_SIZE: usize = 2048;
const SDP_RSP_BUFFER_SIZE: usize = 65535;
const SDP_PDU_CHUNK_SIZE: usize = 1024;

const SDP_SVC_SEARCH_ATTR_REQ: u8 = 0x06;
const SDP_SVC_SEARCH_ATTR_RSP: u8 = 0x07;

mod protocol;

pub use protocol::{AttributeSearch, Data, Uuid};

/// Non-blocking socket for SDP queries with a remote.
pub struct SdpClient {
    /// Socket for communications. Implements `Sink` and `Stream` for `protocol::Packet`.
    socket: tokio_codec::Framed<tokio_reactor::PollEvented<l2cap::L2capSocket>, protocol::Codec>,
    /// Queue of packets to try to send.
    send_queue: SmallVec<[protocol::Packet; 4]>,
    /// Need to call `poll_complete()` on the socket.
    needs_poll_complete: bool,
    /// List of pending requests.
    requests: SmallVec<[OngoingRequest; 4]>,
    /// Id of the next request to assign.
    next_transaction_id: u16,
}

/// State of a request being made by us.
struct OngoingRequest {
    /// Id we have assigned for this request.
    transaction_id: u16,
    /// Either a single `0` at initialization, or a value returned by the server in the previous
    /// iteration so that we continue the query.
    continuation_state: protocol::ContinuationState,
}

impl SdpClient {
    /// Builds a new client that tries to connect to the given address.
    pub fn connect(addr: Addr) -> Result<SdpClient, io::Error> {
        let socket = l2cap::L2capSocket::connect(addr, 0x1)?;

        Ok(SdpClient {
            socket: tokio_codec::Framed::new(tokio_reactor::PollEvented::new(socket), Default::default()),
            send_queue: SmallVec::new(),
            needs_poll_complete: false,
            requests: SmallVec::new(),
            next_transaction_id: 0,
        })
    }

    /// Starts a request. You need to poll the client in order to receive the response.
    pub fn start_request(&mut self, searched_uuid: Uuid, attributes: impl Iterator<Item = AttributeSearch>) -> RequestId {
        // Assign an ID for the request.
        let transaction_id = {
            let i = self.next_transaction_id;
            loop {
                self.next_transaction_id = self.next_transaction_id.wrapping_add(1);
                if !self.requests.iter().any(|r| r.transaction_id == self.next_transaction_id) {
                    break;
                }
            }
            i
        };

        self.requests.push(OngoingRequest {
            transaction_id,
            continuation_state: protocol::ContinuationState::zero(),
        });

        self.send_queue.push(protocol::Packet {
            transaction_id,
            packet_ty: protocol::PacketTy::ServiceSearchAttributeRequest {
                service_search_pattern: vec![searched_uuid],
                maximum_attribute_byte_count: u16::max_value(),
                attribute_id_list: attributes.collect(),
                continuation_state: protocol::ContinuationState::zero(),
            },
        });

        RequestId(transaction_id)
    }

    /// Polls the client for events that happened on the network.
    // TODO: timeout?
    pub fn poll(&mut self) -> Poll<SdpClientEvent, io::Error> {
        // Try writing the send queue.
        while !self.send_queue.is_empty() {
            let elem = self.send_queue.remove(0);
            match self.socket.start_send(elem)? {
                AsyncSink::NotReady(packet) => { self.send_queue.insert(0, packet); break; }
                AsyncSink::Ready => self.needs_poll_complete = true,
            }
        }

        // Flush the writings.
        if self.needs_poll_complete {
            match self.socket.poll_complete()? {
                Async::Ready(()) => self.needs_poll_complete = false,
                Async::NotReady => (),
            }
        }

        // Try reading a packet.
        while let Async::Ready(Some(packet)) = self.socket.poll()? {
            // Find the corresponding request we made earlier and extract it from the list. We are
            // only a client, so we ignore requests potentially coming from the remote.
            let request = match self.requests.iter().position(|rq| rq.transaction_id == packet.transaction_id) {
                Some(rq) => self.requests.remove(rq),
                None => continue,
            };

            match packet.packet_ty {
                // Error produced an error for our request.
                protocol::PacketTy::ErrorResponse(code) => {
                    return Ok(Async::Ready(SdpClientEvent::RequestFailed {
                        request_id: RequestId(request.transaction_id),
                        reason: code.message(),
                    }))
                },

                protocol::PacketTy::ServiceSearchAttributeResponse { attribute_lists, continuation_state } => {
                    assert!(continuation_state.is_zero());  // not implemented
                    return Ok(Async::Ready(SdpClientEvent::RequestSuccess {
                        request_id: RequestId(request.transaction_id),
                        results: attribute_lists,
                    }))
                },

                // TODO:

                _ => ()
            }

            // TODO:
        }

        Ok(Async::NotReady)
    }
}

#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub struct RequestId(u16);

/// Event that can be produced by the SDP client.
#[derive(Debug)]
pub enum SdpClientEvent {
    /// A request has failed.
    RequestFailed {
        /// Identifier of the request that failed.
        request_id: RequestId,
        // TODO: proper error type?
        /// Message indicating the reason.
        reason: &'static str,
    },

    /// A request has succeeded.
    RequestSuccess {
        /// Identifier of the request that succeeded.
        request_id: RequestId,
        /// Results of the request.
        results: Vec<Vec<(u16, Data)>>,
    },
}

#[cfg(test)]
mod tests {
    use super::SdpClient;
    use futures::{prelude::*, future, try_ready};
    use std::io;

    #[test]
    fn test() {
        let mut client = SdpClient::connect("3C:77:E6:F0:FD:A2".parse().unwrap()).unwrap();
        client.start_request();
        let future = future::poll_fn(move || -> Poll<(), io::Error> {
            loop {
                let ev = try_ready!(client.poll());
                println!("{:?}", ev);
            }
        });
        tokio::runtime::Runtime::new().unwrap().block_on(future).unwrap();
    }
}

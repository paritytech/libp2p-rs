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

//! Provides the `KadMsg` enum of all the possible messages transmitted with the Kademlia protocol,
//! and the `KademliaProtocolConfig` connection upgrade whose output is a
//! `Stream<Item = KadMsg> + Sink<SinkItem = KadMsg>`.
//!
//! The `Stream` component is used to poll the underlying transport, and the `Sink` component is
//! used to send messages.

use bytes::Bytes;
use error::KadError;
use futures::{IntoFuture, Sink, Stream};
use futures::future::FutureResult;
use libp2p_peerstore::PeerId;
use libp2p_swarm::{ConnectionUpgrade, Endpoint, Multiaddr};
use protobuf::{self, Message};
use protobuf_structs;
use std::io::Error as IoError;
use std::iter;
use tokio_io::{AsyncRead, AsyncWrite};
use varint::VarintCodec;

#[derive(Copy, Clone, PartialEq, Eq, Debug, Hash)]
pub enum ConnectionType {
	/// Sender hasn't tried to connect to peer.
	NotConnected = 0,
	/// Sender is currently connected to peer.
	Connected = 1,
	/// Sender was recently connected to peer.
	CanConnect = 2,
	/// Sender tried to connect to peer but failed.
	CannotConnect = 3,
}

impl From<protobuf_structs::dht::Message_ConnectionType> for ConnectionType {
	#[inline]
	fn from(raw: protobuf_structs::dht::Message_ConnectionType) -> ConnectionType {
		use protobuf_structs::dht::Message_ConnectionType::*;
		match raw {
			NOT_CONNECTED => ConnectionType::NotConnected,
			CONNECTED => ConnectionType::Connected,
			CAN_CONNECT => ConnectionType::CanConnect,
			CANNOT_CONNECT => ConnectionType::CannotConnect,
		}
	}
}

impl Into<protobuf_structs::dht::Message_ConnectionType> for ConnectionType {
	#[inline]
	fn into(self) -> protobuf_structs::dht::Message_ConnectionType {
		use protobuf_structs::dht::Message_ConnectionType::*;
		match self {
			ConnectionType::NotConnected => NOT_CONNECTED,
			ConnectionType::Connected => CONNECTED,
			ConnectionType::CanConnect => CAN_CONNECT,
			ConnectionType::CannotConnect => CANNOT_CONNECT,
		}
	}
}

/// Information about a peer, as known by the sender.
#[derive(Debug, Clone)]
pub struct Peer {
	pub node_id: PeerId,
	/// The multiaddresses that are known for that peer.
	pub multiaddrs: Vec<Multiaddr>,
	pub connection_ty: ConnectionType,
}

impl<'a> From<&'a mut protobuf_structs::dht::Message_Peer> for Peer {
	fn from(peer: &'a mut protobuf_structs::dht::Message_Peer) -> Peer {
		let node_id = PeerId::from_bytes(peer.get_id().to_vec()).unwrap(); // TODO: don't unwrap
		let addrs = peer.take_addrs()
			.into_iter()
			.map(|a| Multiaddr::from_bytes(a))
			.collect();
		let connection_ty = peer.get_connection().into();

		Peer {
			node_id: node_id,
			multiaddrs: addrs,
			connection_ty: connection_ty,
		}
	}
}

impl Into<protobuf_structs::dht::Message_Peer> for Peer {
	fn into(self) -> protobuf_structs::dht::Message_Peer {
		let mut out = protobuf_structs::dht::Message_Peer::new();
		out.set_id(self.node_id.into_bytes());
		for addr in self.multiaddrs {
			out.mut_addrs().push(addr.into_bytes());
		}
		out.set_connection(self.connection_ty.into());
		out
	}
}

/// Configuration for a Kademlia connection upgrade. When applied to a connection, turns this
/// connection into a `Stream + Sink` whose items are of type `KadMsg`.
#[derive(Debug, Default, Copy, Clone)]
pub struct KademliaProtocolConfig;

impl<C> ConnectionUpgrade<C> for KademliaProtocolConfig
where
	C: AsyncRead + AsyncWrite + 'static, // TODO: 'static :-/
{
	type Output = Box<
		KadStreamSink<Item = KadMsg, Error = KadError, SinkItem = KadMsg, SinkError = KadError>,
	>;
	type Future = FutureResult<Self::Output, IoError>;
	type NamesIter = iter::Once<(Bytes, ())>;
	type UpgradeIdentifier = ();

	#[inline]
	fn protocol_names(&self) -> Self::NamesIter {
		iter::once(("/ipfs/kad/1.0.0".into(), ()))
	}

	#[inline]
	fn upgrade(self, incoming: C, _: (), _: Endpoint, _: &Multiaddr) -> Self::Future {
		Ok(kademlia_protocol(incoming)).into_future()
	}
}

// Upgrades a socket to use the Kademlia protocol.
fn kademlia_protocol<'a, S>(
	socket: S,
) -> Box<KadStreamSink<Item = KadMsg, Error = KadError, SinkItem = KadMsg, SinkError = KadError> + 'a>
where
	S: AsyncRead + AsyncWrite + 'a,
{
	let wrapped = socket
		.framed(VarintCodec::default())
		.from_err::<KadError>()
		.with(|request| -> Result<_, KadError> {
			let proto_struct = msg_to_proto(request);
			Ok(proto_struct.write_to_bytes().unwrap()) // TODO: error?
		})
		.and_then(|bytes| {
			if let Ok(response) = protobuf::parse_from_bytes(&bytes) {
				Ok(proto_to_msg(response))
			} else {
				Err(KadError::Failure)
			}
		});

	Box::new(wrapped)
}

/// Custom trait that derives `Sink` and `Stream`, so that we can box it.
pub trait KadStreamSink
	: Stream<Item = KadMsg, Error = KadError> + Sink<SinkItem = KadMsg, SinkError = KadError> {
}
impl<T> KadStreamSink for T
where
	T: Stream<Item = KadMsg, Error = KadError> + Sink<SinkItem = KadMsg, SinkError = KadError>,
{
}

/// Message that we can send to a peer or received from a peer.
// TODO: document the rest
#[derive(Debug, Clone)]
pub enum KadMsg {
	/// Ping request or response.
	Ping,
	/// Target must save the given record, can be queried later with `GetValueReq`.
	PutValue {
		/// Identifier of the record.
		key: Vec<u8>,
		/// The record itself.
		record: protobuf_structs::record::Record, // TODO: no
	},
	GetValueReq {
		/// Identifier of the record.
		key: Vec<u8>,
		cluster_level: u32,
	},
	GetValueRes {
		/// Identifier of the returned record.
		key: Vec<u8>,
		cluster_level: u32,
		record: Option<protobuf_structs::record::Record>, // TODO: no
		closer_peers: Vec<Peer>,
	},
	/// Request for the list of nodes whose IDs are the closest to `key`. The number of nodes
	/// returned is not specified, but should be around 20.
	FindNodeReq {
		/// Identifier of the node.
		key: Vec<u8>,
		cluster_level: u32,
	},
	/// Response to a `FindNodeReq`.
	FindNodeRes {
		cluster_level: u32,
		/// Results of the request.
		closer_peers: Vec<Peer>,
	},
	GetProvidersReq {
		key: Vec<u8>,
		cluster_level: u32,
	},
	GetProvidersRes {
		key: Vec<u8>,
		cluster_level: u32,
		closer_peers: Vec<Peer>,
		provider_peers: Vec<Peer>,
	},
	AddProvider {
		key: Vec<u8>,
		cluster_level: u32,
	},
}

// Turns a type-safe kadmelia message into the corresponding row protobuf message.
fn msg_to_proto(kad_msg: KadMsg) -> protobuf_structs::dht::Message {
	match kad_msg {
		KadMsg::Ping => {
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::PING);
			msg
		}
		KadMsg::PutValue { key, record } => {
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::PUT_VALUE);
			msg.set_key(key);
			msg.set_record(record);
			msg
		}
		KadMsg::GetValueReq { key, cluster_level } => {
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::GET_VALUE);
			msg.set_key(key);
			msg.set_clusterLevelRaw(cluster_level as i32);
			msg
		}
		KadMsg::GetValueRes {
			key,
			cluster_level,
			record,
			closer_peers,
		} => unimplemented!(),
		KadMsg::FindNodeReq { key, cluster_level } => {
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::FIND_NODE);
			msg.set_key(key);
			msg.set_clusterLevelRaw(cluster_level as i32);
			msg
		}
		KadMsg::FindNodeRes {
			cluster_level,
			closer_peers,
		} => {
			assert!(!closer_peers.is_empty()); // TODO:
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::FIND_NODE);
			msg.set_clusterLevelRaw(cluster_level as i32);
			for peer in closer_peers {
				msg.mut_closerPeers().push(peer.into());
			}
			msg
		}
		KadMsg::GetProvidersReq { key, cluster_level } => {
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::GET_PROVIDERS);
			msg.set_key(key);
			msg.set_clusterLevelRaw(cluster_level as i32);
			msg
		}
		KadMsg::GetProvidersRes {
			key,
			cluster_level,
			closer_peers,
			provider_peers,
		} => unimplemented!(),
		KadMsg::AddProvider { key, cluster_level } => {
			let mut msg = protobuf_structs::dht::Message::new();
			msg.set_field_type(protobuf_structs::dht::Message_MessageType::ADD_PROVIDER);
			msg.set_key(key);
			msg.set_clusterLevelRaw(cluster_level as i32);
			msg
		}
	}
}

/// Turns a raw Kademlia message into a type-safe message.
fn proto_to_msg(mut message: protobuf_structs::dht::Message) -> KadMsg {
	match message.get_field_type() {
		protobuf_structs::dht::Message_MessageType::PING => KadMsg::Ping,
		protobuf_structs::dht::Message_MessageType::PUT_VALUE => {
			let key = message.take_key();
			let record = message.take_record();
			KadMsg::PutValue {
				key: key,
				record: record,
			}
		}
		protobuf_structs::dht::Message_MessageType::GET_VALUE => {
			let key = message.take_key();
			KadMsg::GetValueReq {
				key: key,
				cluster_level: message.get_clusterLevelRaw() as u32,
			}
		}
		protobuf_structs::dht::Message_MessageType::FIND_NODE => {
			if message.get_closerPeers().is_empty() {
				KadMsg::FindNodeReq {
					key: message.take_key(),
					cluster_level: message.get_clusterLevelRaw() as u32,
				}
			} else {
				KadMsg::FindNodeRes {
					closer_peers: message
						.mut_closerPeers()
						.iter_mut()
						.map(|peer| peer.into())
						.collect(),
					cluster_level: message.get_clusterLevelRaw() as u32,
				}
			}
		}
		_ => unimplemented!(),
	}
}

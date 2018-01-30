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

use bytes::{Bytes, BytesMut};
use futures::{future, Future, Stream, Sink};
use libp2p_swarm::{ConnectionUpgrade, Endpoint};
use multiaddr::Multiaddr;
use protobuf::Message as ProtobufMessage;
use protobuf::core::parse_from_bytes as protobuf_parse_from_bytes;
use protobuf::repeated::RepeatedField;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::iter;
use structs_proto;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;
use varint::VarintCodec;

/// Configuration for an upgrade to the identity protocol.
#[derive(Debug, Clone)]
pub struct IdentifyProtocolConfig;

/// Output of the connection upgrade.
pub enum IdentifyOutput<T> {
	/// We obtained information from the remote. Happens when we are the dialer.
	RemoteInfo {
		info: IdentifyInfo,
		/// Address the remote sees for us.
		observed_addr: Multiaddr,
	},

	/// We opened a connection to the remote and need to send it information. Happens when we are
	/// the listener.
	Sender {
		/// Object used to send identify info to the client.
		sender: IdentifySender<T>,
		/// Observed multiaddress of the client.
		observed_addr: Multiaddr,
	},
}

/// Object used to send back information to the client.
pub struct IdentifySender<T> {
	future: Framed<T, VarintCodec<Vec<u8>>>,
}

impl<'a, T> IdentifySender<T> where T: AsyncWrite + 'a {
	/// Sends back information to the client. Returns a future that is signalled whenever the
	/// info have been sent.
	pub fn send(self, info: IdentifyInfo, observed_addr: &Multiaddr)
				-> Box<Future<Item = (), Error = IoError> + 'a>
	{
		let listen_addrs = info.listen_addrs
							   .into_iter()
							   .map(|addr| addr.to_string().into_bytes())
							   .collect();

		let mut message = structs_proto::Identify::new();
		message.set_agentVersion(info.agent_version);
		message.set_protocolVersion(info.protocol_version);
		message.set_publicKey(info.public_key);
		message.set_listenAddrs(listen_addrs);
		message.set_observedAddr(observed_addr.to_string().into_bytes());
		message.set_protocols(RepeatedField::from_vec(info.protocols));

		let bytes = message.write_to_bytes()
			.expect("writing protobuf failed ; should never happen");

		let future = self.future
			.send(bytes)
			.map(|_| ());
		Box::new(future) as Box<_>
	}
}

/// Information sent from the listener to the dialer.
#[derive(Debug, Clone)]
pub struct IdentifyInfo {
	/// Public key of the node in the DER format.
	pub public_key: Vec<u8>,
	/// Version of the "global" protocol, eg. `ipfs/1.0.0` or `polkadot/1.0.0`.
	pub protocol_version: String,
	/// Name and version of the client. Can be thought as similar to the `User-Agent` header
	/// of HTTP.
	pub agent_version: String,
	/// Addresses that the remote is listening on.
	pub listen_addrs: Vec<Multiaddr>,
	/// Protocols supported by the remote.
	pub protocols: Vec<String>,
}

impl<C> ConnectionUpgrade<C> for IdentifyProtocolConfig
    where C: AsyncRead + AsyncWrite + 'static
{
	type NamesIter = iter::Once<(Bytes, Self::UpgradeIdentifier)>;
	type UpgradeIdentifier = ();
	type Output = IdentifyOutput<C>;
	type Future = Box<Future<Item = Self::Output, Error = IoError>>;

	#[inline]
	fn protocol_names(&self) -> Self::NamesIter {
		iter::once((Bytes::from("/ipfs/id/1.0.0"), ()))
	}

	fn upgrade(self, socket: C, _: (), ty: Endpoint, observed_addr: &Multiaddr) -> Self::Future {
		let socket = socket.framed(VarintCodec::default());

		match ty {
			Endpoint::Dialer => {
				let future = socket.into_future()
				      .map(|(msg, _)| msg)
					  .map_err(|(err, _)| err)
					  .and_then(|msg| if let Some(msg) = msg {
					let (info, observed_addr) = parse_proto_msg(msg)?;
					Ok(IdentifyOutput::RemoteInfo { info, observed_addr })
				} else {
					Err(IoErrorKind::InvalidData.into())
				});

				Box::new(future) as Box<_>
			}

			Endpoint::Listener => {
				let sender = IdentifySender {
					future: socket,
				};

				let future = future::ok(IdentifyOutput::Sender {
					sender,
					observed_addr: observed_addr.clone(),
				});

				Box::new(future) as Box<_>
			}
		}
	}
}

// Turns a protobuf message into an `IdentifyInfo` and an observed address. If something bad
// happens, turn it into an `IoError`.
fn parse_proto_msg(msg: BytesMut) -> Result<(IdentifyInfo, Multiaddr), IoError> {
	match protobuf_parse_from_bytes::<structs_proto::Identify>(&msg) {
		Ok(mut msg) => {
			let listen_addrs = {
				let mut addrs = Vec::new();
				for addr in msg.take_listenAddrs().into_iter() {
					addrs.push(bytes_to_multiaddr(addr)?);
				}
				addrs
			};

			let observed_addr = bytes_to_multiaddr(msg.take_observedAddr())?;

			let info = IdentifyInfo {
				public_key: msg.take_publicKey(),
				protocol_version: msg.take_protocolVersion(),
				agent_version: msg.take_agentVersion(),
				listen_addrs: listen_addrs,
				protocols: msg.take_protocols().into_vec(),
			};

			Ok((info, observed_addr))
		}

		Err(err) => {
			Err(IoError::new(IoErrorKind::InvalidData, err))
		}
	}
}

// Turn a `Vec<u8>` into a `Multiaddr`. If something bad happens, turn it into an `IoError`.
fn bytes_to_multiaddr(bytes: Vec<u8>) -> Result<Multiaddr, IoError> {
	Multiaddr::from_bytes(bytes)
		.map_err(|err| {
			IoError::new(IoErrorKind::InvalidData, err)
		})
}

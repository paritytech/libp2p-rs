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

use crate::structs_proto;
use futures::prelude::*;
use libp2p_core::{
    Multiaddr,
    PublicKey,
    upgrade::{self, InboundUpgrade, OutboundUpgrade, UpgradeInfo}
};
use log::{debug, trace};
use prost::Message;
use std::convert::TryFrom;
use std::{fmt, io, iter, pin::Pin};

/// Substream upgrade protocol for `/ipfs/id/1.0.0`.
#[derive(Debug, Clone)]
pub struct IdentifyProtocol;

/// Substream upgrade protocol for `/ipfs/id/push/1.0.0`.
#[derive(Debug, Clone)]
pub struct IdentifyPushProtocol<T>(T);
pub struct InboundPush();
pub struct OutboundPush(IdentifyInfo);

impl IdentifyPushProtocol<InboundPush> {
    pub fn inbound() -> Self {
        IdentifyPushProtocol(InboundPush())
    }
}

impl IdentifyPushProtocol<OutboundPush> {
    pub fn outbound(info: IdentifyInfo) -> Self {
        IdentifyPushProtocol(OutboundPush(info))
    }
}

/// Information of a peer sent in protocol messages.
#[derive(Debug, Clone)]
pub struct IdentifyInfo {
    /// The public key of the local peer.
    pub public_key: PublicKey,
    /// Application-specific version of the protocol family used by the peer,
    /// e.g. `ipfs/1.0.0` or `polkadot/1.0.0`.
    pub protocol_version: String,
    /// Name and version of the peer, similar to the `User-Agent` header in
    /// the HTTP protocol.
    pub agent_version: String,
    /// The addresses that the peer is listening on.
    pub listen_addrs: Vec<Multiaddr>,
    /// The list of protocols supported by the peer, e.g. `/ipfs/ping/1.0.0`.
    pub protocols: Vec<String>,
    /// Address observed by or for the remote.
    pub observed_addr: Multiaddr,
}

/// The substream on which a reply is expected to be sent.
pub struct ReplySubstream<T> {
    inner: T,
}

impl<T> fmt::Debug for ReplySubstream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ReplySubstream").finish()
    }
}

impl<T> ReplySubstream<T>
where
    T: AsyncWrite + Unpin
{
    /// Sends back the requested information on the substream.
    ///
    /// Consumes the substream, returning a future that resolves
    /// when the reply has been sent on the underlying connection.
    pub async fn send(self, info: IdentifyInfo) -> io::Result<()> {
        send(self.inner, info).await
    }
}

impl UpgradeInfo for IdentifyProtocol {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/id/1.0.0")
    }
}

impl<C> InboundUpgrade<C> for IdentifyProtocol {
    type Output = ReplySubstream<C>;
    type Error = io::Error;
    type Future = future::Ready<Result<Self::Output, io::Error>>;

    fn upgrade_inbound(self, socket: C, _: Self::Info) -> Self::Future {
        future::ok(ReplySubstream { inner: socket })
    }
}

impl<C> OutboundUpgrade<C> for IdentifyProtocol
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = IdentifyInfo;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        recv(socket).boxed()
    }
}

impl<T> UpgradeInfo for IdentifyPushProtocol<T> {
    type Info = &'static [u8];
    type InfoIter = iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        iter::once(b"/ipfs/id/push/1.0.0")
    }
}

impl<C> InboundUpgrade<C> for IdentifyPushProtocol<InboundPush>
where
    C: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = IdentifyInfo;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, socket: C, _: Self::Info) -> Self::Future {
        recv(socket).boxed()
    }
}

impl<C> OutboundUpgrade<C> for IdentifyPushProtocol<OutboundPush>
where
    C: AsyncWrite + Unpin + Send + 'static,
{
    type Output = ();
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, socket: C, _: Self::Info) -> Self::Future {
        send(socket, self.0.0).boxed()
    }
}

async fn send<T>(mut io: T, info: IdentifyInfo) -> io::Result<()>
where
    T: AsyncWrite + Unpin
{
    trace!("Sending: {:?}", info);

    let listen_addrs = info.listen_addrs
        .into_iter()
        .map(|addr| addr.to_vec())
        .collect();

    let pubkey_bytes = info.public_key.into_protobuf_encoding();

    let message = structs_proto::Identify {
        agent_version: Some(info.agent_version),
        protocol_version: Some(info.protocol_version),
        public_key: Some(pubkey_bytes),
        listen_addrs,
        observed_addr: Some(info.observed_addr.to_vec()),
        protocols: info.protocols
    };

    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes).expect("Vec<u8> provides capacity as needed");
    upgrade::write_one(&mut io, &bytes).await
}

async fn recv<T>(mut socket: T) -> io::Result<IdentifyInfo>
where
    T: AsyncRead + AsyncWrite + Unpin
{
    socket.close().await?;

    let msg = upgrade::read_one(&mut socket, 4096)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        .await?;

    let info = match parse_proto_msg(msg) {
        Ok(v) => v,
        Err(err) => {
            debug!("Invalid message: {:?}", err);
            return Err(err.into())
        }
    };

    trace!("Received: {:?}", info);

    Ok(info)
}

/// Turns a protobuf message into an `IdentifyInfo`.
fn parse_proto_msg(msg: impl AsRef<[u8]>) -> Result<IdentifyInfo, io::Error> {
    match structs_proto::Identify::decode(msg.as_ref()) {
        Ok(msg) => {
            fn parse_multiaddr(bytes: Vec<u8>) -> Result<Multiaddr, io::Error> {
                Multiaddr::try_from(bytes)
                    .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
            }

            let listen_addrs = {
                let mut addrs = Vec::new();
                for addr in msg.listen_addrs.into_iter() {
                    addrs.push(parse_multiaddr(addr)?);
                }
                addrs
            };

            let public_key = PublicKey::from_protobuf_encoding(&msg.public_key.unwrap_or_default())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let observed_addr = parse_multiaddr(msg.observed_addr.unwrap_or_default())?;
            let info = IdentifyInfo {
                public_key,
                protocol_version: msg.protocol_version.unwrap_or_default(),
                agent_version: msg.agent_version.unwrap_or_default(),
                listen_addrs,
                protocols: msg.protocols,
                observed_addr,
            };

            Ok(info)
        }

        Err(err) => Err(io::Error::new(io::ErrorKind::InvalidData, err)),
    }
}

#[cfg(test)]
mod tests {
    use libp2p_tcp::TcpConfig;
    use futures::{prelude::*, channel::oneshot};
    use libp2p_core::{
        identity,
        Transport,
        upgrade::{self, apply_outbound, apply_inbound}
    };
    use super::*;

    #[test]
    fn correct_transfer() {
        // We open a server and a client, send info from the server to the client, and check that
        // they were successfully received.
        let send_pubkey = identity::Keypair::generate_ed25519().public();
        let recv_pubkey = send_pubkey.clone();

        let (tx, rx) = oneshot::channel();

        let bg_task = async_std::task::spawn(async move {
            let transport = TcpConfig::new();

            let mut listener = transport
                .listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap())
                .unwrap();

            let addr = listener.next().await
                .expect("some event")
                .expect("no error")
                .into_new_address()
                .expect("listen address");
            tx.send(addr).unwrap();

            let socket = listener
                .next().await.unwrap().unwrap()
                .into_upgrade().unwrap()
                .0.await.unwrap();

            let sender = apply_inbound(socket, IdentifyProtocol).await.unwrap();

            sender.send(
                IdentifyInfo {
                    public_key: send_pubkey,
                    protocol_version: "proto_version".to_owned(),
                    agent_version: "agent_version".to_owned(),
                    listen_addrs: vec![
                        "/ip4/80.81.82.83/tcp/500".parse().unwrap(),
                        "/ip6/::1/udp/1000".parse().unwrap(),
                    ],
                    protocols: vec!["proto1".to_string(), "proto2".to_string()],
                    observed_addr: "/ip4/100.101.102.103/tcp/5000".parse().unwrap(),
                },
            ).await.unwrap();
        });

        async_std::task::block_on(async move {
            let transport = TcpConfig::new();

            let socket = transport.dial(rx.await.unwrap()).unwrap().await.unwrap();
            let info = apply_outbound(
                socket,
                IdentifyProtocol,
                upgrade::Version::V1
            ).await.unwrap();
            assert_eq!(info.observed_addr, "/ip4/100.101.102.103/tcp/5000".parse().unwrap());
            assert_eq!(info.public_key, recv_pubkey);
            assert_eq!(info.protocol_version, "proto_version");
            assert_eq!(info.agent_version, "agent_version");
            assert_eq!(info.listen_addrs,
                &["/ip4/80.81.82.83/tcp/500".parse().unwrap(),
                "/ip6/::1/udp/1000".parse().unwrap()]);
            assert_eq!(info.protocols, &["proto1".to_string(), "proto2".to_string()]);

            bg_task.await;
        });
    }
}

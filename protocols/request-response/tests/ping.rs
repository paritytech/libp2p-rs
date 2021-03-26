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

//! Integration tests for the `RequestResponse` network behaviour.

use async_trait::async_trait;
use libp2p_core::{
    Multiaddr,
    PeerId,
    identity,
    muxing::StreamMuxerBox,
    transport::{self, Transport},
    upgrade::{self, read_one, write_one}
};
use libp2p_noise::{NoiseConfig, X25519Spec, Keypair};
use libp2p_request_response::*;
use libp2p_swarm::{Swarm, SwarmEvent};
use libp2p_tcp::TcpConfig;
use futures::{prelude::*, channel::mpsc, executor::LocalPool, task::SpawnExt};
use rand::{self, Rng};
use std::{io, iter};
use std::{collections::HashSet, num::NonZeroU16};

#[test]
fn is_response_outbound() {
    let ping = Ping("ping".to_string().into_bytes());
    let offline_peer = PeerId::random();

    let protocols = iter::once((PingProtocol(), ProtocolSupport::Full));
    let cfg = RequestResponseConfig::default();

    let (peer1_id, trans) = mk_transport();
    let ping_proto1 = RequestResponse::new(PingCodec(), protocols, cfg);
    let mut swarm1 = Swarm::new(trans, ping_proto1, peer1_id);

    let request_id1 = swarm1.behaviour_mut().send_request(&offline_peer, ping.clone());

    match futures::executor::block_on(swarm1.next()) {
        RequestResponseEvent::OutboundFailure{peer, request_id: req_id, error: _error} => {
            assert_eq!(&offline_peer, &peer);
            assert_eq!(req_id, request_id1);
        },
        e => panic!("Peer: Unexpected event: {:?}", e),
    }

    let request_id2 = swarm1.behaviour_mut().send_request(&offline_peer, ping);

    assert!(!swarm1.behaviour().is_pending_outbound(&offline_peer, &request_id1));
    assert!(swarm1.behaviour().is_pending_outbound(&offline_peer, &request_id2));
}

/// Exercises a simple ping protocol.
#[test]
fn ping_protocol() {
    let ping = Ping("ping".to_string().into_bytes());
    let pong = Pong("pong".to_string().into_bytes());

    let protocols = iter::once((PingProtocol(), ProtocolSupport::Full));
    let cfg = RequestResponseConfig::default();

    let (peer1_id, trans) = mk_transport();
    let ping_proto1 = RequestResponse::new(PingCodec(), protocols.clone(), cfg.clone());
    let mut swarm1 = Swarm::new(trans, ping_proto1, peer1_id);

    let (peer2_id, trans) = mk_transport();
    let ping_proto2 = RequestResponse::new(PingCodec(), protocols, cfg);
    let mut swarm2 = Swarm::new(trans, ping_proto2, peer2_id);

    let (mut tx, mut rx) = mpsc::channel::<Multiaddr>(1);

    let addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    swarm1.listen_on(addr).unwrap();

    let expected_ping = ping.clone();
    let expected_pong = pong.clone();

    let peer1 = async move {
        loop {
            match swarm1.next_event().await {
                SwarmEvent::NewListenAddr(addr) => tx.send(addr).await.unwrap(),
                SwarmEvent::Behaviour(RequestResponseEvent::Message {
                    peer,
                    message: RequestResponseMessage::Request { request, channel, .. }
                }) => {
                    assert_eq!(&request, &expected_ping);
                    assert_eq!(&peer, &peer2_id);
                    swarm1.behaviour_mut().send_response(channel, pong.clone()).unwrap();
                },
                SwarmEvent::Behaviour(RequestResponseEvent::ResponseSent {
                    peer, ..
                }) => {
                    assert_eq!(&peer, &peer2_id);
                }
                SwarmEvent::Behaviour(e) => panic!("Peer1: Unexpected event: {:?}", e),
                _ => {}
            }
        }
    };

    let num_pings: u8 = rand::thread_rng().gen_range(1, 100);

    let peer2 = async move {
        let mut count = 0;
        let addr = rx.next().await.unwrap();
        swarm2.behaviour_mut().add_address(&peer1_id, addr.clone());
        let mut req_id = swarm2.behaviour_mut().send_request(&peer1_id, ping.clone());
        assert!(swarm2.behaviour().is_pending_outbound(&peer1_id, &req_id));

        loop {
            match swarm2.next().await {
                RequestResponseEvent::Message {
                    peer,
                    message: RequestResponseMessage::Response { request_id, response }
                } => {
                    count += 1;
                    assert_eq!(&response, &expected_pong);
                    assert_eq!(&peer, &peer1_id);
                    assert_eq!(req_id, request_id);
                    if count >= num_pings {
                        return
                    } else {
                        req_id = swarm2.behaviour_mut().send_request(&peer1_id, ping.clone());
                    }
                },
                e => panic!("Peer2: Unexpected event: {:?}", e)
            }
        }
    };

    async_std::task::spawn(Box::pin(peer1));
    let () = async_std::task::block_on(peer2);
}

#[test]
fn emits_inbound_connection_closed_failure() {
    let ping = Ping("ping".to_string().into_bytes());

    let protocols = iter::once((PingProtocol(), ProtocolSupport::Full));
    let cfg = RequestResponseConfig::default();

    let (peer1_id, trans) = mk_transport();
    let ping_proto1 = RequestResponse::new(PingCodec(), protocols.clone(), cfg.clone());
    let mut swarm1 = Swarm::new(trans, ping_proto1, peer1_id);

    let (peer2_id, trans) = mk_transport();
    let ping_proto2 = RequestResponse::new(PingCodec(), protocols, cfg);
    let mut swarm2 = Swarm::new(trans, ping_proto2, peer2_id);

    let addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    swarm1.listen_on(addr).unwrap();

    futures::executor::block_on(async move {
        while swarm1.next().now_or_never().is_some() {}
        let addr1 = Swarm::listeners(&swarm1).next().unwrap();

        swarm2.behaviour_mut().add_address(&peer1_id, addr1.clone());
        swarm2.behaviour_mut().send_request(&peer1_id, ping.clone());

        // Wait for swarm 1 to receive request by swarm 2.
        let _channel = loop {
            futures::select!(
                event = swarm1.next().fuse() => match event {
                    RequestResponseEvent::Message {
                        peer,
                        message: RequestResponseMessage::Request { request, channel, .. }
                    } => {
                        assert_eq!(&request, &ping);
                        assert_eq!(&peer, &peer2_id);
                        break channel;
                    },
                    e => panic!("Peer1: Unexpected event: {:?}", e)
                },
                event = swarm2.next().fuse() => panic!("Peer2: Unexpected event: {:?}", event),
            )
        };

        // Drop swarm 2 in order for the connection between swarm 1 and 2 to close.
        drop(swarm2);

        match swarm1.next().await {
            RequestResponseEvent::InboundFailure { error: InboundFailure::ConnectionClosed, ..} => {},
            e => panic!("Peer1: Unexpected event: {:?}", e)
        }
    });
}

/// We expect the substream to be properly closed when response channel is dropped.
/// Since the ping protocol used here expects a response, the sender considers this
/// early close as a protocol violation which results in the connection being closed.
/// If the substream were not properly closed when dropped, the sender would instead
/// run into a timeout waiting for the response.
#[test]
fn emits_inbound_connection_closed_if_channel_is_dropped() {
    let ping = Ping("ping".to_string().into_bytes());

    let protocols = iter::once((PingProtocol(), ProtocolSupport::Full));
    let cfg = RequestResponseConfig::default();

    let (peer1_id, trans) = mk_transport();
    let ping_proto1 = RequestResponse::new(PingCodec(), protocols.clone(), cfg.clone());
    let mut swarm1 = Swarm::new(trans, ping_proto1, peer1_id);

    let (peer2_id, trans) = mk_transport();
    let ping_proto2 = RequestResponse::new(PingCodec(), protocols, cfg);
    let mut swarm2 = Swarm::new(trans, ping_proto2, peer2_id);

    let addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    swarm1.listen_on(addr).unwrap();

    futures::executor::block_on(async move {
        while swarm1.next().now_or_never().is_some() {}
        let addr1 = Swarm::listeners(&swarm1).next().unwrap();

        swarm2.behaviour_mut().add_address(&peer1_id, addr1.clone());
        swarm2.behaviour_mut().send_request(&peer1_id, ping.clone());

        // Wait for swarm 1 to receive request by swarm 2.
        let event = loop {
            futures::select!(
                event = swarm1.next().fuse() => if let RequestResponseEvent::Message {
                     peer,
                     message: RequestResponseMessage::Request { request, channel, .. }
                 } = event {
                     assert_eq!(&request, &ping);
                     assert_eq!(&peer, &peer2_id);

                    drop(channel);
                    continue;
                },
                event = swarm2.next().fuse() => {
                    break event;
                },
            )
        };

        let error = match event {
            RequestResponseEvent::OutboundFailure { error, .. } => error,
            e => panic!("unexpected event from peer 2: {:?}", e)
        };

        assert_eq!(error, OutboundFailure::ConnectionClosed);
    });
}

#[test]
fn ping_protocol_throttled() {
    let ping = Ping("ping".to_string().into_bytes());
    let pong = Pong("pong".to_string().into_bytes());

    let protocols = iter::once((PingProtocol(), ProtocolSupport::Full));
    let cfg = RequestResponseConfig::default();

    let (peer1_id, trans) = mk_transport();
    let ping_proto1 = RequestResponse::throttled(PingCodec(), protocols.clone(), cfg.clone());
    let mut swarm1 = Swarm::new(trans, ping_proto1, peer1_id);

    let (peer2_id, trans) = mk_transport();
    let ping_proto2 = RequestResponse::throttled(PingCodec(), protocols, cfg);
    let mut swarm2 = Swarm::new(trans, ping_proto2, peer2_id);

    let (mut tx, mut rx) = mpsc::channel::<Multiaddr>(1);

    let addr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
    swarm1.listen_on(addr).unwrap();

    let expected_ping = ping.clone();
    let expected_pong = pong.clone();

    let limit1: u16 = rand::thread_rng().gen_range(1, 10);
    let limit2: u16 = rand::thread_rng().gen_range(1, 10);
    swarm1.behaviour_mut().set_receive_limit(NonZeroU16::new(limit1).unwrap());
    swarm2.behaviour_mut().set_receive_limit(NonZeroU16::new(limit2).unwrap());

    let peer1 = async move {
        for i in 1 .. {
            match swarm1.next_event().await {
                SwarmEvent::NewListenAddr(addr) => tx.send(addr).await.unwrap(),
                SwarmEvent::Behaviour(throttled::Event::Event(RequestResponseEvent::Message {
                    peer,
                    message: RequestResponseMessage::Request { request, channel, .. },
                })) => {
                    assert_eq!(&request, &expected_ping);
                    assert_eq!(&peer, &peer2_id);
                    swarm1.behaviour_mut().send_response(channel, pong.clone()).unwrap();
                },
                SwarmEvent::Behaviour(throttled::Event::Event(RequestResponseEvent::ResponseSent {
                    peer, ..
                })) => {
                    assert_eq!(&peer, &peer2_id);
                }
                SwarmEvent::Behaviour(e) => panic!("Peer1: Unexpected event: {:?}", e),
                _ => {}
            }
            if i % 31 == 0 {
                let lim = rand::thread_rng().gen_range(1, 17);
                swarm1.behaviour_mut().override_receive_limit(&peer2_id, NonZeroU16::new(lim).unwrap());
            }
        }
    };

    let num_pings: u16 = rand::thread_rng().gen_range(100, 1000);

    let peer2 = async move {
        let mut count = 0;
        let addr = rx.next().await.unwrap();
        swarm2.behaviour_mut().add_address(&peer1_id, addr.clone());

        let mut blocked = false;
        let mut req_ids = HashSet::new();

        loop {
            if !blocked {
                while let Some(id) = swarm2.behaviour_mut().send_request(&peer1_id, ping.clone()).ok() {
                    req_ids.insert(id);
                }
                blocked = true;
            }
            match swarm2.next().await {
                throttled::Event::ResumeSending(peer) => {
                    assert_eq!(peer, peer1_id);
                    blocked = false
                }
                throttled::Event::Event(RequestResponseEvent::Message {
                    peer,
                    message: RequestResponseMessage::Response { request_id, response }
                }) => {
                    count += 1;
                    assert_eq!(&response, &expected_pong);
                    assert_eq!(&peer, &peer1_id);
                    assert!(req_ids.remove(&request_id));
                    if count >= num_pings {
                        break
                    }
                }
                e => panic!("Peer2: Unexpected event: {:?}", e)

            }
        }
    };

    let mut pool = LocalPool::new();
    pool.spawner().spawn(peer1.boxed()).unwrap();
    pool.run_until(peer2);
}

fn mk_transport() -> (PeerId, transport::Boxed<(PeerId, StreamMuxerBox)>) {
    let id_keys = identity::Keypair::generate_ed25519();
    let peer_id = id_keys.public().into_peer_id();
    let noise_keys = Keypair::<X25519Spec>::new().into_authentic(&id_keys).unwrap();
    (peer_id, TcpConfig::new()
        .nodelay(true)
        .upgrade(upgrade::Version::V1)
        .authenticate(NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(libp2p_yamux::YamuxConfig::default())
        .boxed())
}

// Simple Ping-Pong Protocol

#[derive(Debug, Clone)]
struct PingProtocol();
#[derive(Clone)]
struct PingCodec();
#[derive(Debug, Clone, PartialEq, Eq)]
struct Ping(Vec<u8>);
#[derive(Debug, Clone, PartialEq, Eq)]
struct Pong(Vec<u8>);

impl ProtocolName for PingProtocol {
    fn protocol_name(&self) -> &[u8] {
        "/ping/1".as_bytes()
    }
}

#[async_trait]
impl RequestResponseCodec for PingCodec {
    type Protocol = PingProtocol;
    type Request = Ping;
    type Response = Pong;

    async fn read_request<T>(&mut self, _: &PingProtocol, io: &mut T)
        -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send
    {
        read_one(io, 1024)
            .map(|res| match res {
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
                Ok(vec) if vec.is_empty() => Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(vec) => Ok(Ping(vec))
            })
            .await
    }

    async fn read_response<T>(&mut self, _: &PingProtocol, io: &mut T)
        -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send
    {
        read_one(io, 1024)
            .map(|res| match res {
                Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
                Ok(vec) if vec.is_empty() => Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(vec) => Ok(Pong(vec))
            })
            .await
    }

    async fn write_request<T>(&mut self, _: &PingProtocol, io: &mut T, Ping(data): Ping)
        -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send
    {
        write_one(io, data).await
    }

    async fn write_response<T>(&mut self, _: &PingProtocol, io: &mut T, Pong(data): Pong)
        -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send
    {
        write_one(io, data).await
    }
}

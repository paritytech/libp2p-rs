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

mod util;

use futures::prelude::*;
use libp2p_core::multiaddr::multiaddr;
use libp2p_core::{
    PeerId,
    connection::PendingConnectionError,
    multiaddr::Protocol,
    network::{NetworkEvent, NetworkConfig},
};
use rand::seq::SliceRandom;
use std::{io, task::Poll};
use util::{TestHandler, test_network};

#[test]
fn deny_incoming_connec() {
    // Checks whether refusing an incoming connection on a swarm triggers the correct events.

    let mut swarm1 = test_network(NetworkConfig::default());
    let mut swarm2 = test_network(NetworkConfig::default());

    swarm1.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

    let address = async_std::task::block_on(future::poll_fn(|cx| {
        match swarm1.poll(cx) {
            Poll::Ready(NetworkEvent::NewListenerAddress { listen_addr, .. }) => {
                Poll::Ready(listen_addr)
            }
            Poll::Pending => Poll::Pending,
            _ => panic!("Was expecting the listen address to be reported"),
        }
    }));

    swarm2
        .peer(swarm1.local_peer_id().clone())
        .dial(address.clone(), Vec::new(), TestHandler())
        .unwrap();

    async_std::task::block_on(future::poll_fn(|cx| -> Poll<Result<(), io::Error>> {
        match swarm1.poll(cx) {
            Poll::Ready(NetworkEvent::IncomingConnection { connection, .. }) => drop(connection),
            Poll::Ready(_) => unreachable!(),
            Poll::Pending => (),
        }

        match swarm2.poll(cx) {
            Poll::Ready(NetworkEvent::DialError {
                attempts_remaining: 0,
                peer_id,
                multiaddr,
                error: PendingConnectionError::Transport(_)
            }) => {
                assert_eq!(&peer_id, swarm1.local_peer_id());
                assert_eq!(multiaddr, address.clone().with(Protocol::P2p(peer_id.into())));
                return Poll::Ready(Ok(()));
            },
            Poll::Ready(_) => unreachable!(),
            Poll::Pending => (),
        }

        Poll::Pending
    })).unwrap();
}

#[test]
fn dial_self() {

    // Check whether dialing ourselves correctly fails.
    //
    // Dialing the same address we're listening should result in three events:
    //
    // - The incoming connection notification (before we know the incoming peer ID).
    // - The connection error for the dialing endpoint (once we've determined that it's our own ID).
    // - The connection error for the listening endpoint (once we've determined that it's our own ID).
    //
    // The last two can happen in any order.

    let mut swarm = test_network(NetworkConfig::default());
    swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

    let local_address = async_std::task::block_on(future::poll_fn(|cx| {
        match swarm.poll(cx) {
            Poll::Ready(NetworkEvent::NewListenerAddress { listen_addr, .. }) => {
                Poll::Ready(listen_addr)
            }
            Poll::Pending => Poll::Pending,
            _ => panic!("Was expecting the listen address to be reported"),
        }
    }));

    swarm.dial(&local_address, TestHandler()).unwrap();

    let mut got_dial_err = false;
    let mut got_inc_err = false;
    async_std::task::block_on(future::poll_fn(|cx| -> Poll<Result<(), io::Error>> {
        loop {
            match swarm.poll(cx) {
                Poll::Ready(NetworkEvent::UnknownPeerDialError {
                    multiaddr,
                    error: PendingConnectionError::InvalidPeerId { .. },
                    ..
                }) => {
                    assert!(!got_dial_err);
                    assert_eq!(multiaddr, local_address);
                    got_dial_err = true;
                    if got_inc_err {
                        return Poll::Ready(Ok(()))
                    }
                },
                Poll::Ready(NetworkEvent::IncomingConnectionError {
                    local_addr, ..
                }) => {
                    assert!(!got_inc_err);
                    assert_eq!(local_addr, local_address);
                    got_inc_err = true;
                    if got_dial_err {
                       return Poll::Ready(Ok(()))
                    }
                },
                Poll::Ready(NetworkEvent::IncomingConnection { connection, .. }) => {
                    assert_eq!(&connection.local_addr, &local_address);
                    swarm.accept(connection, TestHandler()).unwrap();
                },
                Poll::Ready(ev) => {
                    panic!("Unexpected event: {:?}", ev)
                }
                Poll::Pending => break Poll::Pending,
            }
        }
    })).unwrap();
}

#[test]
fn dial_self_by_id() {
    // Trying to dial self by passing the same `PeerId` shouldn't even be possible in the first
    // place.
    let mut swarm = test_network(NetworkConfig::default());
    let peer_id = swarm.local_peer_id().clone();
    assert!(swarm.peer(peer_id).into_disconnected().is_none());
}

#[test]
fn multiple_addresses_err() {
    // Tries dialing multiple addresses, and makes sure there's one dialing error per address.

    let target = PeerId::random();

    let mut swarm = test_network(NetworkConfig::default());

    let mut addresses = Vec::new();
    for _ in 0 .. 3 {
        addresses.push(multiaddr![
            Ip4([0, 0, 0, 0]),
            Tcp(rand::random::<u16>())
        ]);
    }
    for _ in 0 .. 5 {
        addresses.push(multiaddr![
            Udp(rand::random::<u16>())
        ]);
    }
    addresses.shuffle(&mut rand::thread_rng());

    let first = addresses[0].clone();
    let rest = (&addresses[1..]).iter().cloned();

    swarm.peer(target.clone())
        .dial(first, rest, TestHandler())
        .unwrap();

    async_std::task::block_on(future::poll_fn(|cx| -> Poll<Result<(), io::Error>> {
        loop {
            match swarm.poll(cx) {
                Poll::Ready(NetworkEvent::DialError {
                    attempts_remaining,
                    peer_id,
                    multiaddr,
                    error: PendingConnectionError::Transport(_)
                }) => {
                    assert_eq!(peer_id, target);
                    let expected = addresses.remove(0).with(Protocol::P2p(target.clone().into()));
                    assert_eq!(multiaddr, expected);
                    if addresses.is_empty() {
                        assert_eq!(attempts_remaining, 0);
                        return Poll::Ready(Ok(()));
                    } else {
                        assert_eq!(attempts_remaining, addresses.len() as u32);
                    }
                },
                Poll::Ready(_) => unreachable!(),
                Poll::Pending => break Poll::Pending,
            }
        }
    })).unwrap();
}

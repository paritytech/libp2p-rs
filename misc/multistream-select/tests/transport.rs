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

use libp2p_core::{
    connection::{ConnectionHandler, ConnectionHandlerEvent, Substream, SubstreamEndpoint},
    identity,
    muxing::StreamMuxerBox,
    upgrade,
    multiaddr::Protocol,
    Multiaddr,
    Network,
    network::{NetworkEvent, NetworkConfig},
    PeerId,
    Transport,
    transport::{self, MemoryTransport}
};
use libp2p_mplex::MplexConfig;
use libp2p_plaintext::PlainText2Config;
use futures::{channel::oneshot, ready, prelude::*};
use rand::random;
use std::{io, task::{Context, Poll}};

type TestTransport = transport::Boxed<(PeerId, StreamMuxerBox)>;
type TestNetwork = Network<TestTransport, (), (), TestHandler>;

// TODO: Fix _up
fn mk_transport(_up: upgrade::Version) -> (PeerId, TestTransport) {
    let keys = identity::Keypair::generate_ed25519();
    let id = keys.public().into_peer_id();
    (id, MemoryTransport::default()
        .upgrade()
        .authenticate(PlainText2Config { local_public_key: keys.public() })
        .multiplex(MplexConfig::default())
        .boxed())
}

/// Tests the transport upgrade process with all supported
/// upgrade protocol versions.
#[test]
fn transport_upgrade() {
    let _ = env_logger::try_init();

    fn run(up: upgrade::Version) {
        let (dialer_id, dialer_transport) = mk_transport(up);
        let (listener_id, listener_transport) = mk_transport(up);

        let listen_addr = Multiaddr::from(Protocol::Memory(random::<u64>()));

        let mut dialer = TestNetwork::new(dialer_transport, dialer_id, NetworkConfig::default());
        let mut listener = TestNetwork::new(listener_transport, listener_id, NetworkConfig::default());

        listener.listen_on(listen_addr).unwrap();
        let (addr_sender, addr_receiver) = oneshot::channel();

        let client = async move {
            let addr = addr_receiver.await.unwrap();
            dialer.dial(&addr, TestHandler()).unwrap();
            futures::future::poll_fn(move |cx| {
                loop {
                    match ready!(dialer.poll(cx)) {
                        NetworkEvent::ConnectionEstablished { .. } => {
                            return Poll::Ready(())
                        }
                        _ => {}
                    }
                }
            }).await
        };

        let mut addr_sender = Some(addr_sender);
        let server = futures::future::poll_fn(move |cx| {
            loop {
                match ready!(listener.poll(cx)) {
                    NetworkEvent::NewListenerAddress { listen_addr, .. } => {
                        addr_sender.take().unwrap().send(listen_addr).unwrap();
                    }
                    NetworkEvent::IncomingConnection { connection, .. } => {
                        listener.accept(connection, TestHandler()).unwrap();
                    }
                    NetworkEvent::ConnectionEstablished { .. } => {
                        return Poll::Ready(())
                    }
                    _ => {}
                }
            }
        });

        async_std::task::block_on(future::select(Box::pin(server), Box::pin(client)));
    }

    run(upgrade::Version::V1);
    run(upgrade::Version::V1Lazy);
}

struct TestHandler();

impl ConnectionHandler for TestHandler {
    type InEvent = ();
    type OutEvent = ();
    type Error = io::Error;
    type Substream = Substream<StreamMuxerBox>;
    type OutboundOpenInfo = ();

    fn inject_substream(&mut self, _: Self::Substream, _: SubstreamEndpoint<Self::OutboundOpenInfo>)
    {}

    fn inject_event(&mut self, _: Self::InEvent)
    {}

    fn inject_address_change(&mut self, _: &Multiaddr)
    {}

    fn poll(&mut self, _: &mut Context<'_>)
        -> Poll<Result<ConnectionHandlerEvent<Self::OutboundOpenInfo, Self::OutEvent>, Self::Error>>
    {
        Poll::Pending
    }
}

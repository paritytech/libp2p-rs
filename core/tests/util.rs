
#![allow(dead_code)]

use futures::prelude::*;
use libp2p_core::{
    Multiaddr,
    PeerId,
    Transport,
    connection::{
        ConnectionHandler,
        ConnectionHandlerEvent,
        Substream,
        SubstreamEndpoint,
    },
    identity,
    muxing::{StreamMuxer, StreamMuxerBox},
    network::{Network, NetworkConfig},
    transport,
};
use libp2p_mplex as mplex;
use libp2p_noise as noise;
use libp2p_tcp as tcp;
use std::{io, pin::Pin, task::Context, task::Poll};

type TestNetwork = Network<TestTransport, (), (), TestHandler>;
type TestTransport = transport::Boxed<(PeerId, StreamMuxerBox)>;

/// Creates a new `TestNetwork` with a TCP transport.
pub fn test_network(cfg: NetworkConfig) -> TestNetwork {
    let local_key = identity::Keypair::generate_ed25519();
    let local_public_key = local_key.public();
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new().into_authentic(&local_key).unwrap();
    let transport: TestTransport = tcp::TcpConfig::new()
        .upgrade()
        .authenticate(noise::NoiseConfig::xx(noise_keys).into_authenticated())
        .multiplex(mplex::MplexConfig::new())
        .boxed();

    TestNetwork::new(transport, local_public_key.into(), cfg)
}

pub struct TestHandler();

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

pub struct CloseMuxer<M> {
    state: CloseMuxerState<M>,
}

impl<M> CloseMuxer<M> {
    pub fn new(m: M) -> CloseMuxer<M> {
        CloseMuxer {
            state: CloseMuxerState::Close(m)
        }
    }
}

pub enum CloseMuxerState<M> {
    Close(M),
    Done,
}

impl<M> Future for CloseMuxer<M>
where
    M: StreamMuxer,
    M::Error: From<std::io::Error>
{
    type Output = Result<M, M::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match std::mem::replace(&mut self.state, CloseMuxerState::Done) {
                CloseMuxerState::Close(muxer) => {
                    if !muxer.close(cx)?.is_ready() {
                        self.state = CloseMuxerState::Close(muxer);
                        return Poll::Pending
                    }
                    return Poll::Ready(Ok(muxer))
                }
                CloseMuxerState::Done => panic!()
            }
        }
    }
}

impl<M> Unpin for CloseMuxer<M> {
}

// Copyright 2021 COMIT Network.
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

pub mod harness;

use crate::harness::{await_events_or_timeout, new_swarm, SwarmExt};
use futures::StreamExt;
use libp2p::rendezvous::{Namespace, Ttl};
use libp2p_core::identity;
use libp2p_core::PeerId;
use libp2p_rendezvous::{
    Config, ErrorCode, Event, RegisterError, Registration, Rendezvous, DEFAULT_TTL,
};
use libp2p_swarm::{Swarm, SwarmEvent};

#[tokio::test]
async fn given_successful_registration_then_successful_discovery() {
    let _ = env_logger::try_init();
    let mut test = RendezvousTest::setup().await;

    let namespace = Namespace::from_static("some-namespace");

    let _ =
        test.alice
            .behaviour_mut()
            .register(namespace.clone(), *test.robert.local_peer_id(), None);

    test.assert_successful_registration(namespace.clone(), DEFAULT_TTL)
        .await;

    test.bob.behaviour_mut().discover(
        Some(namespace.clone()),
        None,
        None,
        *test.robert.local_peer_id(),
    );

    test.assert_successful_discovery(namespace.clone(), DEFAULT_TTL, *test.alice.local_peer_id())
        .await;
}

#[tokio::test]
async fn given_successful_registration_then_refresh_ttl() {
    let _ = env_logger::try_init();
    let mut test = RendezvousTest::setup().await;

    let namespace = Namespace::from_static("some-namespace");

    let refesh_ttl = 10_000;

    let _ =
        test.alice
            .behaviour_mut()
            .register(namespace.clone(), *test.robert.local_peer_id(), None);

    test.assert_successful_registration(namespace.clone(), DEFAULT_TTL)
        .await;

    test.bob.behaviour_mut().discover(
        Some(namespace.clone()),
        None,
        None,
        *test.robert.local_peer_id(),
    );

    test.assert_successful_discovery(namespace.clone(), DEFAULT_TTL, *test.alice.local_peer_id())
        .await;

    test.alice.behaviour_mut().register(
        namespace.clone(),
        *test.robert.local_peer_id(),
        Some(refesh_ttl),
    );

    test.assert_successful_registration(namespace.clone(), refesh_ttl)
        .await;

    test.bob.behaviour_mut().discover(
        Some(namespace.clone()),
        None,
        None,
        *test.robert.local_peer_id(),
    );

    test.assert_successful_discovery(namespace.clone(), refesh_ttl, *test.alice.local_peer_id())
        .await;
}

#[tokio::test]
async fn given_invalid_ttl_then_unsuccessful_registration() {
    let _ = env_logger::try_init();
    let mut test = RendezvousTest::setup().await;

    let namespace = Namespace::from_static("some-namespace");

    test.alice.behaviour_mut().register(
        namespace.clone(),
        *test.robert.local_peer_id(),
        Some(100_000_000),
    );

    match await_events_or_timeout(&mut test.robert, &mut test.alice).await {
        (
            SwarmEvent::Behaviour(Event::PeerNotRegistered { .. }),
            SwarmEvent::Behaviour(Event::RegisterFailed(RegisterError::Remote {error , ..})),
        ) => {
            assert_eq!(error, ErrorCode::InvalidTtl);
        }
        (rendezvous_swarm_event, registration_swarm_event) => panic!(
            "Received unexpected event, rendezvous swarm emitted {:?} and registration swarm emitted {:?}",
            rendezvous_swarm_event, registration_swarm_event
        ),
    }
}

#[tokio::test]
async fn discover_allows_for_dial_by_peer_id() {
    let _ = env_logger::try_init();
    let RendezvousTest {
        mut alice,
        mut bob,
        mut robert,
        ..
    } = RendezvousTest::setup().await;
    let roberts_peer_id = *robert.local_peer_id();

    // poll rendezvous point continuously
    tokio::spawn(async move {
        loop {
            robert.next().await;
        }
    });

    let namespace = Namespace::from_static("some-namespace");

    alice
        .behaviour_mut()
        .register(namespace.clone(), roberts_peer_id, None);
    bob.behaviour_mut()
        .discover(Some(namespace), None, None, roberts_peer_id);

    match await_events_or_timeout(&mut alice, &mut bob).await {
        (
            SwarmEvent::Behaviour(Event::Registered { .. }),
            SwarmEvent::Behaviour(Event::Discovered { .. }),
        ) => {}
        _ => panic!("bad event combination emitted"),
    };

    let alices_peer_id = *alice.local_peer_id();
    let bobs_peer_id = *bob.local_peer_id();

    bob.dial(&alices_peer_id).unwrap();

    let alice_connected_to = tokio::spawn(async move {
        loop {
            if let SwarmEvent::ConnectionEstablished { peer_id, .. } =
                alice.select_next_some().await
            {
                break peer_id;
            }
        }
    });
    let bob_connected_to = tokio::spawn(async move {
        loop {
            if let SwarmEvent::ConnectionEstablished { peer_id, .. } = bob.select_next_some().await
            {
                break peer_id;
            }
        }
    });

    assert_eq!(alice_connected_to.await.unwrap(), bobs_peer_id);
    assert_eq!(bob_connected_to.await.unwrap(), alices_peer_id);
}

#[tokio::test]
async fn eve_cannot_register() {
    let _ = env_logger::try_init();
    let mut test = RendezvousTest::setup().await;

    let namespace = Namespace::from_static("some-namespace");

    test.eve.behaviour_mut().register(
        namespace.clone(),
        *test.robert.local_peer_id(),
        Some(100_000),
    );

    match await_events_or_timeout(&mut test.robert, &mut test.eve).await {
        (
            SwarmEvent::Behaviour(Event::PeerNotRegistered { .. }),
            SwarmEvent::Behaviour(Event::RegisterFailed(RegisterError::Remote { error: err_code , ..})),
        ) => {
            assert_eq!(err_code, ErrorCode::NotAuthorized);
        }
        (rendezvous_swarm_event, registration_swarm_event) => panic!(
            "Received unexpected event, rendezvous swarm emitted {:?} and registration swarm emitted {:?}",
            rendezvous_swarm_event, registration_swarm_event
        ),
    }
}

/// Holds a network of nodes that is used to test certain rendezvous functionality.
///
/// In all cases, Alice would like to connect to Bob with Robert acting as a rendezvous point.
/// Eve is an evil actor that tries to act maliciously.
struct RendezvousTest {
    pub alice: Swarm<Rendezvous>,
    pub bob: Swarm<Rendezvous>,
    pub eve: Swarm<Rendezvous>,
    pub robert: Swarm<Rendezvous>,
}

impl RendezvousTest {
    pub async fn setup() -> Self {
        let mut alice = new_swarm(|_, identity| Rendezvous::new(identity, Config::default()));
        alice.listen_on_random_memory_address().await;

        let mut bob = new_swarm(|_, identity| Rendezvous::new(identity, Config::default()));
        bob.listen_on_random_memory_address().await;

        let mut robert = new_swarm(|_, identity| Rendezvous::new(identity, Config::default()));
        robert.listen_on_random_memory_address().await;

        let mut eve = {
            // In reality, if Eve were to try and fake someones identity, she would obviously only know the public key.
            // Due to the type-safe API of the `Rendezvous` behaviour and `PeerRecord`, we actually cannot construct a bad `PeerRecord` (i.e. one that is claims to be someone else).
            // As such, the best we can do is hand eve a completely different keypair from what she is using to authenticate her connection.
            let someone_else = identity::Keypair::generate_ed25519();
            let mut eve = new_swarm(move |_, _| Rendezvous::new(someone_else, Config::default()));
            eve.listen_on_random_memory_address().await;

            eve
        };

        alice.block_on_connection(&mut robert).await;
        bob.block_on_connection(&mut robert).await;
        eve.block_on_connection(&mut robert).await;

        Self {
            alice,
            bob,
            eve,
            robert,
        }
    }

    pub async fn assert_successful_registration(
        &mut self,
        expected_namespace: Namespace,
        expected_ttl: Ttl,
    ) {
        match await_events_or_timeout(&mut self.robert, &mut self.alice).await {
            (
                SwarmEvent::Behaviour(Event::PeerRegistered { peer, registration }),
                SwarmEvent::Behaviour(Event::Registered { rendezvous_node, ttl, namespace: register_node_namespace }),
            ) => {
                assert_eq!(&peer, self.alice.local_peer_id());
                assert_eq!(&rendezvous_node, self.robert.local_peer_id());
                assert_eq!(registration.namespace, expected_namespace);
                assert_eq!(register_node_namespace, expected_namespace);
                assert_eq!(ttl, expected_ttl);
            }
            (rendezvous_swarm_event, registration_swarm_event) => panic!(
                "Received unexpected event, rendezvous swarm emitted {:?} and registration swarm emitted {:?}",
                rendezvous_swarm_event, registration_swarm_event
            ),
        }
    }

    pub async fn assert_successful_discovery(
        &mut self,
        expected_namespace: Namespace,
        expected_ttl: Ttl,
        expected_peer_id: PeerId,
    ) {
        match await_events_or_timeout(&mut self.robert, &mut self.bob).await {
            (
                SwarmEvent::Behaviour(Event::DiscoverServed { .. }),
                SwarmEvent::Behaviour(Event::Discovered { registrations, .. }),
            ) => match registrations.as_slice() {
                [Registration {
                    namespace,
                    record,
                    ttl,
                }] => {
                    assert_eq!(*ttl, expected_ttl);
                    assert_eq!(record.peer_id(), expected_peer_id);
                    assert_eq!(*namespace, expected_namespace);
                }
                _ => panic!("Expected exactly one registration to be returned from discover"),
            },
            (e1, e2) => panic!("Unexpected events {:?} {:?}", e1, e2),
        }
    }
}

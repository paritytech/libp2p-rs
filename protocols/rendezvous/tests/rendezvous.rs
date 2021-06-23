pub mod harness;

use crate::harness::{await_events_or_timeout, new_swarm, SwarmExt};
use libp2p_core::identity;
use libp2p_core::PeerId;
use libp2p_rendezvous::{ErrorCode, DEFAULT_TTL};
use libp2p_rendezvous::{Event, RegisterError, Registration, Rendezvous};
use libp2p_swarm::{Swarm, SwarmEvent};

#[tokio::test]
async fn given_successful_registration_then_successful_discovery() {
    let _ = env_logger::try_init();
    let mut test = RendezvousTest::setup().await;

    let namespace = "some-namespace".to_string();

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

    let namespace = "some-namespace".to_string();

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

    let _ = test.alice.behaviour_mut().register(
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

    let namespace = "some-namespace".to_string();

    let _ = test.alice.behaviour_mut().register(
        namespace.clone(),
        *test.robert.local_peer_id(),
        Some(100_000),
    );

    match await_events_or_timeout(&mut test.robert, &mut test.alice).await {
        (
            SwarmEvent::Behaviour(Event::PeerNotRegistered { .. }),
            SwarmEvent::Behaviour(Event::RegisterFailed(RegisterError::Remote { error: err_code , ..})),
        ) => {
            assert_eq!(err_code, ErrorCode::InvalidTtl);
        }
        (rendezvous_swarm_event, registration_swarm_event) => panic!(
            "Received unexpected event, rendezvous swarm emitted {:?} and registration swarm emitted {:?}",
            rendezvous_swarm_event, registration_swarm_event
        ),
    }
}

#[tokio::test]
async fn eve_cannot_register() {
    let _ = env_logger::try_init();
    let mut test = RendezvousTest::setup().await;

    let namespace = "some-namespace".to_string();

    let _ = test.eve.behaviour_mut().register(
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

const DEFAULT_TTL_UPPER_BOUND: i64 = 56_000;

impl RendezvousTest {
    pub async fn setup() -> Self {
        let mut alice = new_swarm(|_, identity| Rendezvous::new(identity, DEFAULT_TTL_UPPER_BOUND));
        alice.listen_on_random_memory_address().await;

        let mut bob = new_swarm(|_, identity| Rendezvous::new(identity, DEFAULT_TTL_UPPER_BOUND));
        bob.listen_on_random_memory_address().await;

        let mut robert =
            new_swarm(|_, identity| Rendezvous::new(identity, DEFAULT_TTL_UPPER_BOUND));
        robert.listen_on_random_memory_address().await;

        let mut eve = {
            // In reality, if Eve were to try and fake someones identity, she would obviously only know the public key.
            // Due to the type-safe API of the `Rendezvous` behaviour and `PeerRecord`, we actually cannot construct a bad `PeerRecord` (i.e. one that is claims to be someone else).
            // As such, the best we can do is hand eve a completely different keypair from what she is using to authenticate her connection.
            let someone_else = identity::Keypair::generate_ed25519();
            let mut eve =
                new_swarm(move |_, _| Rendezvous::new(someone_else, DEFAULT_TTL_UPPER_BOUND));
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
        expected_namespace: String,
        expected_ttl: i64,
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
        expected_namespace: String,
        expected_ttl: i64,
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

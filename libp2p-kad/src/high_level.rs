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

//! High-level structs/traits of the crate.

use bytes::Bytes;
use fnv::FnvHashMap;
use futures::{self, future, Future};
use futures::sync::oneshot;
use kad_server::{KademliaServerController, KademliaServerConfig, KadServerInterface};
use kbucket::{KBucketsTable, UpdateOutcome};
use libp2p_peerstore::{PeerAccess, PeerId, Peerstore};
use libp2p_swarm::{Endpoint, MuxedTransport, SwarmController};
use libp2p_swarm::ConnectionUpgrade;
use multiaddr::Multiaddr;
use parking_lot::Mutex;
use query;
use std::collections::hash_map::Entry;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::iter;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer;

/// Prototype for a future Kademlia protocol running on a socket.
#[derive(Debug, Clone)]
pub struct KademliaConfig<P, R> {
	/// Degree of parallelism on the network. Often called `alpha` in technical papers.
	/// No more than this number of remotes will be used at a given time for any given operation.
	// TODO: ^ share this number between operations? or does each operation use `alpha` remotes?
	pub parallelism: u32,
	/// Used to load and store data requests of peers. TODO: say that must implement the `Recordstore` trait.
	pub record_store: R,
	/// Used to load and store information about peers.
	pub peer_store: P,
	/// Id of the local peer.
	pub local_peer_id: PeerId,
	/// The Kademlia system uses cycles. This is the duration of one cycle.
	pub cycles_duration: Duration,
	/// When contacting a node, duration after which we consider that it doesn't respond.
	pub timeout: Duration,
}

/// Object that allows one to make queries on the Kademlia system.
//#[derive(Debug)]      // TODO:
pub struct KademliaControllerPrototype<P, R> {
    inner: Arc<Inner<P, R>>,
}

impl<P, Pc, R> KademliaControllerPrototype<P, R>
    where P: Deref<Target = Pc>,
          for<'r> &'r Pc: Peerstore,
{
    /// Creates a new controller from that configuration.
    pub fn new(config: KademliaConfig<P, R>) -> KademliaControllerPrototype<P, R> {
		let buckets = KBucketsTable::new(config.local_peer_id.clone(), config.timeout);
		for peer_id in config.peer_store.deref().peers() {
			let _ = buckets.update(peer_id, ());
        }

		let inner = Arc::new(Inner {
			kbuckets: buckets,
			timer: tokio_timer::wheel().build(),
			record_store: config.record_store,
			peer_store: config.peer_store,
			connections: Default::default(),
            timeout: config.timeout,
            cycles_duration: config.cycles_duration,
            parallelism: config.parallelism,
		});

        KademliaControllerPrototype {
            inner: inner,
        }
    }

    pub fn start<T, C>(self, swarm: SwarmController<T, C>) -> KademliaController<P, R, T, C>
        where T: MuxedTransport + 'static,  // TODO: 'static :-/
              C: ConnectionUpgrade<T::RawConn> + 'static,       // TODO: 'static :-/
    {
        // TODO: initialization

        KademliaController {
            inner: self.inner.clone(),
            swarm_controller: swarm,
        }
    }
}

/// Object that allows one to make queries on the Kademlia system.
//#[derive(Debug)]      // TODO:
pub struct KademliaController<P, R, T, C>
    where T: MuxedTransport + 'static,          // TODO: 'static :-/
          C: ConnectionUpgrade<T::RawConn> + 'static,           // TODO: 'static :-/
{
    inner: Arc<Inner<P, R>>,
    swarm_controller: SwarmController<T, C>,
}

impl<P, R, T, C> Clone for KademliaController<P, R, T, C>
    where T: Clone + MuxedTransport + 'static,          // TODO: 'static :-/
          C: Clone + ConnectionUpgrade<T::RawConn> + 'static,           // TODO: 'static :-/
{
    #[inline]
    fn clone(&self) -> Self {
        KademliaController {
            inner: self.inner.clone(),
            swarm_controller: self.swarm_controller.clone(),
        }
    }
}

impl<P, Pc, R, T, C> KademliaController<P, R, T, C>
    where P: Deref<Target = Pc>,
          for<'r> &'r Pc: Peerstore,
          R: Clone,
          T: Clone + MuxedTransport + 'static,          // TODO: 'static :-/
          C: Clone + ConnectionUpgrade<T::RawConn> + 'static,           // TODO: 'static :-/
{
    #[inline]
    pub fn find_node(&self, searched_key: PeerId)
                     -> Box<Future<Item = Vec<PeerId>, Error = IoError>>
        where P: Clone + 'static,
              R: 'static,
              C::NamesIter: Clone,
              C::Output: From<KademliaProcessingFuture>,
    {
        query::find_node(self.clone(), searched_key)
    }
}

/// Connection upgrade to the Kademlia protocol.
#[derive(Clone)]        // TODO: Debug
pub struct KademliaUpgrade<P, R> {
    inner: Arc<Inner<P, R>>,
    upgrade: KademliaServerConfig<Arc<Inner<P, R>>>,
}

impl<P, R> KademliaUpgrade<P, R> {
    /// Builds a connection upgrade from the controller.
    #[inline]
    pub fn new(proto: &KademliaControllerPrototype<P, R>) -> Self {
        KademliaUpgrade {
            inner: proto.inner.clone(),
            upgrade: KademliaServerConfig::new(proto.inner.clone()),
        }
    }

    /// Builds a connection upgrade from the controller.
    #[inline]
    pub fn from_controller<T, C>(ctl: &KademliaController<P, R, T, C>) -> Self
        where T: MuxedTransport,
              C: ConnectionUpgrade<T::RawConn>,
    {
        KademliaUpgrade {
            inner: ctl.inner.clone(),
            upgrade: KademliaServerConfig::new(ctl.inner.clone()),
        }
    }
}

impl<C, P, Pc, R> ConnectionUpgrade<C> for KademliaUpgrade<P, R>
where
	C: AsyncRead + AsyncWrite + 'static, // TODO: 'static :-/
    P: Deref<Target = Pc> + Clone + 'static,        // TODO: 'static :-/
    for<'r> &'r Pc: Peerstore,
    R: 'static,         // TODO: 'static :-/
{
	type Output = KademliaProcessingFuture;
	type Future = Box<Future<Item = Self::Output, Error = IoError>>;
	type NamesIter = iter::Once<(Bytes, ())>;
	type UpgradeIdentifier = ();

	#[inline]
	fn protocol_names(&self) -> Self::NamesIter {
        ConnectionUpgrade::<C>::protocol_names(&self.upgrade)
	}

	#[inline]
	fn upgrade(self, incoming: C, id: (), endpoint: Endpoint, addr: &Multiaddr) -> Self::Future {
        let inner = self.inner;
        let client_addr = addr.clone();

        let future = self.upgrade
            .upgrade(incoming, id, endpoint, addr)
            .map(move |(controller, future)| {
                match inner.connections.lock().entry(client_addr) {
                    Entry::Occupied(mut entry) => {
                        match entry.insert(Connection::Active(controller)) {
                            Connection::Active(_) => {},
                            Connection::Pending(closures) => {
                                let new_ctl = match entry.get_mut() {
                                    &mut Connection::Active(ref mut ctl) => ctl,
                                    _ => unreachable!("we just inserted an Active enum variant")
                                };

                                for mut closure in closures {
                                    closure(new_ctl);
                                }
                            },
                        };
                    },
                    Entry::Vacant(entry) => {
                        println!("vacant");
                        entry.insert(Connection::Active(controller));
                    },
                };

                KademliaProcessingFuture { inner: future }
            });

        Box::new(future) as Box<_>
	}
}

/// Future that must be processed for the Kademlia system to work.
pub struct KademliaProcessingFuture {
    inner: Box<Future<Item = (), Error = IoError>>,
}

impl Future for KademliaProcessingFuture {
    type Item = ();
    type Error = IoError;

    #[inline]
    fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

// Inner struct shared throughout the Kademlia system.
//#[derive(Debug)]      // TODO:
struct Inner<P, R> {
	// The remotes are identified by their public keys.
	kbuckets: KBucketsTable<PeerId, ()>,

	// Timer used for building the timeouts.
	timer: tokio_timer::Timer,

    // Same as in the config.
    timeout: Duration,

    // Same as in the config.
    parallelism: u32,

    // Same as in the config.
    cycles_duration: Duration,

	// Same fields as `KademliaConfig`.
	record_store: R,
	peer_store: P,

	// List of open connections with remotes.
	// TODO: is it correct to use FnvHashMap with a Multiaddr? needs benchmarks
	connections: Mutex<FnvHashMap<Multiaddr, Connection>>,
}

//#[derive(Debug)]      // TODO:
enum Connection {
    Active(KademliaServerController),
    // TODO: should be FnOnce once Rust allows that
    Pending(Vec<Box<FnMut(&mut KademliaServerController)>>),
}

impl<P, Pc, R> KadServerInterface for Arc<Inner<P, R>>
    where P: Deref<Target = Pc>,
          for<'r> &'r Pc: Peerstore,
{
    #[inline]
	fn local_id(&self) -> &PeerId {
        self.kbuckets.my_id()
    }

    #[inline]
	fn kbuckets_update(&self, peer: &PeerId) {
		// TODO: is this the right place for this check?
		if peer == self.kbuckets.my_id() {
			return;
		}

		match self.kbuckets.update(peer.clone(), ()) {
			UpdateOutcome::NeedPing(node_to_ping) => {
				// TODO: return this info somehow
				println!("need to ping {:?}", node_to_ping);
			}
			_ => (),
		}
    }

    #[inline]
	fn kbuckets_find_closest(&self, addr: &PeerId) -> Vec<PeerId> {
		self.kbuckets.find_closest(addr).collect()
    }
}

impl<R, P, Pc, T, C> query::QueryInterface for KademliaController<P, R, T, C>
where
	P: Clone + Deref<Target = Pc> + 'static,            // TODO: 'static :-/
    for<'r> &'r Pc: Peerstore,
	R: Clone + 'static,         // TODO: 'static :-/
    T: Clone + MuxedTransport + 'static,          // TODO: 'static :-/
    C: Clone + ConnectionUpgrade<T::RawConn> + 'static,           // TODO: 'static :-/
    C::NamesIter: Clone,
    C::Output: From<KademliaProcessingFuture>,
{
	#[inline]
	fn local_id(&self) -> &PeerId {
		self.inner.kbuckets.my_id()
	}

	#[inline]
	fn kbuckets_find_closest(&self, addr: &PeerId) -> Vec<PeerId> {
		self.inner.kbuckets.find_closest(addr).collect()
	}

    #[inline]
    fn peer_add_addrs<I>(&self, peer: &PeerId, multiaddrs: I, ttl: Duration)
		where I: Iterator<Item = Multiaddr>
    {
        self.inner.peer_store.peer_or_create(peer).add_addrs(multiaddrs, ttl);
    }

	#[inline]
	fn parallelism(&self) -> usize {
		self.inner.parallelism as usize
	}

	#[inline]
	fn cycle_duration(&self) -> Duration {
		self.inner.cycles_duration
	}

	#[inline]
	fn send<F, FRet>(&self, addr: Multiaddr, and_then: F)
                     -> Box<Future<Item = FRet, Error = IoError>>
        where F: FnOnce(&KademliaServerController) -> FRet + 'static,
              FRet: 'static,
    {
        let mut lock = self.inner.connections.lock();

        let pending_list = match lock.entry(addr.clone()) {
            Entry::Occupied(entry) => {
                match entry.into_mut() {
                    &mut Connection::Pending(ref mut list) => list,
                    &mut Connection::Active(ref mut ctrl) => {
                        let output = future::ok(and_then(ctrl));
                        return Box::new(output) as Box<_>;
                    },
                }
            },
            Entry::Vacant(entry) => {
                let proto = KademliaUpgrade {
                    inner: self.inner.clone(),
                    upgrade: KademliaServerConfig::new(self.inner.clone()),
                };
                self.swarm_controller.dial_to_handler(addr, proto).unwrap();      // TODO: don't unwrap
                match entry.insert(Connection::Pending(Vec::with_capacity(1))) {
                    &mut Connection::Pending(ref mut list) => list,
                    _ => unreachable!("we just inserted a Pending variant")
                }
            },
        };

        let (tx, rx) = oneshot::channel();
        let mut tx = Some(tx);
        let mut and_then = Some(and_then);
        pending_list.push(Box::new(move |ctrl: &mut KademliaServerController| {
            let and_then = and_then.take().expect("pending closures are only ever called once");
            let tx = tx.take().expect("pending closures are only ever called once");
            let ret = and_then(ctrl);
            let _ = tx.send(ret);       // TODO: better error handling
        }) as Box<_>);

        let future = rx.map_err(|_| IoErrorKind::ConnectionAborted.into());
        let future = self.inner.timer.timeout(future, self.inner.timeout);
        Box::new(future) as Box<_>
	}
}

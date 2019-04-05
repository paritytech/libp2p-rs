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

use crate::{
    Multiaddr, PeerId,
    nodes::raw_swarm::ConnectedPoint,
    protocols_handler::{IntoProtocolsHandler, ProtocolsHandler},
    swarm::PollParameters,
};
use futures::prelude::*;
use std::error;

/// A behaviour for the network. Allows customizing the swarm.
///
/// This trait has been designed to be composable. Multiple implementations can be combined into
/// one that handles all the behaviours at once.
pub trait NetworkBehaviour {
    /// Handler for all the protocols the network supports.
    type ProtocolsHandler: IntoProtocolsHandler;
    /// Event generated by the swarm.
    type OutEvent;

    /// Creates a new `ProtocolsHandler` for a connection with a peer.
    fn new_handler(&mut self) -> Self::ProtocolsHandler;

    /// Addresses that this behaviour is aware of for this specific peer, and that may allow
    /// reaching the peer.
    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr>;

    /// Indicates the behaviour that we connected to the node with the given peer id through the
    /// given endpoint.
    fn inject_connected(&mut self, peer_id: PeerId, endpoint: ConnectedPoint);

    /// Indicates the behaviour that we disconnected from the node with the given peer id. The
    /// endpoint is the one we used to be connected to.
    fn inject_disconnected(&mut self, peer_id: &PeerId, endpoint: ConnectedPoint);

    /// Indicates the behaviour that we replace the connection from the node with another.
    fn inject_replaced(&mut self, peer_id: PeerId, closed_endpoint: ConnectedPoint, new_endpoint: ConnectedPoint) {
        self.inject_disconnected(&peer_id, closed_endpoint);
        self.inject_connected(peer_id, new_endpoint);
    }

    /// Indicates the behaviour that the node with the given peer id has generated an event for
    /// us.
    ///
    /// > **Note**: This method is only called for events generated by the protocols handler.
    fn inject_node_event(
        &mut self,
        peer_id: PeerId,
        event: <<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::OutEvent
    );

    /// Indicates to the behaviour that we tried to reach an address, but failed.
    ///
    /// If we were trying to reach a specific node, its ID is passed as parameter. If this is the
    /// last address to attempt for the given node, then `inject_dial_failure` is called afterwards.
    fn inject_addr_reach_failure(&mut self, _peer_id: Option<&PeerId>, _addr: &Multiaddr, _error: &dyn error::Error) {
    }

    /// Indicates to the behaviour that we tried to dial all the addresses known for a node, but
    /// failed.
    fn inject_dial_failure(&mut self, _peer_id: &PeerId) {
    }

    /// Polls for things that swarm should do.
    ///
    /// This API mimics the API of the `Stream` trait.
    fn poll(&mut self, params: &mut PollParameters<'_>) -> Async<NetworkBehaviourAction<<<Self::ProtocolsHandler as IntoProtocolsHandler>::Handler as ProtocolsHandler>::InEvent, Self::OutEvent>>;
}

/// Used when deriving `NetworkBehaviour`. When deriving `NetworkBehaviour`, must be implemented
/// for all the possible event types generated by the various fields.
// TODO: document how the custom behaviour works and link this here
pub trait NetworkBehaviourEventProcess<TEvent> {
    /// Called when one of the fields of the type you're deriving `NetworkBehaviour` on generates
    /// an event.
    fn inject_event(&mut self, event: TEvent);
}

/// An action that a [`NetworkBehaviour`] can trigger in the [`Swarm`]
/// in whose context it is executing.
#[derive(Debug, Clone)]
pub enum NetworkBehaviourAction<TInEvent, TOutEvent> {
    /// Instructs the `Swarm` to return an event when it is being polled.
    GenerateEvent(TOutEvent),

    // TODO: report new raw connection for usage after intercepting an address dial

    /// Instructs the swarm to dial the given multiaddress, without a known `PeerId`.
    DialAddress {
        /// The address to dial.
        address: Multiaddr,
    },

    /// Instructs the swarm to dial a known `PeerId`.
    ///
    /// On success, [`NetworkBehaviour::inject_connected`] is invoked.
    /// On failure, [`NetworkBehaviour::inject_dial_failure`] is invoked.
    DialPeer {
        /// The peer to try reach.
        peer_id: PeerId,
    },

    /// Instructs the `Swarm` to send a message to a connected peer.
    ///
    /// If the `Swarm` is connected to the peer, the message is delivered to the remote's
    /// protocol handler. If there is no connection to the peer, the message is ignored.
    /// To ensure delivery, the `NetworkBehaviour` must keep track of connected peers.
    SendEvent {
        /// The peer to which to send the message.
        peer_id: PeerId,
        /// The message to send.
        event: TInEvent,
    },

    /// Informs the `Swarm` about a multi-address observed by a remote for
    /// the local node.
    ///
    /// The swarm will pass this address through the transport's NAT traversal.
    ReportObservedAddr {
        /// The observed address of the local node.
        address: Multiaddr,
    },
}

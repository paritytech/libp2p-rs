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

use bytes::{BufMut, Bytes, BytesMut};
use crate::rpc_proto;
use futures::future;
use libp2p_core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo, PeerId};

use protobuf::Message as ProtobufMessage;
use std::{io, iter};
use tokio_codec::{Decoder, Encoder, Framed};
use tokio_io::{AsyncRead, AsyncWrite};
use topic::TopicHash;
use unsigned_varint::codec;

/// Implementation of `ConnectionUpgrade` for the Gossipsub protocol.
#[derive(Debug, Clone)]
pub struct GossipsubConfig {}

impl GossipsubConfig {
    /// Builds a new `GossipsubConfig`.
    #[inline]
    pub fn new() -> GossipsubConfig {
        GossipsubConfig {}
    }
}

impl UpgradeInfo for GossipsubConfig {
    type UpgradeId = ();
    type NamesIter = iter::Once<(Bytes, Self::UpgradeId)>;

    #[inline]
    fn protocol_names(&self) -> Self::NamesIter {
        iter::once(("/gossipsub/1.0.0".into(), ()))
    }
}

impl<TSocket> InboundUpgrade<TSocket> for GossipsubConfig
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = Framed<TSocket, GossipsubCodec>;
    type Error = io::Error;
    type Future = future::FutureResult<Self::Output, Self::Error>;

    #[inline]
    fn upgrade_inbound(self, socket: TSocket, _: Self::UpgradeId) -> Self::Future {
        future::ok(Framed::new(socket, GossipsubCodec { length_prefix: Default::default() }))
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for GossipsubConfig
where
    TSocket: AsyncRead + AsyncWrite,
{
    type Output = Framed<TSocket, GossipsubCodec>;
    type Error = io::Error;
    type Future = future::FutureResult<Self::Output, Self::Error>;

    #[inline]
    fn upgrade_outbound(self, socket: TSocket, _: Self::UpgradeId) -> Self::Future {
        future::ok(Framed::new(socket, GossipsubCodec { length_prefix: Default::default() }))
    }
}

/// Implementation of `tokio_codec::Codec`.
pub struct GossipsubCodec {
    /// The codec for encoding/decoding the length prefix of messages.
    length_prefix: codec::UviBytes,
}

impl Encoder for GossipsubCodec {
    type Item = GossipsubRpc;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut proto = rpc_proto::RPC::new();

        for message in item.messages.into_iter() {
            let mut msg = rpc_proto::Message::new();
            msg.set_from(message.source.into_bytes());
            msg.set_data(message.data);
            msg.set_seqno(message.sequence_number);
            msg.set_topicIDs(
                message
                    .topics
                    .into_iter()
                    .map(TopicHash::into_string)
                    .collect(),
            );
            msg.set_signature(message.signature);
            msg.set_key(message.key);
            proto.mut_publish().push(msg);
        }

        for topic in item.subscriptions.into_iter() {
            let mut subscription = rpc_proto::RPC_SubOpts::new();
            subscription.set_subscribe(topic.action == GossipsubSubscriptionAction::Subscribe);
            subscription.set_topicid(topic.topic.into_string());
            proto.mut_subscriptions().push(subscription);
        }

        let msg_size = proto.compute_size();
        // Reserve enough space for the data and the length. The length has a maximum of 32 bits,
        // which means that 5 bytes is enough for the variable-length integer.
        dst.reserve(msg_size as usize + 5);

        proto
            .write_length_delimited_to_writer(&mut dst.by_ref().writer())
            .expect(
                "there is no situation in which the protobuf message can be invalid, and \
                 writing to a BytesMut never fails as we reserved enough space beforehand",
            );
        Ok(())
    }
}

impl Decoder for GossipsubCodec {
    type Item = GossipsubRpc;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let packet = match self.length_prefix.decode(src)? {
            Some(p) => p,
            None => return Ok(None),
        };

        let mut rpc: rpc_proto::RPC = protobuf::parse_from_bytes(&packet)?;

        let mut messages = Vec::with_capacity(rpc.get_publish().len());
        for mut publish in rpc.take_publish().into_iter() {
            messages.push(GossipsubMessage {
                source: PeerId::from_bytes(publish.take_from()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid peer ID in message")
                })?,
                data: publish.take_data(),
                sequence_number: publish.take_seqno(),
                topics: publish
                    .take_topicIDs()
                    .into_iter()
                    .map(|topic| TopicHash::from_raw(topic))
                    .collect(),
                signature: publish.take_signature(),
                key: publish.take_key(),
            });
        }

        Ok(Some(GossipsubRpc {
            messages,
            subscriptions: rpc
                .take_subscriptions()
                .into_iter()
                .map(|mut sub| GossipsubSubscription {
                    action: if sub.get_subscribe() {
                        GossipsubSubscriptionAction::Subscribe
                    } else {
                        GossipsubSubscriptionAction::Unsubscribe
                    },
                    topic: TopicHash::from_raw(sub.take_topicid()),
                })
                .collect(),
        }))
    }
}

/// An RPC received by the Gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GossipsubRpc {
    /// List of messages that were part of this RPC query.
    pub messages: Vec<GossipsubMessage>,
    /// List of subscriptions.
    pub subscriptions: Vec<GossipsubSubscription>,
    /// Optional control message.
    pub controlMessage: ControlMessage,
}

/// A message received by the Gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GossipsubMessage {
    /// Id of the peer that published this message.
    pub source: PeerId,

    /// Content of the message. Its meaning is out of scope of this library.
    pub data: Vec<u8>,

    /// An incrementing sequence number.
    pub sequence_number: Vec<u8>,

    /// List of topics this message belongs to.
    ///
    /// Each message can belong to multiple topics at once.
    pub topics: Vec<TopicHash>,

    /// To use for an authentication scheme (not yet defined or implemented),
    /// see rpc.proto for more info.
    pub signature: Vec<u8>,

    /// To use for an encryption scheme (not yet defined or implemented),
    /// see rpc.proto for more info.
    pub key: Vec<u8>,
}

/// A subscription received by the Gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GossipsubSubscription {
    /// Action to perform.
    pub action: GossipsubSubscriptionAction,
    /// The topic from which to subscribe or unsubscribe.
    pub topic: TopicHash,
}

/// Action that a subscription wants to perform.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GossipsubSubscriptionAction {
    /// The remote wants to subscribe to the given topic.
    Subscribe,
    /// The remote wants to unsubscribe from the given topic.
    Unsubscribe,
}

/// A message used to control the Gossipsub network.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlMessage {
    /// The control message for gossiping.
    pub ihave: ControlIHave,
    /// Request transmission of messages announced in a `ControlIHave`
    /// message.
    pub iwant: ControlIWant,
    /// Graft a mesh link; this notifies the peer that it has been added
    /// to the local mesh view.
    pub graft: ControlGraft,
    /// The control message for pruning mesh links.
    pub prune: ControlPrune,
}

/// Gossip control message; this notifies the peer that the following
/// messages were recently seen and are available on request.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlIHave {
    /// Topic that the messages belong to.
    pub topic: TopicHash,
    /// List of messages that have been recently seen and are available
    /// on request.
    pub messages: Vec<messageID>,
}

/// Control message that requests messages from a peer that announced them
/// with an IHave message.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlWant {
    /// List of messages that are being requested.
    pub messages: Vec<messageID>,
}

/// Control message that grafts a mesh link; this notifies the peer that it
/// has been added to the local mesh view of a topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlGraft {
    /// Topic to graft a peer to.
    pub topic: TopicHash,
}

/// Control message that prunes a mesh link; this notifies the peer that it
/// has been removed from the local mesh view of a topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ControlPrune {
    /// Topic to prune a peer from.
    pub topic: TopicHash,
}

/// A graft/prune received by the Gossipsub system.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GossipsubGraftPrune {
    /// Graft or prune action to perform.
    pub action: GossipSubGraftPruneAction,
    /// The topic from which to graft a peer to or prune from.
    pub topic: TopicHash,
}

/// Action to graft or prune to/from a topic. Manages mesh membership.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GossipSubGraftPruneAction {
    /// The remote wants to graft to the given topic.
    Graft,
    /// The remote wants to prune from the given topic.
    Prune,
}

/// The different kinds of messages transferred over a Gossipsub network.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Message {
    /// A `GossipsubMessage` message, contains arbitrary data.
    GossipsubMessage,
    /// A `ControlMessage`, use to control the Gossipsub network.
    ControlMessage,
}

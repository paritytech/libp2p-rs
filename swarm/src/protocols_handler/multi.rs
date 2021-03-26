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

//! A [`ProtocolsHandler`] implementation that combines multiple other `ProtocolsHandler`s
//! indexed by some key.

use crate::NegotiatedSubstream;
use crate::protocols_handler::{
    KeepAlive,
    IntoProtocolsHandler,
    ProtocolsHandler,
    ProtocolsHandlerEvent,
    ProtocolsHandlerUpgrErr,
    SubstreamProtocol
};
use crate::upgrade::{
    InboundUpgradeSend,
    OutboundUpgradeSend,
    UpgradeInfoSend
};
use futures::{future::BoxFuture, prelude::*};
use libp2p_core::{ConnectedPoint, Multiaddr, PeerId};
use libp2p_core::upgrade::{ProtocolName, UpgradeError, NegotiationError, ProtocolError};
use rand::Rng;
use std::{
    cmp,
    collections::{HashMap, HashSet},
    error,
    fmt,
    hash::Hash,
    iter::{self, FromIterator},
    task::{Context, Poll},
    time::Duration
};

/// A [`ProtocolsHandler`] for multiple `ProtocolsHandler`s of the same type.
#[derive(Clone)]
pub struct MultiHandler<K, H> {
    handlers: HashMap<K, H>
}

impl<K, H> fmt::Debug for MultiHandler<K, H>
where
    K: fmt::Debug + Eq + Hash,
    H: fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiHandler")
            .field("handlers", &self.handlers)
            .finish()
    }
}

impl<K, H> MultiHandler<K, H>
where
    K: Hash + Eq,
    H: ProtocolsHandler
{
    /// Create and populate a `MultiHandler` from the given handler iterator.
    ///
    /// It is an error for any two protocols handlers to share the same protocol name.
    pub fn try_from_iter<I>(iter: I) -> Result<Self, DuplicateProtonameError>
    where
        I: IntoIterator<Item = (K, H)>
    {
        let m = MultiHandler { handlers: HashMap::from_iter(iter) };
        uniq_proto_names(m.handlers.values().map(|h| h.listen_protocol().into_upgrade().0))?;
        Ok(m)
    }
}

impl<K, H> ProtocolsHandler for MultiHandler<K, H>
where
    K: Clone + Hash + Eq + Send + 'static,
    H: ProtocolsHandler,
    H::InboundProtocol: InboundUpgradeSend,
    H::OutboundProtocol: OutboundUpgradeSend
{
    type InEvent = (K, <H as ProtocolsHandler>::InEvent);
    type OutEvent = (K, <H as ProtocolsHandler>::OutEvent);
    type Error = <H as ProtocolsHandler>::Error;
    type InboundProtocol = Upgrade<K, <H as ProtocolsHandler>::InboundProtocol>;
    type OutboundProtocol = <H as ProtocolsHandler>::OutboundProtocol;
    type InboundOpenInfo = Info<K, <H as ProtocolsHandler>::InboundOpenInfo>;
    type OutboundOpenInfo = (K, <H as ProtocolsHandler>::OutboundOpenInfo);

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        let (upgrade, info, timeout) = self.handlers.iter()
            .map(|(key, handler)| {
                let proto = handler.listen_protocol();
                let timeout = *proto.timeout();
                let (upgrade, info) = proto.into_upgrade();
                (key.clone(), (upgrade, info, timeout))
            })
            .fold((Upgrade::new(), Info::new(), Duration::from_secs(0)),
                |(mut upg, mut inf, mut timeout), (k, (u, i, t))| {
                    upg.upgrades.push((k.clone(), u));
                    inf.infos.push((k, i));
                    timeout = cmp::max(timeout, t);
                    (upg, inf, timeout)
                }
            );
        SubstreamProtocol::new(upgrade, info).with_timeout(timeout)
    }

    fn inject_fully_negotiated_outbound (
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        (key, arg): Self::OutboundOpenInfo
    ) {
        if let Some(h) = self.handlers.get_mut(&key) {
            h.inject_fully_negotiated_outbound(protocol, arg)
        } else {
            log::error!("inject_fully_negotiated_outbound: no handler for key")
        }
    }

    fn inject_fully_negotiated_inbound (
        &mut self,
        (key, arg): <Self::InboundProtocol as InboundUpgradeSend>::Output,
        mut info: Self::InboundOpenInfo
    ) {
        if let Some(h) = self.handlers.get_mut(&key) {
            if let Some(i) = info.take(&key) {
                h.inject_fully_negotiated_inbound(arg, i)
            }
        } else {
            log::error!("inject_fully_negotiated_inbound: no handler for key")
        }
    }

    fn inject_event(&mut self, (key, event): Self::InEvent) {
        if let Some(h) = self.handlers.get_mut(&key) {
            h.inject_event(event)
        } else {
            log::error!("inject_event: no handler for key")
        }
    }

    fn inject_address_change(&mut self, addr: &Multiaddr) {
        for h in self.handlers.values_mut() {
            h.inject_address_change(addr)
        }
    }

    fn inject_dial_upgrade_error (
        &mut self,
        (key, arg): Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>
    ) {
        if let Some(h) = self.handlers.get_mut(&key) {
            h.inject_dial_upgrade_error(arg, error)
        } else {
            log::error!("inject_dial_upgrade_error: no handler for protocol")
        }
    }

    fn inject_listen_upgrade_error(
        &mut self,
        mut info: Self::InboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::InboundProtocol as InboundUpgradeSend>::Error>
    ) {
        match error {
            ProtocolsHandlerUpgrErr::Timer =>
                for (k, h) in &mut self.handlers {
                    if let Some(i) = info.take(k) {
                        h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Timer)
                    }
                }
            ProtocolsHandlerUpgrErr::Timeout =>
                for (k, h) in &mut self.handlers {
                    if let Some(i) = info.take(k) {
                        h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Timeout)
                    }
                }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)) =>
                for (k, h) in &mut self.handlers {
                    if let Some(i) = info.take(k) {
                        h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::Failed)))
                    }
                }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(NegotiationError::ProtocolError(e))) =>
                match e {
                    ProtocolError::IoError(e) =>
                        for (k, h) in &mut self.handlers {
                            if let Some(i) = info.take(k) {
                                let e = NegotiationError::ProtocolError(ProtocolError::IoError(e.kind().into()));
                                h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(e)))
                            }
                        }
                    ProtocolError::InvalidMessage =>
                        for (k, h) in &mut self.handlers {
                            if let Some(i) = info.take(k) {
                                let e = NegotiationError::ProtocolError(ProtocolError::InvalidMessage);
                                h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(e)))
                            }
                        }
                    ProtocolError::InvalidProtocol =>
                        for (k, h) in &mut self.handlers {
                            if let Some(i) = info.take(k) {
                                let e = NegotiationError::ProtocolError(ProtocolError::InvalidProtocol);
                                h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(e)))
                            }
                        }
                    ProtocolError::TooManyProtocols =>
                        for (k, h) in &mut self.handlers {
                            if let Some(i) = info.take(k) {
                                let e = NegotiationError::ProtocolError(ProtocolError::TooManyProtocols);
                                h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Select(e)))
                            }
                        }
                }
            ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply((k, e))) =>
                if let Some(h) = self.handlers.get_mut(&k) {
                    if let Some(i) = info.take(&k) {
                        h.inject_listen_upgrade_error(i, ProtocolsHandlerUpgrErr::Upgrade(UpgradeError::Apply(e)))
                    }
                }
        }
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        self.handlers.values()
            .map(|h| h.connection_keep_alive())
            .max()
            .unwrap_or(KeepAlive::No)
    }

    fn poll(&mut self, cx: &mut Context<'_>)
        -> Poll<ProtocolsHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::OutEvent, Self::Error>>
    {
        // Calling `gen_range(0, 0)` (see below) would panic, so we have return early to avoid
        // that situation.
        if self.handlers.is_empty() {
            return Poll::Pending;
        }

        // Not always polling handlers in the same order should give anyone the chance to make progress.
        let pos = rand::thread_rng().gen_range(0, self.handlers.len());

        for (k, h) in self.handlers.iter_mut().skip(pos) {
            if let Poll::Ready(e) = h.poll(cx) {
                let e = e.map_outbound_open_info(|i| (k.clone(), i)).map_custom(|p| (k.clone(), p));
                return Poll::Ready(e)
            }
        }

        for (k, h) in self.handlers.iter_mut().take(pos) {
            if let Poll::Ready(e) = h.poll(cx) {
                let e = e.map_outbound_open_info(|i| (k.clone(), i)).map_custom(|p| (k.clone(), p));
                return Poll::Ready(e)
            }
        }

        Poll::Pending
    }
}

/// A [`IntoProtocolsHandler`] for multiple other `IntoProtocolsHandler`s.
#[derive(Clone)]
pub struct IntoMultiHandler<K, H> {
    handlers: HashMap<K, H>
}

impl<K, H> fmt::Debug for IntoMultiHandler<K, H>
where
    K: fmt::Debug + Eq + Hash,
    H: fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntoMultiHandler")
            .field("handlers", &self.handlers)
            .finish()
    }
}


impl<K, H> IntoMultiHandler<K, H>
where
    K: Hash + Eq,
    H: IntoProtocolsHandler
{
    /// Create and populate an `IntoMultiHandler` from the given iterator.
    ///
    /// It is an error for any two protocols handlers to share the same protocol name.
    pub fn try_from_iter<I>(iter: I) -> Result<Self, DuplicateProtonameError>
    where
        I: IntoIterator<Item = (K, H)>
    {
        let m = IntoMultiHandler { handlers: HashMap::from_iter(iter) };
        uniq_proto_names(m.handlers.values().map(|h| h.inbound_protocol()))?;
        Ok(m)
    }
}

impl<K, H> IntoProtocolsHandler for IntoMultiHandler<K, H>
where
    K: Clone + Eq + Hash + Send + 'static,
    H: IntoProtocolsHandler
{
    type Handler = MultiHandler<K, H::Handler>;

    fn into_handler(self, p: &PeerId, c: &ConnectedPoint) -> Self::Handler {
        MultiHandler {
            handlers: self.handlers.into_iter()
                .map(|(k, h)| (k, h.into_handler(p, c)))
                .collect()
        }
    }

    fn inbound_protocol(&self) -> <Self::Handler as ProtocolsHandler>::InboundProtocol {
        Upgrade {
            upgrades: self.handlers.iter()
                .map(|(k, h)| (k.clone(), h.inbound_protocol()))
                .collect()
        }
    }
}

/// Index and protocol name pair used as `UpgradeInfo::Info`.
#[derive(Debug, Clone)]
pub struct IndexedProtoName<H>(usize, H);

impl<H: ProtocolName> ProtocolName for IndexedProtoName<H> {
    fn protocol_name(&self) -> &[u8] {
        self.1.protocol_name()
    }
}

/// The aggregated `InboundOpenInfo`s of supported inbound substream protocols.
#[derive(Clone)]
pub struct Info<K, I> {
    infos: Vec<(K, I)>
}

impl<K: Eq, I> Info<K, I> {
    fn new() -> Self {
        Info { infos: Vec::new() }
    }

    pub fn take(&mut self, k: &K) -> Option<I> {
        if let Some(p) = self.infos.iter().position(|(key, _)| key == k) {
            return Some(self.infos.remove(p).1)
        }
        None
    }
}

/// Inbound and outbound upgrade for all `ProtocolsHandler`s.
#[derive(Clone)]
pub struct Upgrade<K, H> {
    upgrades: Vec<(K, H)>
}

impl<K, H> Upgrade<K, H> {
    fn new() -> Self {
        Upgrade { upgrades: Vec::new() }
    }
}

impl<K, H> fmt::Debug for Upgrade<K, H>
where
    K: fmt::Debug + Eq + Hash,
    H: fmt::Debug
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upgrade")
            .field("upgrades", &self.upgrades)
            .finish()
    }
}

impl<K, H> UpgradeInfoSend for Upgrade<K, H>
where
    H: UpgradeInfoSend,
    K: Send + 'static
{
    type Info = IndexedProtoName<H::Info>;
    type InfoIter = std::vec::IntoIter<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        self.upgrades.iter().enumerate()
            .map(|(i, (_, h))| iter::repeat(i).zip(h.protocol_info()))
            .flatten()
            .map(|(i, h)| IndexedProtoName(i, h))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl<K, H> InboundUpgradeSend for Upgrade<K, H>
where
    H: InboundUpgradeSend,
    K: Send + 'static
{
    type Output = (K, <H as InboundUpgradeSend>::Output);
    type Error  = (K, <H as InboundUpgradeSend>::Error);
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(mut self, resource: NegotiatedSubstream, info: Self::Info) -> Self::Future {
        let IndexedProtoName(index, info) = info;
        let (key, upgrade) = self.upgrades.remove(index);
        upgrade.upgrade_inbound(resource, info)
            .map(move |out| {
                match out {
                    Ok(o) => Ok((key, o)),
                    Err(e) => Err((key, e))
                }
            })
            .boxed()
    }
}

impl<K, H> OutboundUpgradeSend for Upgrade<K, H>
where
    H: OutboundUpgradeSend,
    K: Send + 'static
{
    type Output = (K, <H as OutboundUpgradeSend>::Output);
    type Error  = (K, <H as OutboundUpgradeSend>::Error);
    type Future = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(mut self, resource: NegotiatedSubstream, info: Self::Info) -> Self::Future {
        let IndexedProtoName(index, info) = info;
        let (key, upgrade) = self.upgrades.remove(index);
        upgrade.upgrade_outbound(resource, info)
            .map(move |out| {
                match out {
                    Ok(o) => Ok((key, o)),
                    Err(e) => Err((key, e))
                }
            })
            .boxed()
    }
}

/// Check that no two protocol names are equal.
fn uniq_proto_names<I, T>(iter: I) -> Result<(), DuplicateProtonameError>
where
    I: Iterator<Item = T>,
    T: UpgradeInfoSend
{
    let mut set = HashSet::new();
    for infos in iter {
        for i in infos.protocol_info() {
            let v = Vec::from(i.protocol_name());
            if set.contains(&v) {
                return Err(DuplicateProtonameError(v))
            } else {
                set.insert(v);
            }
        }
    }
    Ok(())
}

/// It is an error if two handlers share the same protocol name.
#[derive(Debug, Clone)]
pub struct DuplicateProtonameError(Vec<u8>);

impl DuplicateProtonameError {
    /// The protocol name bytes that occured in more than one handler.
    pub fn protocol_name(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for DuplicateProtonameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "duplicate protocol name: {}", s)
        } else {
            write!(f, "duplicate protocol name: {:?}", self.0)
        }
    }
}

impl error::Error for DuplicateProtonameError {}

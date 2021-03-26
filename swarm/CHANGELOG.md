# 0.29.0 [unreleased]

- Remove `Deref` and `DerefMut` implementations previously dereferencing to the
  `NetworkBehaviour` on `Swarm`. Instead one can access the `NetworkBehaviour`
  via `Swarm::behaviour` and `Swarm::behaviour_mut`. Methods on `Swarm` can now
  be accessed directly, e.g. via `my_swarm.local_peer_id()`. You may use the
  command below to transform fully qualified method calls on `Swarm` to simple
  method calls [PR 1995](https://github.com/libp2p/rust-libp2p/pull/1995).
  
  ``` bash
  # Go from e.g. `Swarm::local_peer_id(&my_swarm)` to `my_swarm.local_peer_id()`.
  grep -RiIl --include \*.rs --exclude-dir target . --exclude-dir .git | xargs sed -i "s/\(libp2p::\)*Swarm::\([a-z_]*\)(&mut \([a-z_0-9]*\), /\3.\2(/g"
  ```
  
- Extend `NetworkBehaviour` callbacks, more concretely introducing new `fn
  inject_new_listener` and `fn inject_expired_external_addr` and have `fn
  inject_{new,expired}_listen_addr` provide a `ListenerId` [PR
  2011](https://github.com/libp2p/rust-libp2p/pull/2011).

# 0.28.0 [2021-03-17]

- New error variant `DialError::InvalidAddress`

- `Swarm::dial_addr()` now returns a `DialError` on error.

- Remove the option for a substream-specific multistream select protocol override.
  The override at this granularity is no longer deemed useful, in particular because
  it can usually not be configured for existing protocols like `libp2p-kad` and others.
  There is a `Swarm`-scoped configuration for this version available since
  [1858](https://github.com/libp2p/rust-libp2p/pull/1858).

# 0.27.2 [2021-02-04]

- Have `ToggleProtoHandler` ignore listen upgrade errors when disabled.
  [PR 1945](https://github.com/libp2p/rust-libp2p/pull/1945/files).

# 0.27.1 [2021-01-27]

- Make `OneShotHandler`s `max_dial_negotiate` limit configurable.
  [PR 1936](https://github.com/libp2p/rust-libp2p/pull/1936).

- Fix handling of DialPeerCondition::Always.
  [PR 1937](https://github.com/libp2p/rust-libp2p/pull/1937).

# 0.27.0 [2021-01-12]

- Update dependencies.

# 0.26.0 [2020-12-17]

- Update `libp2p-core`.

- Remove `NotifyHandler::All` thus removing the requirement for events send from
  a `NetworkBehaviour` to a `ProtocolsHandler` to be `Clone`.
  [PR 1880](https://github.com/libp2p/rust-libp2p/pull/1880).

# 0.25.1 [2020-11-26]

- Add `ExpandedSwarm::is_connected`.
  [PR 1862](https://github.com/libp2p/rust-libp2p/pull/1862).

# 0.25.0 [2020-11-25]

- Permit a configuration override for the substream upgrade protocol
  to use for all (outbound) substreams.
  [PR 1858](https://github.com/libp2p/rust-libp2p/pull/1858).

- Changed parameters for connection limits from `usize` to `u32`.
  Connection limits are now configured via `SwarmBuilder::connection_limits()`.

- Update `libp2p-core`.

- Expose configurable scores for external addresses, as well as
  the ability to remove them and to add addresses that are
  retained "forever" (or until explicitly removed).
  [PR 1842](https://github.com/libp2p/rust-libp2p/pull/1842).

# 0.24.0 [2020-11-09]

- Update dependencies.

# 0.23.0 [2020-10-16]

- Require a `Boxed` transport to be given to the `Swarm`
  or `SwarmBuilder` to avoid unnecessary double-boxing of
  transports and simplify API bounds.
  [PR 1794](https://github.com/libp2p/rust-libp2p/pull/1794)

- Respect inbound timeouts and upgrade versions in the `MultiHandler`.
  [PR 1786](https://github.com/libp2p/rust-libp2p/pull/1786).

- Instead of iterating each inbound and outbound substream upgrade looking for
  one to make progress, use a `FuturesUnordered` for both pending inbound and
  pending outbound upgrades. As a result only those upgrades are polled that are
  ready to progress.

  Implementors of `InboundUpgrade` and `OutboundUpgrade` need to ensure to wake
  up the underlying task once they are ready to make progress as they won't be
  polled otherwise.

  [PR 1775](https://github.com/libp2p/rust-libp2p/pull/1775)

# 0.22.0 [2020-09-09]

- Bump `libp2p-core` dependency.

- Adds `ProtocolsHandler::InboundOpenInfo` type which mirrors the existing
  `OutboundOpenInfo` type. A value of this type is passed as an extra argument
  to `ProtocolsHandler::inject_fully_negotiated_inbound` and
  `ProtocolsHandler::inject_listen_upgrade_error`.

- `SubstreamProtocol` now has a second type parameter corresponding to
  inbound or outbound information, a value of which is part of `SubstreamProtocol`
  now. Consequently `ProtocolsHandlerEvent::OutboundSubstreamRequest` no longer
  has a separate `info` field.

# 0.21.0 [2020-08-18]

- Add missing delegation calls in some `ProtocolsHandler` wrappers.
See [PR 1710](https://github.com/libp2p/rust-libp2p/pull/1710).

- Add as_ref and as_mut functions to Toggle
[PR 1684](https://github.com/libp2p/rust-libp2p/pull/1684).

- The `cause` of `SwarmEvent::ConnectionClosed` is now an `Option`,
and `None` indicates an active connection close not caused by an
error.

- `DialError::Banned` has been added and is returned from `Swarm::dial`
if the peer is banned, thereby also invoking the `NetworkBehaviour::inject_dial_failure`
callback.

- Update the `libp2p-core` dependency to `0.21`, fixing [1584](https://github.com/libp2p/rust-libp2p/issues/1584).

- Fix connections being kept alive by `OneShotHandler` when not handling any
  requests [PR 1698](https://github.com/libp2p/rust-libp2p/pull/1698).

# 0.20.1 [2020-07-08]

- Documentation updates.

- Ignore addresses returned by `NetworkBehaviour::addresses_of_peer`
that the `Swarm` considers to be listening addresses of the local node. This
avoids futile dialing attempts of a node to itself, which can otherwise
even happen in genuine situations, e.g. after the local node changed
its network identity and a behaviour makes a dialing attempt to a
former identity using the same addresses.

# 0.20.0 [2020-07-01]

- Updated the `libp2p-core` dependency.

- Add `ProtocolsHandler::inject_listen_upgrade_error`, the inbound
analogue of `ProtocolsHandler::inject_dial_upgrade_error`, with an
empty default implementation. No implementation is required to
retain existing behaviour.

- Add `ProtocolsHandler::inject_address_change` and
`NetworkBehaviour::inject_address_change` to notify of a change in
the address of an existing connection.

# 0.19.1 [2020-06-18]

- Bugfix: Fix MultiHandler panicking when empty
  ([PR 1598](https://github.com/libp2p/rust-libp2p/pull/1598)).

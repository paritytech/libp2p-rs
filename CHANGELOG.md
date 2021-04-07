# Individual crates

## Main APIs

- [`libp2p-core` CHANGELOG](core/CHANGELOG.md)
- [`libp2p-swarm` CHANGELOG](swarm/CHANGELOG.md)
- [`libp2p-swarm-derive` CHANGELOG](swarm-derive/CHANGELOG.md)

## Application Protocols

- [`libp2p-floodsub` CHANGELOG](protocols/floodsub/CHANGELOG.md)
- [`libp2p-gossipsub` CHANGELOG](protocols/gossipsub/CHANGELOG.md)
- [`libp2p-identify` CHANGELOG](protocols/identify/CHANGELOG.md)
- [`libp2p-kad` CHANGELOG](protocols/kad/CHANGELOG.md)
- [`libp2p-mdns` CHANGELOG](protocols/mdns/CHANGELOG.md)
- [`libp2p-ping` CHANGELOG](protocols/ping/CHANGELOG.md)
- [`libp2p-relay` CHANGELOG](protocols/relay/CHANGELOG.md)
- [`libp2p-request-response` CHANGELOG](protocols/request-response/CHANGELOG.md)

## Transport Protocols & Upgrades

- [`libp2p-deflate` CHANGELOG](transports/deflate/CHANGELOG.md)
- [`libp2p-dns` CHANGELOG](transports/dns/CHANGELOG.md)
- [`libp2p-noise` CHANGELOG](transports/noise/CHANGELOG.md)
- [`libp2p-plaintext` CHANGELOG](transports/plaintext/CHANGELOG.md)
- [`libp2p-pnet` CHANGELOG](transports/pnet/CHANGELOG.md)
- [`libp2p-tcp` CHANGELOG](transports/tcp/CHANGELOG.md)
- [`libp2p-uds` CHANGELOG](transports/uds/CHANGELOG.md)
- [`libp2p-wasm-ext` CHANGELOG](transports/wasm-ext/CHANGELOG.md)
- [`libp2p-websocket` CHANGELOG](transports/websocket/CHANGELOG.md)

## Multiplexers

- [`libp2p-mplex` CHANGELOG](muxers/mplex/CHANGELOG.md)
- [`libp2p-yamux` CHANGELOG](muxers/yamux/CHANGELOG.md)

## Utilities

- [`parity-multiaddr` CHANGELOG](misc/multiaddr/CHANGELOG.md)
- [`multistream-select` CHANGELOG](misc/multistream-select/CHANGELOG.md)

# `libp2p` facade crate

## Version 0.37.0 [unreleased]

- Update individual crates.
    - `libp2p-core`
    - `libp2p-dns`
    - `libp2p-floodsub`
    - `libp2p-gossipsub`
    - `libp2p-kad`
    - `libp2p-mdns`
    - `libp2p-ping`
    - `libp2p-relay`
    - `libp2p-request-response`
    - `libp2p-swarm`
    - `libp2p-wasm-ext`

## Version 0.36.0 [2021-03-17]

- Consolidate top-level utility functions for constructing development
  transports. There is now just `development_transport()` (available with default features)
  and `tokio_development_transport()` (available when the corresponding tokio features are enabled).
  Furthermore, these are now `async fn`s. The minor variations that also included `pnet`
  support have been removed.
  [PR 1927](https://github.com/libp2p/rust-libp2p/pull/1927)

- Update libp2p crates.

- Do not leak default features from libp2p crates.
  [PR 1986](https://github.com/libp2p/rust-libp2p/pull/1986).

- Add `libp2p-relay` to `libp2p` facade crate.

## Version 0.35.1 [2021-02-17]

- Update `libp2p-yamux` to latest patch version.

## Version 0.35.0 [2021-02-15]

- Use `libp2p-swarm-derive`, the former `libp2p-core-derive`.

- Update `libp2p-deflate`, `libp2p-gossipsub`, `libp2p-mdns`, `libp2p-request-response`,
  `libp2p-swarm` and `libp2p-tcp`.

## Version 0.34.0 [2021-01-12]

- Update `libp2p-core` and all dependent crates.

- The `tcp-async-std` feature is now `tcp-async-io`, still
  enabled by default.

## Version 0.33.0 [2020-12-17]

- Update `libp2p-core` and all dependent crates.

## Version 0.32.2 [2020-12-10]

- Update `libp2p-websocket`.

## Version 0.32.1 [2020-12-09]

- Update minimum patch version of `libp2p-websocket`.

## Version 0.32.0 [2020-12-08]

- Update `libp2p-request-response`.

- Update to `libp2p-mdns-0.26`.

- Update `libp2p-websocket` minimum patch version.

## Version 0.31.2 [2020-12-02]

- Bump minimum `libp2p-core` patch version.

## Version 0.31.1 [2020-11-26]

- Bump minimum `libp2p-tcp` patch version.

## Version 0.31.0 [2020-11-25]

- Update `multistream-select` and all dependent crates.

## Version 0.30.1 [2020-11-11]

- Update `libp2p-plaintext`.

## Version 0.30.0 [2020-11-09]

- Update `libp2p-mdns`, `libp2p-tcp` and `libp2p-uds` as well as `libp2p-core`
  and all its dependers.

## Version 0.29.1 [2020-10-20]

- Update `libp2p-core`.

## Version 0.29.0 [2020-10-16]

- Update `libp2p-core`, `libp2p-floodsub`, `libp2p-gossipsub`, `libp2p-mplex`,
  `libp2p-noise`, `libp2p-plaintext`, `libp2p-pnet`, `libp2p-request-response`,
  `libp2p-swarm`, `libp2p-tcp`, `libp2p-websocket` and `parity-multiaddr`.

## Version 0.28.1 [2020-09-10]

- Update to `libp2p-core` `0.22.1`.

## Version 0.28.0 [2020-09-09]

- Update `libp2p-yamux` to `0.25.0`. *Step 4 of 4 in a multi-release
  upgrade process.* See the `libp2p-yamux` CHANGELOG for details.

## Version 0.27.0 [2020-09-09]

- Update `libp2p-yamux` to `0.24.0`. *Step 3 of 4 in a multi-release
  upgrade process.* See the `libp2p-yamux` CHANGELOG for details.

## Version 0.26.0 [2020-09-09]

- Update `libp2p-yamux` to `0.23.0`. *Step 2 of 4 in a multi-release
  upgrade process.* See the `libp2p-yamux` CHANGELOG for details.

## Version 0.25.0 [2020-09-09]

- Remove the deprecated `libp2p-secio` dependency. To continue to use
  SECIO, add an explicit dependency on `libp2p-secio`. However,
  transitioning to `libp2p-noise` is strongly recommended.

- Update `libp2p-yamux` to `0.22.0`. *This version starts a multi-release
  upgrade process.* See the `libp2p-yamux` CHANGELOG for details.

- Bump `libp2p-noise` to `0.24`. See the `libp2p-noise`
changelog for details about the `LegacyConfig`.

- The `ProtocolsHandler` in `libp2p-swarm` has a new associated type
  `InboundOpenInfo` ([PR 1714]).

[PR 1714]: https://github.com/libp2p/rust-libp2p/pull/1714

## Version 0.24.0 [2020-08-18]

- Update `libp2p-core`, `libp2p-gossipsub`, `libp2p-kad`, `libp2p-mdns`,
  `libp2p-ping`, `libp2p-request-response`, `libp2p-swarm` and dependent crates.

## Version 0.23.0 (2020-08-03)

**NOTE**: For a smooth upgrade path from `0.21` to `> 0.22`
on an existing deployment, this version must not be skipped
or the provided legacy configuration for `libp2p-noise` used!

- Bump `libp2p-noise` dependency to `0.22`. See the `libp2p-noise`
changelog for details about the `LegacyConfig`.

- Refactored bandwidth logging ([PR 1670](https://github.com/libp2p/rust-libp2p/pull/1670)).

## Version 0.22.0 (2020-07-17)

**NOTE**: For a smooth upgrade path from `0.21` to `> 0.22`
on an existing deployment using `libp2p-noise`, this version
must not be skipped!

- Bump `libp2p-noise` dependency to `0.21`.

## Version 0.21.1 (2020-07-02)

- Bump `libp2p-websockets` lower bound.

## Version 0.21.0 (2020-07-01)

- Conditional compilation fixes for the `wasm32-wasi` target
  ([PR 1633](https://github.com/libp2p/rust-libp2p/pull/1633)).

- New `libp2p-request-response` crate
  ([PR 1596](https://github.com/libp2p/rust-libp2p/pull/1596)).

- Updated libp2p dependencies.

## Version 0.19.1 (2020-05-25)

- Temporarily pin all `async-std` dependencies to `< 1.6`.
  [PR 1589](https://github.com/libp2p/rust-libp2p/pull/1589)

- `libp2p-core-derive`: Fully qualified std::result::Result in macro
  [PR 1587](https://github.com/libp2p/rust-libp2p/pull/1587)

## Version 0.19.0 (2020-05-18)

- `libp2p-core`, `libp2p-swarm`: Added support for multiple dialing
  attempts per peer, with a configurable limit.
  [PR 1506](https://github.com/libp2p/rust-libp2p/pull/1506)

- `libp2p-core`: `PeerId`s that use the identity hashing will now be properly
  displayed using the string representation of an identity multihash, rather
  than the canonical SHA 256 representation.
  [PR 1576](https://github.com/libp2p/rust-libp2p/pull/1576)

- `libp2p-core`: Updated to multihash 0.11.0.
  [PR 1566](https://github.com/libp2p/rust-libp2p/pull/1566)

- `libp2p-core`: Make the number of events buffered to/from tasks configurable.
  [PR 1574](https://github.com/libp2p/rust-libp2p/pull/1574)

- `libp2p-dns`, `parity-multiaddr`: Added support for the `/dns` multiaddr
  protocol. Additionally, the `multiaddr::from_url` function will now use
  `/dns` instead of `/dns4`.
  [PR 1575](https://github.com/libp2p/rust-libp2p/pull/1575)

- `libp2p-noise`: Added the `X25519Spec` protocol suite which uses
  libp2p-noise-spec compliant signatures on static keys as well as the
  `/noise` protocol upgrade, hence providing a libp2p-noise-spec compliant
  `XX` handshake. `IK` and `IX` are still supported with `X25519Spec`
  though not guaranteed to be interoperable with other libp2p
  implementations as these handshake patterns are not currently
  included in the libp2p-noise-spec. The `X25519Spec` implementation
  will eventually replace the current `X25519` implementation, with
  the former being removed. To upgrade without interruptions, you may
  temporarily include `NoiseConfig`s for both implementations as
  alternatives in your transport upgrade pipeline.

- `libp2p-kad`: Consider fixed (K_VALUE) amount of peers at closest query
  initialization. Unless `KademliaConfig::set_replication_factor` is used change
  has no effect.
  [PR 1536](https://github.com/libp2p/rust-libp2p/pull/1536)

- `libp2p-kad`: Provide more insight into, and control of, the execution of
  queries. All query results are now wrapped in `KademliaEvent::QueryResult`.
  As a side-effect of these changes and for as long as the record storage
  API is not asynchronous, local storage errors on `put_record` are reported
  synchronously in a `Result`, instead of being reported asynchronously by
  an event.
  [PR 1567](https://github.com/libp2p/rust-libp2p/pull/1567)

- `libp2p-tcp`, `libp2p`: Made the `libp2p-tcp/async-std` feature flag
  disabled by default, and split the `libp2p/tcp` feature in two:
  `tcp-async-std` and `tcp-tokio`. `tcp-async-std` is still enabled by default.
  [PR 1471](https://github.com/libp2p/rust-libp2p/pull/1471)

- `libp2p-tcp`: On listeners started with an IPv6 multi-address the socket
  option `IPV6_V6ONLY` is set to true. Instead of relying on IPv4-mapped IPv6
  address support, two listeners can be started if IPv4 and IPv6 should both
  be supported. IPv4 listener addresses are not affected by this change.
  [PR 1555](https://github.com/libp2p/rust-libp2p/pull/1555)

## Version 0.18.1 (2020-04-17)

- `libp2p-swarm`: Make sure inject_dial_failure is called in all situations.
  [PR 1549](https://github.com/libp2p/rust-libp2p/pull/1549)

## Version 0.18.0 (2020-04-09)

- `libp2p-core`: Treat connection limit errors as pending connection errors.
  [PR 1546](https://github.com/libp2p/rust-libp2p/pull/1546)

- `libp2p-core-derive`: Disambiguate calls to `NetworkBehaviour::inject_event`.
  [PR 1543](https://github.com/libp2p/rust-libp2p/pull/1543)

- `libp2p-floodsub`: Allow sent messages seen as subscribed.
  [PR 1520](https://github.com/libp2p/rust-libp2p/pull/1520)

- `libp2p-kad`: Return peers independent of record existence.
  [PR 1544](https://github.com/libp2p/rust-libp2p/pull/1544)

- `libp2p-wasm-ext`: Fix "parsed is null" errors being thrown.
  [PR 1535](https://github.com/libp2p/rust-libp2p/pull/1535)

## Version 0.17.0 (2020-04-02)

- `libp2p-core`: Finished "identity hashing" for peer IDs migration.
  [PR 1460](https://github.com/libp2p/rust-libp2p/pull/1460)
- `libp2p-core`: Remove `poll_broadcast`.
  [PR 1527](https://github.com/libp2p/rust-libp2p/pull/1527)
- `libp2p-core`, `libp2p-swarm`: Report addresses of closed listeners.
  [PR 1485](https://github.com/libp2p/rust-libp2p/pull/1485)
- `libp2p-core`: Support for multiple connections per peer and configurable connection limits.
  See [PR #1440](https://github.com/libp2p/rust-libp2p/pull/1440),
  [PR #1519](https://github.com/libp2p/rust-libp2p/pull/1519) and
  [issue #912](https://github.com/libp2p/rust-libp2p/issues/912) for details.

- `libp2p-swarm`: Pass the cause of closing a listener to `inject_listener_closed`.
  [PR 1517](https://github.com/libp2p/rust-libp2p/pull/1517)
- `libp2p-swarm`: Support for multiple connections per peer and configurable connection limits.
  See [PR #1440](https://github.com/libp2p/rust-libp2p/pull/1440),
  [PR #1519](https://github.com/libp2p/rust-libp2p/pull/1519) and
  [issue #912](https://github.com/libp2p/rust-libp2p/issues/912) for details.
- `libp2p-swarm`: The `SwarmEvent` now returns more events.
  [PR 1515](https://github.com/libp2p/rust-libp2p/pull/1515)
- `libp2p-swarm`: New `protocols_handler::multi` module.
  [PR 1497](https://github.com/libp2p/rust-libp2p/pull/1497)
- `libp2p-swarm`: Allow configuration of outbound substreams.
  [PR 1521](https://github.com/libp2p/rust-libp2p/pull/1521)

- `libp2p-kad`: Providers returned from a lookup are now deduplicated.
  [PR 1528](https://github.com/libp2p/rust-libp2p/pull/1528)
- `libp2p-kad`: Allow customising the maximum packet size.
  [PR 1502](https://github.com/libp2p/rust-libp2p/pull/1502)
- `libp2p-kad`: Allow customising the (libp2p) connection keep-alive timeout.
  [PR 1477](https://github.com/libp2p/rust-libp2p/pull/1477)
- `libp2p-kad`: Avoid storing records that are expired upon receipt (optimisation).
  [PR 1496](https://github.com/libp2p/rust-libp2p/pull/1496)
- `libp2p-kad`: Fixed potential panic on computing record expiry.
  [PR 1492](https://github.com/libp2p/rust-libp2p/pull/1492)

- `libp2p-mplex`: Guard against use of underlying `Sink` upon
  error or connection close.
  [PR 1529](https://github.com/libp2p/rust-libp2p/pull/1529)

- `multistream-select`: Upgrade to stable futures.
  [PR 1484](https://github.com/libp2p/rust-libp2p/pull/1484)

- `multihash`: Removed the crate in favour of the upstream crate.
  [PR 1472](https://github.com/libp2p/rust-libp2p/pull/1472)

## Version 0.16.2 (2020-02-28)

- Fixed yamux connections not properly closing and being stuck in the `CLOSE_WAIT` state.
- Added a `websocket_transport()` function in `libp2p-wasm-ext`, behind a Cargo feature.
- Fixed ambiguity in `IntoProtocolsHandler::select` vs `ProtocolsHandler::select` in the `NetworkBehaviour` custom derive.

## Version 0.16.1 (2020-02-18)

- Fixed wrong representation of `PeerId`s being used in `Kademlia::get_closest_peers`.
- Implemented `FusedStream` for `Swarm`.

## Version 0.16.0 (2020-02-13)

- Removed the `Substream` associated type from the `ProtocolsHandler` trait. The type of the substream is now always `libp2p::swarm::NegotiatedSubstream`.
- As a consequence of the previous change, most of the implementations of the `NetworkBehaviour` trait provided by libp2p (`Ping`, `Identify`, `Kademlia`, `Floodsub`, `Gossipsub`) have lost a generic parameter.
- Removed the first generic parameter (the transport) from `Swarm` and `ExpandedSwarm`. The transport is now abstracted away in the internals of the swarm.
- The `Send` and `'static` bounds are now enforced directly on the `ProtocolsHandler` trait and its associated `InboundUpgrade` and `OutboundUpgrade` implementations.
- Modified `PeerId`s to compare equal across the identity and SHA256 hashes. As a consequence, the `Borrow` implementation of `PeerId` now always returns the bytes representation of a multihash with a SHA256 hash.
- Modified libp2p-floodsub to no longer hash the topic. The new behaviour is now compatible with go-libp2p and js-libp2p, but is a breaking change with regards to rust-libp2p.
- Added libp2p-pnet. It makes it possible to protect networks with a pre-shared key (PSK).
- Modified the `poll_method` parameter of the `NetworkBehaviour` custom derive. The expected method now takes an additional parameter of type `impl PollParameters` to be consistent with the `NetworkBehaviour::poll` method.
- libp2p-noise now compiles for WASM targets.
- Changed libp2p-noise to grow its memory buffers dynamically. This should reduce the overall memory usage of connections that use the noise encryption.
- Fixed libp2p-gossipsub to no longer close the connection if the inbound substream is closed by the remote.
- All crates prefixed with `libp2p-` now use the same version number.
- Added a new variant `ListenerEvent::Error` for listeners to report non-fatal errors. `libp2p-tcp` uses this variant to report errors that happen on remote sockets before they have been accepted and errors when trying to determine the local machine's IP address.

## Version 0.15.0 (2020-01-24)

- Added `libp2p-gossipsub`.
- Added `SwarmBuilder::executor` to allow configuring which tasks executor to use.
- Added `TokioTcpConfig` in `libp2p-tcp` and `TokioUdsConfig` in `libp2p-uds` behind `tokio` features. These structs use `tokio` and require a `tokio` runtime executor to be configured via `SwarmBuilder::executor`.
- Changed the `OutboundUpgrade` and `InboundUpgrade` traits to no longer be passed a `Negotiated<C>` but just a `C`. The `Negotiated` is now in the trait bounds requirements of `ProtocolsHandler`.
- Fixed `libp2p-wasm-ext` returning `Err(WouldBlock)` rather than `Pending`.
- Fixed `libp2p-dns` not segregating DNS4 and DNS6.
- Removed some unnecessary `Unpin` requirements on futures.
- Changed `Mdns::new` to no longer be `async`.
- Fixed `libp2p-kad` keeping connections alive when it shouldn't.
- Fixed `InboundUpgrade` not always properly implemented on `NoiseConfig`.

## Version 0.14.0-alpha.1 (2020-01-07)

- Upgraded the crate to stable futures.
- Use varints instead of fixed sized (4 byte) integers to delimit plaintext 2.0 messages to align implementation with the specification.
- Refactored the `core::upgrade` module to provide async functions.
- Changed the `Stream` trait implementation of `Swarm` to no longer return a `Result`.
- Added the `Swarm::next` and `Swarm::next_event` functions and the `SwarmEvent` enum.
- Changed `ProtocolsHandler::poll` to no longer return an error. Instead, `ProtocolsHandlerEvent` has a new `Close` variant which corresponds to what an error represented before.
- Changed all the traits that have a `poll` function (i.e. `NetworkBehaviour`, `ProtocolsHandler`, `NodeHandler`) to have an additional `&mut Context` parameter, to reflect the changes in the `Future` trait.
- Revamped the API of `libp2p_websockets::framed`.
- Added protocol string to `Error::UnknownProtocolString`.

## Version 0.13.2 (2020-01-02)

- Fixed the `libp2p-noise` handshake not flushing the underlying stream before waiting for a response.
- Fixed semver issue with the `protobuf` crate.

## Version 0.13.1 (2019-11-13)

- Maintenance release to bump dependencies and deal with an accidental breaking change in multihash 0.1.4.

## Version 0.13.0 (2019-11-05)

- Reworked the transport upgrade API. See https://github.com/libp2p/rust-libp2p/pull/1240 for more information.
- Added a parameter allowing to choose the protocol negotiation protocol when upgrading a connection or a substream. See https://github.com/libp2p/rust-libp2p/pull/1245 for more information.
- Added an alternative `multistream-select` protocol called `V1Lazy`.
- Added `PlainText2Config` that implements the `/plaintext/2.0.0` protocol.
- Refactored `libp2p-identify`. Some items have been renamed.
- Now accepting `PeerId`s using the `identity` hashing algorithm as valid.
- Removed `libp2p-observed` and `libp2p-ratelimit`.
- Fixed mDNS long peer IDs not being transmitted properly.
- Added some `Debug` trait implementations.
- Fixed potential arithmetic overflows in `libp2p-kad` and `multistream-select`.

## Version 0.12.0 (2019-08-15)

- In some situations, `multistream-select` will now assume that protocol negotiation immediately succeeds. If it turns out that it failed, an error is generated when reading or writing from/to the stream.
- Replaced `listen_addr` with `local_addr` in events related to incoming connections. The address no longer has to match a previously-reported address.
- Listeners now have an identifier and can be stopped.
- Added `NetworkBehaviour::inject_listener_error` and `NetworkBehaviour::inject_listener_closed`. For diagnostic purposes, listeners can now report errors on incoming connections, such as when calling `accept(2)` fails.
- Fixed tasks sometimes not being notified when a network event happens in `libp2p-mplex`.
- Fixed a memory leak in `libp2p-kad`.
- Added `Toggle::is_enabled()`.
- Removed `IdentifyTransport`.

## Version 0.11.0 (2019-07-18)

- `libp2p-kad`: Completed the core functionality of the record storage API, thereby extending the `RecordStore` for provider records. All records expire by default and are subject to regular republication and caching as per the Kademlia spec(s). Expiration and publication intervals are configurable through the `KademliaConfig`.
- `libp2p-kad`: The routing table now never stores peers without a known (listen) address. In particular, on receiving a new inbound connection, the Kademlia behaviour now emits `KademliaEvent::UnroutablePeer` to indicate that in order for the peer to be added to the routing table and hence considered a reachable node in the DHT, a listen address of the peer must be discovered and reported via `Kademlia::add_address`. This is usually achieved through the use of the `Identify` protocol on the same connection(s).
- `libp2p-kad`: Documentation updates.
- Extracted the `swarm` and `protocols_handler`-related contents from `libp2p-core` to a new `libp2p-swarm` crate.
- Renamed `RawSwarm` to `Network`.
- Added `Floodsub::publish_any`.
- Replaced unbounded channels with bounded ones at the boundary between the `Network` (formerly `RawSwarm`) and `NodeHandler`. The node handlers will now wait if the main task is busy, instead of continuing to push events to the channel.
- Fixed the `address_translation` function ignoring `/dns` addresses.

## Version 0.10.0 (2019-06-25)

- `PollParameters` is now a trait instead of a struct.
- The `Swarm` can now be customized with connection information.
- Fixed write-only substreams now delivering data properly.
- Fixed the TCP listener accidentally shutting down if an incoming socket was closed too quickly.
- Improved the heuristics for determining external multiaddresses based on reports.
- Various fixes to Kademlia iterative queries and the WebSockets transport.

## Version 0.9.1 (2019-06-05)

- `EitherOutput` now implements `Stream` and `Sink` if their variants also implement these traits.
- `libp2p::websocket::error::Error` now implements `Sync`.

## Version 0.9.0 (2019-06-04)

- Major fixes and performance improvements to libp2p-kad.
- Initial prototype for record storage in libp2p-kad.
- Rewrote the implementation of WebSockets. It now properly supports WebSockets Secure (WSS).
- Removed `BrowserWsConfig`. Please use `libp2p::wasm_ext::ExtTransport` instead.
- Added a `Path` parameter to `multiaddr::Protocol::WS` and `WSS`. The string representation when a path is present is respectively `x-parity-ws/<path>` and `x-parity-wss/<path>` where `<path>` is percent-encoded.
- Fixed an issue with `libp2p-tcp` where the wrong listened address was returned, if the actual address was loopback.
- Added `core::upgrade::OptionalUpgrade`.
- Added some utility functions in `core::identity::secp256k1`.
- It is now possible to inject an artificial connection in the `RawSwarm`.

## Version 0.8.1 (2019-05-15)

- Fixed a vulnerability in ED25519 signatures verification in libp2p-core.

## Version 0.8.0 (2019-05-15)

- Crate now successfully runs from within the browser when compiled to WASM.
- Modified the constructors of `NoiseConfig` to accept any type of public key. The Noise handshake has consequently been modified.
- Changed the `StreamMuxer` trait to have an `Error` associated type.
- The `Swarm` now ranks externally-visible multiaddresses by how often they have been reported, ensuring that weird or malicious reports don't affect connectivity too much.
- Added `IntoProtocolsHandler::inbound_protocol`. Must return the same value as what `ProtocolsHandler::listen_protocol` would return.
- `IntoProtocolsHandler::into_handler` now takes a second parameter with the `&ConnectedPoint` to the node we are connected to.
- Replaced the `secp256k1` crate with `libsecp256k1`.
- Fixed `Kademlia::add_providing` taking a `PeerId` instead of a `Multihash`.
- Fixed various bugs in the implementation of `Kademlia`.
- Added `OneSubstreamMuxer`.
- Added the `libp2p-wasm-ext` crate.
- Added `multiaddr::from_url`.
- Added `OptionalTransport`.

## Version 0.7.1 (2019-05-15)

- Fixed a vulnerability in ED25519 signatures verification in libp2p-core.

## Version 0.7.0 (2019-04-23)

- Fixed the inactive connections shutdown mechanism not working.
- `Transport::listen_on` must now return a `Stream` that produces `ListenEvent`s. This makes it possible to notify about listened addresses at a later point in time.
- `Transport::listen_on` no longer returns an address we're listening on. This is done through `ListenEvent`s. All other `listen_on` methods have been updated accordingly.
- Added `NetworkBehaviour::inject_new_listen_addr`, `NetworkBehaviour::inject_expired_listen_addr` and `NetworkBehaviour::inject_new_external_addr`.
- `ProtocolsHandler::listen_protocol` and `ProtocolsHandlerEvent::OutboundSubstreamRequest` must now return a `SubstreamProtocol` struct containing a timeout for the upgrade.
- `Ping::new` now requires a `PingConfig`, which can be created with `PingConfig::new`.
- Removed `Transport::nat_traversal` in favour of a stand-alone `address_translation` function in `libp2p-core`.
- Reworked the API of `Multiaddr`.
- Removed the `ToMultiaddr` trait in favour of `TryFrom`.
- Added `Swarm::ban_peer_id` and `Swarm::unban_peer_id`.
- The `TPeerId` generic parameter of `RawSwarm` is now `TConnInfo` and must now implement a `ConnectionInfo` trait.
- Reworked the `PingEvent`.
- Renamed `KeepAlive::Forever` to `Yes` and `KeepAlive::Now` to `No`.

## Version 0.6.0 (2019-03-29)

- Replaced `NetworkBehaviour::inject_dial_failure` with `inject_dial_failure` and
  `inject_addr_reach_failure`. The former is called when we have finished trying to dial a node
  without success, while the latter is called when we have failed to reach a specific address.
- Fixed Kademlia storing a different hash than the reference implementation.
- Lots of bugfixes in Kademlia.
- Modified the `InboundUpgrade` and `OutboundUpgrade` trait to take a `Negotiated<TSocket>` instead
  of `TSocket`.
- `PollParameters::external_addresses` now returns `Multiaddr`es as reference instead of by value.
- Added `Swarm::external_addresses`.
- Added a `core::swarm::toggle::Toggle` that allows having a disabled `NetworkBehaviour`.

## Version 0.5.0 (2019-03-13)

- Moved the `SecioKeypair` struct in `core/identity` and renamed it to `Keypair`.
- mplex now supports half-closed substreams.
- Renamed `StreamMuxer::shutdown()` to `close()`.
- Closing a muxer with the `close()` method (formerly `shutdown`) now "destroys" all the existing substreams. After `close()` as been called, they all return either EOF or an error.
- The `shutdown_substream()` method now closes only the writing side of the substream, and you can continue reading from it until EOF or until you delete it. This was actually already more or less the case before, but it wasn't properly reflected in the API or the documentation.
- `poll_inbound()` and `poll_outbound()` no longer return an `Option`, as `None` was the same as returning an error.
- Removed the `NodeClosed` events and renamed `NodeError` to `NodeClosed`. From the API's point of view, a connection now always closes with an error.
- Added the `NodeHandlerWrapperError` enum that describes an error generated by the protocols handlers grouped together. It is either `UselessTimeout` or `Handler`. This allows properly reporting closing a connection because it is useless.
- Removed `NodeHandler::inject_inbound_closed`, `NodeHandler::inject_outbound_closed`, `NodeHandler::shutdown`, and `ProtocolsHandler::shutdown`. The handler is now dropped when a shutdown process starts. This should greatly simplify writing a handler.
- `StreamMuxer::close` now implies `flush_all`.
- Removed the `Shutdown` enum from `stream_muxer`.
- Removed `ProtocolsHandler::fuse()`.
- Reworked some API of `core/nodes/node.rs` and `core/nodes/handled_node.rs`.
- The core now works even outside of a tokio context.

## Version 0.4.2 (2019-02-27)

- Fixed periodic pinging not working.

## Version 0.4.1 (2019-02-20)

- Fixed wrong version of libp2p-noise.

## Version 0.4.0 (2019-02-20)

- The `multiaddr!` macro has been moved to the `multiaddr` crate and is now reexported under the name `build_multiaddr!`.
- Modified the functions in `upgrade::transfer` to be more convenient to use.
- Now properly sending external addresses in the identify protocol.
- Fixed duplicate addresses being reported in identify and Kademlia.
- Fixed infinite looping in the functions in `upgrade::transfer`.
- Fixed infinite loop on graceful node shutdown with the `ProtocolsHandlerSelect`.
- Fixed various issues with nodes dialing each other simultaneously.
- Added the `StreamMuxer::is_remote_acknowledged()` method.
- Added a `BandwidthLogging` transport wrapper that logs the bandwidth consumption.
- The addresses to try dialing when dialing a node is now refreshed by the `Swarm` when necessary.
- Lots of modifications to the semi-private structs in `core/nodes`.
- Added `IdentifyEvent::SendBack`, when we send back our information.
- Rewrote the `MemoryTransport` to be similar to the `TcpConfig`.

## Version 0.3.1 (2019-02-02)

- Added `NetworkBehaviour::inject_replaced` that is called whenever we replace a connection with a different connection to the same peer.
- Fixed various issues with Kademlia.

## Version 0.3.0 (2019-01-30)

- Removed the `topology` module and everything it contained, including the `Topology` trait.
- Added `libp2p-noise` that supports Noise handshakes, as an alternative to `libp2p-secio`.
- Updated `ring` to version 0.14.
- Creating a `Swarm` now expects the `PeerId` of the local node, instead of a `Topology`.
- Added `NetworkBehaviour::addresses_of_peer` that returns the addresses a `NetworkBehaviour` knows about a given peer. This exists as a replacement for the topology.
- The `Kademlia` and `Mdns` behaviours now report and store the list of addresses they discover.
- You must now call `Floodsub::add_node_to_partial_view()` and `Floodsub::remove_node_from_partial_view` to add/remove nodes from the list of nodes that floodsub must send messages to.
- Added `NetworkBehaviour::inject_dial_failure` that is called when we fail to dial an address.
- `ProtocolsHandler::connection_keep_alive()` now returns a `KeepAlive` enum that provides more fine grained control.
- The `NodeHandlerWrapper` no longer has a 5 seconds inactivity timeout. This is now handled entirely by `ProtocolsHandler::connection_keep_alive()`.
- Now properly denying connections incoming from the same `PeerId` as ours.
- Added a `SwarmBuilder`. The `incoming_limit` method lets you configure the number of simultaneous incoming connections.
- Removed `FloodsubHandler`, `PingListenHandler` and `PeriodicPingHandler`.
- The structs in `core::nodes` are now generic over the `PeerId`.
- Added `SecioKeypair::ed25519_raw_key()`.
- Fix improper connection shutdown in `ProtocolsHandler`.

## Version 0.2.2 (2019-01-14)

- Fixed improper dependencies versions causing deriving `NetworkBehaviour` to generate an error.

## Version 0.2.1 (2019-01-14)

- Added the `IntoNodeHandler` and `IntoProtocolsHandler` traits, allowing node handlers and protocol handlers to know the `PeerId` of the node they are interacting with.

## Version 0.2 (2019-01-10)

- The `Transport` trait now has an `Error` associated type instead of always using `std::io::Error`.
- Merged `PeriodicPing` and `PingListen` into one `Ping` behaviour.
- `Floodsub` now generates `FloodsubEvent`s instead of direct floodsub messages.
- Added `ProtocolsHandler::connection_keep_alive`. If all the handlers return `false`, then the connection to the remote node will automatically be gracefully closed after a few seconds.
- The crate now successfuly compiles for the `wasm32-unknown-unknown` target.
- Updated `ring` to version 0.13.
- Updated `secp256k1` to version 0.12.
- The enum returned by `RawSwarm::peer()` can now return `LocalNode`. This makes it impossible to accidentally attempt to dial the local node.
- Removed `Transport::map_err_dial`.
- Removed the `Result` from some connection-related methods in the `RawSwarm`, as they could never error.
- If a node doesn't respond to pings, we now generate an error on the connection instead of trying to gracefully close it.

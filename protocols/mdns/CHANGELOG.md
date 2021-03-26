# 0.30.0 [unreleased]

- Derive `Debug` and `Clone` for `MdnsConfig`.

- Update `libp2p-swarm`.

# 0.29.0 [2021-03-17]

- Introduce `MdnsConfig` with configurable TTL of discovered peer
  records and configurable multicast query interval. The default
  query interval is increased from 20 seconds to 5 minutes, to
  significantly reduce bandwidth usage. To ensure timely peer
  discovery in the majority of cases, a multicast query is
  initiated whenever a change on a network interface is detected,
  which includes MDNS initialisation at node startup. If necessary
  the MDNS query interval can be reduced via the `MdnsConfig`.
  The `MdnsService` has been removed from the public API, making
  it compulsory that all uses occur through the `Mdns` `NetworkBehaviour`.
  An `MdnsConfig` must now be given to `Mdns::new()`.
  [PR 1977](https://github.com/libp2p/rust-libp2p/pull/1977).

- Update `libp2p-swarm`.

# 0.28.1 [2021-02-15]

- Update dependencies.

# 0.28.0 [2021-01-12]

- Update dependencies.

# 0.27.0 [2020-12-17]

- Update `libp2p-swarm` and `libp2p-core`.

# 0.26.0 [2020-12-08]

- Create multiple multicast response packets as required to avoid
  hitting the limit of 9000 bytes per MDNS packet.
  [PR 1877](https://github.com/libp2p/rust-libp2p/pull/1877).

- Detect interface changes and join the MDNS multicast
  group on all interfaces as they become available.
  [PR 1830](https://github.com/libp2p/rust-libp2p/pull/1830).

- Replace the use of macros for abstracting over `tokio`
  and `async-std` with the use of `async-io`. As a result
  there may now be an additional reactor thread running
  called `async-io` when using `tokio`, with the futures
  still being polled by the `tokio` runtime.
  [PR 1830](https://github.com/libp2p/rust-libp2p/pull/1830).

# 0.25.0 [2020-11-25]

- Update `libp2p-swarm` and `libp2p-core`.

# 0.24.0 [2020-11-09]

- Update dependencies.

# 0.23.0 [2020-10-16]

- Update `libp2p-swarm` and `libp2p-core`.

- Double receive buffer to 4KiB. [PR 1779](https://github.com/libp2p/rust-libp2p/pull/1779/files).

# 0.22.0 [2020-09-09]

- Update `libp2p-swarm` and `libp2p-core`.

# 0.21.0 [2020-08-18]

- Bump `libp2p-core` and `libp2p-swarm` dependencies.

- Allow libp2p-mdns to use either async-std or tokio to drive required UDP
  socket ([PR 1699](https://github.com/libp2p/rust-libp2p/pull/1699)).

# 0.20.0 [2020-07-01]

- Updated dependencies.

# 0.19.2 [2020-06-22]

- Updated dependencies.

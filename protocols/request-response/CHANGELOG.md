# 0.11.0 [unreleased]

- Update `libp2p-swarm`.

# 0.10.0 [2021-03-17]

- Update `libp2p-swarm`.

- Close stream even when no response has been sent.
  [PR 1987](https://github.com/libp2p/rust-libp2p/pull/1987).

- Update dependencies.

# 0.9.1 [2021-02-15]

- Make `is_pending_outbound` return true on pending connection.
  [PR 1928](https://github.com/libp2p/rust-libp2p/pull/1928).

- Update dependencies.

# 0.9.0 [2021-01-12]

- Update dependencies.

- Re-export `throttled`-specific response channel. [PR
  1902](https://github.com/libp2p/rust-libp2p/pull/1902).

# 0.8.0 [2020-12-17]

- Update `libp2p-swarm` and `libp2p-core`.

- Emit `InboundFailure::ConnectionClosed` for inbound requests that failed due
  to the underlying connection closing.
  [PR 1886](https://github.com/libp2p/rust-libp2p/pull/1886).

- Derive Clone for `InboundFailure` and `Outbound}Failure`.
  [PR 1891](https://github.com/libp2p/rust-libp2p/pull/1891)

# 0.7.0 [2020-12-08]

- Refine emitted events for inbound requests, introducing
  the `ResponseSent` event and the `ResponseOmission`
  inbound failures. This effectively removes previous
  support for one-way protocols without responses.
  [PR 1867](https://github.com/libp2p/rust-libp2p/pull/1867).

# 0.6.0 [2020-11-25]

- Update `libp2p-swarm` and `libp2p-core`.

# 0.5.0 [2020-11-09]

- Update dependencies.

# 0.4.0 [2020-10-16]

- Update dependencies.

# 0.3.0 [2020-09-09]

- Add support for opt-in request-based flow-control to any
  request-response protocol via `RequestResponse::throttled()`.
  [PR 1726](https://github.com/libp2p/rust-libp2p/pull/1726).

- Update `libp2p-swarm` and `libp2p-core`.

# 0.2.0 [2020-08-18]

- Fixed connection keep-alive, permitting connections to close due
  to inactivity.
- Bump `libp2p-core` and `libp2p-swarm` dependencies.

# 0.1.1

- Always properly `close()` the substream after sending requests and
responses in the `InboundUpgrade` and `OutboundUpgrade`. Otherwise this is
left to `RequestResponseCodec::write_request` and `RequestResponseCodec::write_response`,
which can be a pitfall and lead to subtle problems (see e.g.
https://github.com/libp2p/rust-libp2p/pull/1606).

# 0.1.0

- Initial release.

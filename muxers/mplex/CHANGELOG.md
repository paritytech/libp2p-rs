# 0.28.0 [2021-03-17]

- Update dependencies.

# 0.27.1 [2021-02-15]

- Update dependencies.

# 0.27.0 [2021-01-12]

- Update dependencies.

# 0.26.0 [2020-12-17]

- Update `libp2p-core`.

# 0.25.0 [2020-11-25]

- Update `libp2p-core`.

- Change the default `split_send_size` from 1KiB to 8KiB.
  [PR 1834](https://github.com/libp2p/rust-libp2p/pull/1834).

# 0.24.0 [2020-11-09]

- Change the default configuration to use `MaxBufferBehaviour::Block`
  and yield from waiting for the next substream or reading from a
  particular substream whenever the current read loop may have
  already filled a substream buffer, to give the current task a
  chance to read from the buffer(s) before the `MaxBufferBehaviour`
  takes effect. This is primarily relevant for
  `MaxBufferBehaviour::ResetStream`.
  [PR 1825](https://github.com/libp2p/rust-libp2p/pull/1825/).

- Tweak the naming in the `MplexConfig` API for better
  consistency with `libp2p-yamux`.
  [PR 1822](https://github.com/libp2p/rust-libp2p/pull/1822).

- Update dependencies.

# 0.23.1 [2020-10-28]

- Be lenient with duplicate `Close` frames received. Version
  `0.23.0` started treating duplicate `Close` frames for a
  substream as a protocol violation. As some libp2p implementations
  seem to occasionally send such frames and it is a harmless
  redundancy, this releases reverts back to the pre-0.23 behaviour
  of ignoring duplicate `Close` frames.

# 0.23.0 [2020-10-16]

- More granular execution of pending flushes, better logging and
  avoiding unnecessary hashing.
  [PR 1785](https://github.com/libp2p/rust-libp2p/pull/1785).

- Split receive buffers per substream.
  [PR 1784](https://github.com/libp2p/rust-libp2p/pull/1784).

- Address a potential stall when reading from substreams.

- Send a `Reset` or `Close` to the remote when a substream is dropped,
  as appropriate for the current state of the substream,
  removing that substream from the tracked open substreams,
  to avoid artificially running into substream limits.

- Change the semantics of the `max_substreams` configuration. Now,
  outbound substream attempts beyond the configured limit are delayed,
  with a task wakeup once an existing substream closes, i.e. the limit
  results in back-pressure for new outbound substreams. New inbound
  substreams beyond the limit are immediately answered with a `Reset`.
  If too many (by some internal threshold) pending frames accumulate,
  e.g. as a result of an aggressive number of inbound substreams being
  opened beyond the configured limit, the connection is closed ("DoS protection").

- Update dependencies.

# 0.22.0 [2020-09-09]

- Bump `libp2p-core` dependency.

# 0.21.0 [2020-08-18]

- Bump `libp2p-core` dependency.

# 0.20.0 [2020-07-01]

- Update `libp2p-core`, i.e. `StreamMuxer::poll_inbound` has been renamed
  to `poll_event` and returns a `StreamMuxerEvent`.

# 0.19.2 [2020-06-22]

- Deprecated method `Multiplex::is_remote_acknowledged` has been removed
  as part of [PR 1616](https://github.com/libp2p/rust-libp2p/pull/1616).

- Updated dependencies.

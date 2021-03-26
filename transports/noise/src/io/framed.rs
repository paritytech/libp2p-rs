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

//! This module provides a `Sink` and `Stream` for length-delimited
//! Noise protocol messages in form of [`NoiseFramed`].

use bytes::{Bytes, BytesMut};
use crate::{NoiseError, Protocol, PublicKey};
use crate::io::NoiseOutput;
use futures::ready;
use futures::prelude::*;
use log::{debug, trace};
use std::{fmt, io, pin::Pin, task::{Context, Poll}};

/// Max. size of a noise message.
const MAX_NOISE_MSG_LEN: usize = 65535;
/// Space given to the encryption buffer to hold key material.
const EXTRA_ENCRYPT_SPACE: usize = 1024;
/// Max. length for Noise protocol message payloads.
pub const MAX_FRAME_LEN: usize = MAX_NOISE_MSG_LEN - EXTRA_ENCRYPT_SPACE;

static_assertions::const_assert! {
    MAX_FRAME_LEN + EXTRA_ENCRYPT_SPACE <= MAX_NOISE_MSG_LEN
}

/// A `NoiseFramed` is a `Sink` and `Stream` for length-delimited
/// Noise protocol messages.
///
/// `T` is the type of the underlying I/O resource and `S` the
/// type of the Noise session state.
pub struct NoiseFramed<T, S> {
    io: T,
    session: S,
    read_state: ReadState,
    write_state: WriteState,
    read_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    decrypt_buffer: BytesMut,
}

impl<T, S> fmt::Debug for NoiseFramed<T, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NoiseFramed")
            .field("read_state", &self.read_state)
            .field("write_state", &self.write_state)
            .finish()
    }
}

impl<T> NoiseFramed<T, snow::HandshakeState> {
    /// Creates a nwe `NoiseFramed` for beginning a Noise protocol handshake.
    pub fn new(io: T, state: snow::HandshakeState) -> Self {
        NoiseFramed {
            io,
            session: state,
            read_state: ReadState::Ready,
            write_state: WriteState::Ready,
            read_buffer: Vec::new(),
            write_buffer: Vec::new(),
            decrypt_buffer: BytesMut::new(),
        }
    }

    /// Converts the `NoiseFramed` into a `NoiseOutput` encrypted data stream
    /// once the handshake is complete, including the static DH [`PublicKey`]
    /// of the remote, if received.
    ///
    /// If the underlying Noise protocol session state does not permit
    /// transitioning to transport mode because the handshake is incomplete,
    /// an error is returned. Similarly if the remote's static DH key, if
    /// present, cannot be parsed.
    pub fn into_transport<C>(self) -> Result<(Option<PublicKey<C>>, NoiseOutput<T>), NoiseError>
    where
        C: Protocol<C> + AsRef<[u8]>
    {
        let dh_remote_pubkey = match self.session.get_remote_static() {
            None => None,
            Some(k) => match C::public_from_bytes(k) {
                Err(e) => return Err(e),
                Ok(dh_pk) => Some(dh_pk)
            }
        };
        match self.session.into_transport_mode() {
            Err(e) => Err(e.into()),
            Ok(s) => {
                let io = NoiseFramed {
                    session: s,
                    io: self.io,
                    read_state: ReadState::Ready,
                    write_state: WriteState::Ready,
                    read_buffer: self.read_buffer,
                    write_buffer: self.write_buffer,
                    decrypt_buffer: self.decrypt_buffer,
                };
                Ok((dh_remote_pubkey, NoiseOutput::new(io)))
            }
        }
    }
}

/// The states for reading Noise protocol frames.
#[derive(Debug)]
enum ReadState {
    /// Ready to read another frame.
    Ready,
    /// Reading frame length.
    ReadLen { buf: [u8; 2], off: usize },
    /// Reading frame data.
    ReadData { len: usize, off: usize },
    /// EOF has been reached (terminal state).
    ///
    /// The associated result signals if the EOF was unexpected or not.
    Eof(Result<(), ()>),
    /// A decryption error occurred (terminal state).
    DecErr
}

/// The states for writing Noise protocol frames.
#[derive(Debug)]
enum WriteState {
    /// Ready to write another frame.
    Ready,
    /// Writing the frame length.
    WriteLen { len: usize, buf: [u8; 2], off: usize },
    /// Writing the frame data.
    WriteData { len: usize, off: usize },
    /// EOF has been reached unexpectedly (terminal state).
    Eof,
    /// An encryption error occurred (terminal state).
    EncErr
}

impl WriteState {
    fn is_ready(&self) -> bool {
        if let WriteState::Ready = self {
            return true
        }
        false
    }
}

impl<T, S> futures::stream::Stream for NoiseFramed<T, S>
where
    T: AsyncRead + Unpin,
    S: SessionState + Unpin
{
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = Pin::into_inner(self);
        loop {
            trace!("read state: {:?}", this.read_state);
            match this.read_state {
                ReadState::Ready => {
                    this.read_state = ReadState::ReadLen { buf: [0, 0], off: 0 };
                }
                ReadState::ReadLen { mut buf, mut off } => {
                    let n = match read_frame_len(&mut this.io, cx, &mut buf, &mut off) {
                        Poll::Ready(Ok(Some(n))) => n,
                        Poll::Ready(Ok(None)) => {
                            trace!("read: eof");
                            this.read_state = ReadState::Eof(Ok(()));
                            return Poll::Ready(None)
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Some(Err(e)))
                        }
                        Poll::Pending => {
                            this.read_state = ReadState::ReadLen { buf, off };
                            return Poll::Pending;
                        }
                    };
                    trace!("read: frame len = {}", n);
                    if n == 0 {
                        trace!("read: empty frame");
                        this.read_state = ReadState::Ready;
                        continue
                    }
                    this.read_buffer.resize(usize::from(n), 0u8);
                    this.read_state = ReadState::ReadData { len: usize::from(n), off: 0 }
                }
                ReadState::ReadData { len, ref mut off } => {
                    let n = {
                        let f = Pin::new(&mut this.io).poll_read(cx, &mut this.read_buffer[*off .. len]);
                        match ready!(f) {
                            Ok(n) => n,
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    };
                    trace!("read: {}/{} bytes", *off + n, len);
                    if n == 0 {
                        trace!("read: eof");
                        this.read_state = ReadState::Eof(Err(()));
                        return Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())))
                    }
                    *off += n;
                    if len == *off {
                        trace!("read: decrypting {} bytes", len);
                        this.decrypt_buffer.resize(len, 0);
                        if let Ok(n) = this.session.read_message(&this.read_buffer, &mut this.decrypt_buffer) {
                            this.decrypt_buffer.truncate(n);
                            trace!("read: payload len = {} bytes", n);
                            this.read_state = ReadState::Ready;
                            // Return an immutable view into the current buffer.
                            // If the view is dropped before the next frame is
                            // read, the `BytesMut` will reuse the same buffer
                            // for the next frame.
                            let view = this.decrypt_buffer.split().freeze();
                            return Poll::Ready(Some(Ok(view)))
                        } else {
                            debug!("read: decryption error");
                            this.read_state = ReadState::DecErr;
                            return Poll::Ready(Some(Err(io::ErrorKind::InvalidData.into())))
                        }
                    }
                }
                ReadState::Eof(Ok(())) => {
                    trace!("read: eof");
                    return Poll::Ready(None)
                }
                ReadState::Eof(Err(())) => {
                    trace!("read: eof (unexpected)");
                    return Poll::Ready(Some(Err(io::ErrorKind::UnexpectedEof.into())))
                }
                ReadState::DecErr => return Poll::Ready(Some(Err(io::ErrorKind::InvalidData.into())))
            }
        }
    }
}

impl<T, S> futures::sink::Sink<&Vec<u8>> for NoiseFramed<T, S>
where
    T: AsyncWrite + Unpin,
    S: SessionState + Unpin
{
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = Pin::into_inner(self);
        loop {
            trace!("write state {:?}", this.write_state);
            match this.write_state {
                WriteState::Ready => {
                    return Poll::Ready(Ok(()));
                }
                WriteState::WriteLen { len, buf, mut off } => {
                    trace!("write: frame len ({}, {:?}, {}/2)", len, buf, off);
                    match write_frame_len(&mut this.io, cx, &buf, &mut off) {
                        Poll::Ready(Ok(true)) => (),
                        Poll::Ready(Ok(false)) => {
                            trace!("write: eof");
                            this.write_state = WriteState::Eof;
                            return Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Err(e))
                        }
                        Poll::Pending => {
                            this.write_state = WriteState::WriteLen { len, buf, off };
                            return Poll::Pending
                        }
                    }
                    this.write_state = WriteState::WriteData { len, off: 0 }
                }
                WriteState::WriteData { len, ref mut off } => {
                    let n = {
                        let f = Pin::new(&mut this.io).poll_write(cx, &this.write_buffer[*off .. len]);
                        match ready!(f) {
                            Ok(n) => n,
                            Err(e) => return Poll::Ready(Err(e)),
                        }
                    };
                    if n == 0 {
                        trace!("write: eof");
                        this.write_state = WriteState::Eof;
                        return Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
                    }
                    *off += n;
                    trace!("write: {}/{} bytes written", *off, len);
                    if len == *off {
                        trace!("write: finished with {} bytes", len);
                        this.write_state = WriteState::Ready;
                    }
                }
                WriteState::Eof => {
                    trace!("write: eof");
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()))
                }
                WriteState::EncErr => return Poll::Ready(Err(io::ErrorKind::InvalidData.into()))
            }
        }
    }

    fn start_send(self: Pin<&mut Self>, frame: &Vec<u8>) -> Result<(), Self::Error> {
        assert!(frame.len() <= MAX_FRAME_LEN);
        let mut this = Pin::into_inner(self);
        assert!(this.write_state.is_ready());

        this.write_buffer.resize(frame.len() + EXTRA_ENCRYPT_SPACE, 0u8);
        match this.session.write_message(frame, &mut this.write_buffer[..]) {
            Ok(n) => {
                trace!("write: cipher text len = {} bytes", n);
                this.write_buffer.truncate(n);
                this.write_state = WriteState::WriteLen {
                    len: n,
                    buf: u16::to_be_bytes(n as u16),
                    off: 0
                };
                Ok(())
            }
            Err(e) => {
                log::error!("encryption error: {:?}", e);
                this.write_state = WriteState::EncErr;
                Err(io::ErrorKind::InvalidData.into())
            }
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_ready(cx))?;
        Pin::new(&mut self.io).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        Pin::new(&mut self.io).poll_close(cx)
    }
}

/// A stateful context in which Noise protocol messages can be read and written.
pub trait SessionState {
    fn read_message(&mut self, msg: &[u8], buf: &mut [u8]) -> Result<usize, snow::Error>;
    fn write_message(&mut self, msg: &[u8], buf: &mut [u8]) -> Result<usize, snow::Error>;
}

impl SessionState for snow::HandshakeState {
    fn read_message(&mut self, msg: &[u8], buf: &mut [u8]) -> Result<usize, snow::Error> {
        self.read_message(msg, buf)
    }

    fn write_message(&mut self, msg: &[u8], buf: &mut [u8]) -> Result<usize, snow::Error> {
        self.write_message(msg, buf)
    }
}

impl SessionState for snow::TransportState {
    fn read_message(&mut self, msg: &[u8], buf: &mut [u8]) -> Result<usize, snow::Error> {
        self.read_message(msg, buf)
    }

    fn write_message(&mut self, msg: &[u8], buf: &mut [u8]) -> Result<usize, snow::Error> {
        self.write_message(msg, buf)
    }
}

/// Read 2 bytes as frame length from the given source into the given buffer.
///
/// Panics if `off >= 2`.
///
/// When [`Poll::Pending`] is returned, the given buffer and offset
/// may have been updated (i.e. a byte may have been read) and must be preserved
/// for the next invocation.
///
/// Returns `None` if EOF has been encountered.
fn read_frame_len<R: AsyncRead + Unpin>(
    mut io: &mut R,
    cx: &mut Context<'_>,
    buf: &mut [u8; 2],
    off: &mut usize,
) -> Poll<io::Result<Option<u16>>> {
    loop {
        match ready!(Pin::new(&mut io).poll_read(cx, &mut buf[*off ..])) {
            Ok(n) => {
                if n == 0 {
                    return Poll::Ready(Ok(None));
                }
                *off += n;
                if *off == 2 {
                    return Poll::Ready(Ok(Some(u16::from_be_bytes(*buf))));
                }
            },
            Err(e) => {
                return Poll::Ready(Err(e));
            },
        }
    }
}

/// Write 2 bytes as frame length from the given buffer into the given sink.
///
/// Panics if `off >= 2`.
///
/// When [`Poll::Pending`] is returned, the given offset
/// may have been updated (i.e. a byte may have been written) and must
/// be preserved for the next invocation.
///
/// Returns `false` if EOF has been encountered.
fn write_frame_len<W: AsyncWrite + Unpin>(
    mut io: &mut W,
    cx: &mut Context<'_>,
    buf: &[u8; 2],
    off: &mut usize,
) -> Poll<io::Result<bool>> {
    loop {
        match ready!(Pin::new(&mut io).poll_write(cx, &buf[*off ..])) {
            Ok(n) => {
                if n == 0 {
                    return Poll::Ready(Ok(false))
                }
                *off += n;
                if *off == 2 {
                    return Poll::Ready(Ok(true))
                }
            }
            Err(e) => {
                return Poll::Ready(Err(e));
            }
        }
    }
}


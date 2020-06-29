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

use futures::channel::mpsc::SendError;
use io::ErrorKind;
use std::io;
use thiserror::Error;

/// An error that can be returned by libp2p-quic.
#[derive(Error, Debug)]
pub enum Error {
    /// Fatal I/O error
    #[error("Fatal I/O error {0}")]
    IO(#[from] std::io::Error),
    /// QUIC protocol error
    #[error("QUIC protocol error: {0}")]
    ConnectionError(#[from] quinn_proto::ConnectionError),
    /// Peer stopped receiving data
    #[error("Peer stopped receiving data: code {0}")]
    Stopped(quinn_proto::VarInt),
    /// Connection was prematurely closed
    #[error("Connection was prematurely closed")]
    ConnectionLost,
    /// Error making the connection.
    #[error("Connection failure: {0}")]
    ConnectError(#[from] quinn_proto::ConnectError),
    /// Cannot listen on the same endpoint more than once
    #[error("Cannot listen on the same endpoint more than once")]
    AlreadyListening,
    /// The stream was reset by the peer.
    #[error("Peer reset stream: code {0}")]
    Reset(quinn_proto::VarInt),
    /// Either an attempt was made to write to a stream that was already shut down,
    /// or a previous operation on this stream failed.
    #[error(
        "Use of a stream that has is no longer valid. This is a \
                       bug in the application."
    )]
    ExpiredStream,
    /// Reading from a stream that has not been written to.
    #[error("Reading from a stream that has not been written to.")]
    CannotReadFromUnwrittenStream,
    /// Fatal internal error or network failure
    #[error("Fatal internal error or network failure")]
    NetworkFailure,
    /// Connection already being closed
    #[error("Connection already being closed")]
    ConnectionClosing,
}

impl From<SendError> for Error {
    fn from(_: SendError) -> Error {
        Error::NetworkFailure
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::IO(e) => io::Error::new(e.kind(), Error::IO(e)),
            Error::ConnectionError(e) => e.into(),
            e @ Error::NetworkFailure
            | e @ Error::ConnectionClosing
            | e @ Error::ConnectError(_) => io::Error::new(ErrorKind::Other, e),
            e @ Error::Stopped(_) | e @ Error::Reset(_) | e @ Error::ConnectionLost => {
                io::Error::new(ErrorKind::ConnectionAborted, e)
            }
            e @ Error::ExpiredStream => io::Error::new(ErrorKind::BrokenPipe, e),
            e @ Error::AlreadyListening => io::Error::new(ErrorKind::AddrInUse, e),
            e @ Error::CannotReadFromUnwrittenStream => io::Error::new(ErrorKind::NotConnected, e),
        }
    }
}

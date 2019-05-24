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

use crate::tls;
use std::{error, fmt};

/// Error in WebSockets.
#[derive(Debug)]
pub enum Error<E> {
    /// Error in the transport layer underneath.
    Transport(E),
    /// A TLS related error.
    Tls(tls::Error),
    /// Websocket handshake error.
    Handshake(Box<dyn error::Error + Send>),
    /// Websocket base framing error.
    Base(Box<dyn error::Error + Send>)
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Transport(err) => write!(f, "{}", err),
            Error::Tls(err) => write!(f, "{}", err),
            Error::Handshake(err) => write!(f, "{}", err),
            Error::Base(err) => write!(f, "{}", err)
        }
    }
}

impl<E: error::Error + 'static> error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Transport(err) => Some(err),
            Error::Tls(err) => Some(err),
            Error::Handshake(err) => Some(&**err),
            Error::Base(err) => Some(&**err)
        }
    }
}

impl<E> From<tls::Error> for Error<E> {
    fn from(e: tls::Error) -> Self {
        Error::Tls(e)
    }
}

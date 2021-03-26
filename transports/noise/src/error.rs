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

use libp2p_core::identity;
use snow::error::Error as SnowError;
use std::{error::Error, fmt, io};

/// libp2p_noise error type.
#[derive(Debug)]
#[non_exhaustive]
pub enum NoiseError {
    /// An I/O error has been encountered.
    Io(io::Error),
    /// An noise framework error has been encountered.
    Noise(SnowError),
    /// A public key is invalid.
    InvalidKey,
    /// Authentication in a [`NoiseAuthenticated`](crate::NoiseAuthenticated)
    /// upgrade failed.
    AuthenticationFailed,
    /// A handshake payload is invalid.
    InvalidPayload(prost::DecodeError),
    /// A signature was required and could not be created.
    SigningError(identity::error::SigningError),
}

impl fmt::Display for NoiseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NoiseError::Io(e) => write!(f, "{}", e),
            NoiseError::Noise(e) => write!(f, "{}", e),
            NoiseError::InvalidKey => f.write_str("invalid public key"),
            NoiseError::InvalidPayload(e) => write!(f, "{}", e),
            NoiseError::AuthenticationFailed => f.write_str("Authentication failed"),
            NoiseError::SigningError(e) => write!(f, "{}", e),
        }
    }
}

impl Error for NoiseError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            NoiseError::Io(e) => Some(e),
            NoiseError::Noise(_) => None, // TODO: `SnowError` should implement `Error`.
            NoiseError::InvalidKey => None,
            NoiseError::AuthenticationFailed => None,
            NoiseError::InvalidPayload(e) => Some(e),
            NoiseError::SigningError(e) => Some(e),
        }
    }
}

impl From<io::Error> for NoiseError {
    fn from(e: io::Error) -> Self {
        NoiseError::Io(e)
    }
}

impl From<SnowError> for NoiseError {
    fn from(e: SnowError) -> Self {
        NoiseError::Noise(e)
    }
}

impl From<prost::DecodeError> for NoiseError {
    fn from(e: prost::DecodeError) -> Self {
        NoiseError::InvalidPayload(e)
    }
}

impl From<identity::error::SigningError> for NoiseError {
    fn from(e: identity::error::SigningError) -> Self {
        NoiseError::SigningError(e)
    }
}


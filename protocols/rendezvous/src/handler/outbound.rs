// Copyright 2021 COMIT Network.
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

use crate::codec::{Cookie, Message, NewRegistration, RendezvousCodec};
use crate::handler::Error;
use crate::substream_handler::{FutureSubstream, Next, SubstreamHandler};
use crate::{ErrorCode, Namespace, Registration, Ttl};
use asynchronous_codec::Framed;
use futures::{SinkExt, TryFutureExt, TryStreamExt};
use libp2p_swarm::NegotiatedSubstream;
use std::task::Context;
use void::Void;

pub struct Stream(FutureSubstream<OutEvent, Error>);

impl SubstreamHandler for Stream {
    type InEvent = Void;
    type OutEvent = OutEvent;
    type Error = Error;
    type OpenInfo = OpenInfo;

    fn new(substream: NegotiatedSubstream, info: Self::OpenInfo) -> Self {
        let mut stream = Framed::new(substream, RendezvousCodec::default());
        let sent_message = match info {
            OpenInfo::RegisterRequest(new_registration) => Message::Register(new_registration),
            OpenInfo::UnregisterRequest(namespace) => Message::Unregister(namespace),
            OpenInfo::DiscoverRequest {
                namespace,
                cookie,
                limit,
            } => Message::Discover {
                namespace,
                cookie,
                limit,
            },
        };

        Self(FutureSubstream::new(async move {
            use Message::*;
            use OutEvent::*;

            stream
                .send(sent_message.clone())
                .map_err(Error::WriteMessage)
                .await?;
            let received_message = stream.try_next().map_err(Error::ReadMessage).await?;
            let received_message = received_message.ok_or(Error::UnexpectedEndOfStream)?;

            let event = match (sent_message, received_message) {
                (Register(registration), RegisterResponse(Ok(ttl))) => Registered {
                    namespace: registration.namespace,
                    ttl,
                },
                (Register(registration), RegisterResponse(Err(error))) => {
                    RegisterFailed(registration.namespace, error)
                }
                (Discover { .. }, DiscoverResponse(Ok((registrations, cookie)))) => Discovered {
                    registrations,
                    cookie,
                },
                (Discover { namespace, .. }, DiscoverResponse(Err(error))) => {
                    DiscoverFailed { namespace, error }
                }
                (.., other) => return Err(Error::BadMessage(other)),
            };

            stream.close().map_err(Error::WriteMessage).await?;

            Ok(event)
        }))
    }

    fn inject_event(self, event: Self::InEvent) -> Self {
        void::unreachable(event)
    }

    fn advance(self, cx: &mut Context<'_>) -> Result<Next<Self, Self::OutEvent>, Self::Error> {
        Ok(self.0.advance(cx)?.map_state(Stream))
    }
}

#[derive(Debug, Clone)]
pub enum OutEvent {
    Registered {
        namespace: Namespace,
        ttl: Ttl,
    },
    RegisterFailed(Namespace, ErrorCode),
    Discovered {
        registrations: Vec<Registration>,
        cookie: Cookie,
    },
    DiscoverFailed {
        namespace: Option<Namespace>,
        error: ErrorCode,
    },
}

#[derive(Debug)]
pub enum OpenInfo {
    RegisterRequest(NewRegistration),
    UnregisterRequest(Namespace),
    DiscoverRequest {
        namespace: Option<Namespace>,
        cookie: Option<Cookie>,
        limit: Option<Ttl>,
    },
}

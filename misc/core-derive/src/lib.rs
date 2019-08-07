// Copyright 2018 Parity Technologies (UK) Ltd.
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

#![recursion_limit = "256"]

extern crate proc_macro;

use quote::quote;
use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput, Data, DataStruct, Ident};

/// The interface that satisfies Rust.
#[proc_macro_derive(NetworkBehaviour, attributes(behaviour))]
pub fn hello_macro_derive(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    build(&ast)
}

/// The actual implementation.
fn build(ast: &DeriveInput) -> TokenStream {
    match ast.data {
        Data::Struct(ref s) => build_struct(ast, s),
        Data::Enum(_) => unimplemented!("Deriving NetworkBehaviour is not implemented for enums"),
        Data::Union(_) => unimplemented!("Deriving NetworkBehaviour is not implemented for unions"),
    }
}

/// The version for structs
fn build_struct(ast: &DeriveInput, data_struct: &DataStruct) -> TokenStream {
    let name = &ast.ident;
    let (_, ty_generics, where_clause) = ast.generics.split_for_impl();
    let multiaddr = quote!{::libp2p::Multiaddr};
    let trait_to_impl = quote!{::libp2p::swarm::NetworkBehaviour};
    let net_behv_event_proc = quote!{::libp2p::swarm::NetworkBehaviourEventProcess};
    let either_ident = quote!{::libp2p::libp2p_core::either::EitherOutput};
    let network_behaviour_action = quote!{::libp2p::swarm::NetworkBehaviourAction};
    let into_protocols_handler = quote!{::libp2p::swarm::IntoProtocolsHandler};
    let protocols_handler = quote!{::libp2p::swarm::ProtocolsHandler};
    let into_proto_select_ident = quote!{::libp2p::swarm::IntoProtocolsHandlerSelect};
    let peer_id = quote!{::libp2p::PeerId};
    let connected_point = quote!{::libp2p::libp2p_core::ConnectedPoint};

    // Name of the type parameter that represents the substream.
    let substream_generic = {
        let mut n = "TSubstream".to_string();
        // Avoid collisions.
        while ast.generics.type_params().any(|tp| tp.ident.to_string() == n) {
            n.push('1');
        }
        let n = Ident::new(&n, name.span());
        quote!{#n}
    };

    let poll_parameters = quote!{::libp2p::swarm::PollParameters};

    // Build the generics.
    let impl_generics = {
        let tp = ast.generics.type_params();
        let lf = ast.generics.lifetimes();
        let cst = ast.generics.const_params();
        quote!{<#(#lf,)* #(#tp,)* #(#cst,)* #substream_generic>}
    };

    // Build the `where ...` clause of the trait implementation.
    let where_clause = {
        let mut additional = data_struct.fields.iter()
            .filter(|x| !is_ignored(x))
            .flat_map(|field| {
                let ty = &field.ty;
                vec![
                    quote!{#ty: #trait_to_impl},
                    quote!{Self: #net_behv_event_proc<<#ty as #trait_to_impl>::OutEvent>},
                    quote!{<<#ty as #trait_to_impl>::ProtocolsHandler as #into_protocols_handler>::Handler: #protocols_handler<Substream = #substream_generic>},
                    // Note: this bound is required because of https://github.com/rust-lang/rust/issues/55697
                    quote!{<<<#ty as #trait_to_impl>::ProtocolsHandler as #into_protocols_handler>::Handler as #protocols_handler>::InboundProtocol: ::libp2p::protocols::upgrade::InboundUpgrade<#substream_generic>},
                    quote!{<<<#ty as #trait_to_impl>::ProtocolsHandler as #into_protocols_handler>::Handler as #protocols_handler>::OutboundProtocol: ::libp2p::protocols::upgrade::OutboundUpgrade<#substream_generic>},
                ]
            })
            .collect::<Vec<_>>();

        additional.push(quote!{#substream_generic: ::libp2p::tokio_io::AsyncRead});
        additional.push(quote!{#substream_generic: ::libp2p::tokio_io::AsyncWrite});

        if let Some(where_clause) = where_clause {
            if where_clause.predicates.trailing_punct() {
                Some(quote!{#where_clause #(#additional),*})
            } else {
                Some(quote!{#where_clause, #(#additional),*})
            }
        } else {
            Some(quote!{where #(#additional),*})
        }
    };

    // The final out event.
    // If we find a `#[behaviour(out_event = "Foo")]` attribute on the struct, we set `Foo` as
    // the out event. Otherwise we use `()`.
    let out_event = {
        let mut out = quote!{()};
        for meta_items in ast.attrs.iter().filter_map(get_meta_items) {
            for meta_item in meta_items {
                match meta_item {
                    syn::NestedMeta::Meta(syn::Meta::NameValue(ref m)) if m.ident == "out_event" => {
                        if let syn::Lit::Str(ref s) = m.lit {
                            let ident: syn::Type = syn::parse_str(&s.value()).unwrap();
                            out = quote!{#ident};
                        }
                    }
                    _ => ()
                }
            }
        }
        out
    };

    // Build the list of statements to put in the body of `addresses_of_peer()`.
    let addresses_of_peer_stmts = {
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(match field.ident {
                Some(ref i) => quote!{ out.extend(self.#i.addresses_of_peer(peer_id)); },
                None => quote!{ out.extend(self.#field_n.addresses_of_peer(peer_id)); },
            })
        })
    };

    // Build the list of statements to put in the body of `inject_connected()`.
    let inject_connected_stmts = {
        let num_fields = data_struct.fields.iter().filter(|f| !is_ignored(f)).count();
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(if field_n == num_fields - 1 {
                match field.ident {
                    Some(ref i) => quote!{ self.#i.inject_connected(peer_id, endpoint); },
                    None => quote!{ self.#field_n.inject_connected(peer_id, endpoint); },
                }
            } else {
                match field.ident {
                    Some(ref i) => quote!{ self.#i.inject_connected(peer_id.clone(), endpoint.clone()); },
                    None => quote!{ self.#field_n.inject_connected(peer_id.clone(), endpoint.clone()); },
                }
            })
        })
    };

    // Build the list of statements to put in the body of `inject_disconnected()`.
    let inject_disconnected_stmts = {
        let num_fields = data_struct.fields.iter().filter(|f| !is_ignored(f)).count();
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(if field_n == num_fields - 1 {
                match field.ident {
                    Some(ref i) => quote!{ self.#i.inject_disconnected(peer_id, endpoint); },
                    None => quote!{ self.#field_n.inject_disconnected(peer_id, endpoint); },
                }
            } else {
                match field.ident {
                    Some(ref i) => quote!{ self.#i.inject_disconnected(peer_id, endpoint.clone()); },
                    None => quote!{ self.#field_n.inject_disconnected(peer_id, endpoint.clone()); },
                }
            })
        })
    };

    // Build the list of statements to put in the body of `inject_replaced()`.
    let inject_replaced_stmts = {
        let num_fields = data_struct.fields.iter().filter(|f| !is_ignored(f)).count();
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(if field_n == num_fields - 1 {
                match field.ident {
                    Some(ref i) => quote!{ self.#i.inject_replaced(peer_id, closed_endpoint, new_endpoint); },
                    None => quote!{ self.#field_n.inject_replaced(peer_id, closed_endpoint, new_endpoint); },
                }
            } else {
                match field.ident {
                    Some(ref i) => quote!{
                        self.#i.inject_replaced(peer_id.clone(), closed_endpoint.clone(), new_endpoint.clone());
                    },
                    None => quote!{
                        self.#field_n.inject_replaced(peer_id.clone(), closed_endpoint.clone(), new_endpoint.clone());
                    },
                }
            })
        })
    };

    // Build the list of statements to put in the body of `inject_addr_reach_failure()`.
    let inject_addr_reach_failure_stmts = {
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(match field.ident {
                Some(ref i) => quote!{ self.#i.inject_addr_reach_failure(peer_id, addr, error); },
                None => quote!{ self.#field_n.inject_addr_reach_failure(peer_id, addr, error); },
            })
        })
    };

    // Build the list of statements to put in the body of `inject_dial_failure()`.
    let inject_dial_failure_stmts = {
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(match field.ident {
                Some(ref i) => quote!{ self.#i.inject_dial_failure(peer_id); },
                None => quote!{ self.#field_n.inject_dial_failure(peer_id); },
            })
        })
    };

    // Build the list of statements to put in the body of `inject_new_listen_addr()`.
    let inject_new_listen_addr_stmts = {
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(match field.ident {
                Some(ref i) => quote!{ self.#i.inject_new_listen_addr(addr); },
                None => quote!{ self.#field_n.inject_new_listen_addr(addr); },
            })
        })
    };

    // Build the list of statements to put in the body of `inject_expired_listen_addr()`.
    let inject_expired_listen_addr_stmts = {
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(match field.ident {
                Some(ref i) => quote!{ self.#i.inject_expired_listen_addr(addr); },
                None => quote!{ self.#field_n.inject_expired_listen_addr(addr); },
            })
        })
    };

    // Build the list of statements to put in the body of `inject_new_external_addr()`.
    let inject_new_external_addr_stmts = {
        data_struct.fields.iter().enumerate().filter_map(move |(field_n, field)| {
            if is_ignored(&field) {
                return None;
            }

            Some(match field.ident {
                Some(ref i) => quote!{ self.#i.inject_new_external_addr(addr); },
                None => quote!{ self.#field_n.inject_new_external_addr(addr); },
            })
        })
    };

    // Build the list of variants to put in the body of `inject_node_event()`.
    //
    // The event type is a construction of nested `#either_ident`s of the events of the children.
    // We call `inject_node_event` on the corresponding child.
    let inject_node_event_stmts = data_struct.fields.iter().enumerate().filter(|f| !is_ignored(&f.1)).enumerate().map(|(enum_n, (field_n, field))| {
        let mut elem = if enum_n != 0 {
            quote!{ #either_ident::Second(ev) }
        } else {
            quote!{ ev }
        };

        for _ in 0 .. data_struct.fields.iter().filter(|f| !is_ignored(f)).count() - 1 - field_n {
            elem = quote!{ #either_ident::First(#elem) };
        }

        Some(match field.ident {
            Some(ref i) => quote!{ #elem => self.#i.inject_node_event(peer_id, ev) },
            None => quote!{ #elem => self.#field_n.inject_node_event(peer_id, ev) },
        })
    });

    // The `ProtocolsHandler` associated type.
    let protocols_handler_ty = {
        let mut ph_ty = None;
        for field in data_struct.fields.iter() {
            if is_ignored(&field) {
                continue;
            }
            let ty = &field.ty;
            let field_info = quote!{ <#ty as #trait_to_impl>::ProtocolsHandler };
            match ph_ty {
                Some(ev) => ph_ty = Some(quote!{ #into_proto_select_ident<#ev, #field_info> }),
                ref mut ev @ None => *ev = Some(field_info),
            }
        }
        ph_ty.unwrap_or(quote!{()})     // TODO: `!` instead
    };

    // The content of `new_handler()`.
    // Example output: `self.field1.select(self.field2.select(self.field3))`.
    let new_handler = {
        let mut out_handler = None;

        for (field_n, field) in data_struct.fields.iter().enumerate() {
            if is_ignored(&field) {
                continue;
            }

            let field_name = match field.ident {
                Some(ref i) => quote!{ self.#i },
                None => quote!{ self.#field_n },
            };

            let builder = quote! {
                #field_name.new_handler()
            };

            match out_handler {
                Some(h) => out_handler = Some(quote!{ #h.select(#builder) }),
                ref mut h @ None => *h = Some(builder),
            }
        }

        out_handler.unwrap_or(quote!{()})     // TODO: incorrect
    };

    // The method to use to poll.
    // If we find a `#[behaviour(poll_method = "poll")]` attribute on the struct, we call
    // `self.poll()` at the end of the polling.
    let poll_method = {
        let mut poll_method = quote!{Async::NotReady};
        for meta_items in ast.attrs.iter().filter_map(get_meta_items) {
            for meta_item in meta_items {
                match meta_item {
                    syn::NestedMeta::Meta(syn::Meta::NameValue(ref m)) if m.ident == "poll_method" => {
                        if let syn::Lit::Str(ref s) = m.lit {
                            let ident: Ident = syn::parse_str(&s.value()).unwrap();
                            poll_method = quote!{#name::#ident(self)};
                        }
                    }
                    _ => ()
                }
            }
        }
        poll_method
    };

    // List of statements to put in `poll()`.
    //
    // We poll each child one by one and wrap around the output.
    let poll_stmts = data_struct.fields.iter().enumerate().filter(|f| !is_ignored(&f.1)).enumerate().map(|(enum_n, (field_n, field))| {
        let field_name = match field.ident {
            Some(ref i) => quote!{ self.#i },
            None => quote!{ self.#field_n },
        };

        let mut wrapped_event = if enum_n != 0 {
            quote!{ #either_ident::Second(event) }
        } else {
            quote!{ event }
        };
        for _ in 0 .. data_struct.fields.iter().filter(|f| !is_ignored(f)).count() - 1 - field_n {
            wrapped_event = quote!{ #either_ident::First(#wrapped_event) };
        }

        Some(quote!{
            loop {
                match #field_name.poll(poll_params) {
                    Async::Ready(#network_behaviour_action::GenerateEvent(event)) => {
                        #net_behv_event_proc::inject_event(self, event)
                    }
                    Async::Ready(#network_behaviour_action::DialAddress { address }) => {
                        return Async::Ready(#network_behaviour_action::DialAddress { address });
                    }
                    Async::Ready(#network_behaviour_action::DialPeer { peer_id }) => {
                        return Async::Ready(#network_behaviour_action::DialPeer { peer_id });
                    }
                    Async::Ready(#network_behaviour_action::SendEvent { peer_id, event }) => {
                        return Async::Ready(#network_behaviour_action::SendEvent {
                            peer_id,
                            event: #wrapped_event,
                        });
                    }
                    Async::Ready(#network_behaviour_action::ReportObservedAddr { address }) => {
                        return Async::Ready(#network_behaviour_action::ReportObservedAddr { address });
                    }
                    Async::NotReady => break,
                }
            }
        })
    });

    // Now the magic happens.
    let final_quote = quote!{
        impl #impl_generics #trait_to_impl for #name #ty_generics
        #where_clause
        {
            type ProtocolsHandler = #protocols_handler_ty;
            type OutEvent = #out_event;

            fn new_handler(&mut self) -> Self::ProtocolsHandler {
                use #into_protocols_handler;
                #new_handler
            }

            fn addresses_of_peer(&mut self, peer_id: &#peer_id) -> Vec<#multiaddr> {
                let mut out = Vec::new();
                #(#addresses_of_peer_stmts);*
                out
            }

            fn inject_connected(&mut self, peer_id: #peer_id, endpoint: #connected_point) {
                #(#inject_connected_stmts);*
            }

            fn inject_disconnected(&mut self, peer_id: &#peer_id, endpoint: #connected_point) {
                #(#inject_disconnected_stmts);*
            }

            fn inject_replaced(&mut self, peer_id: #peer_id, closed_endpoint: #connected_point, new_endpoint: #connected_point) {
                #(#inject_replaced_stmts);*
            }

            fn inject_addr_reach_failure(&mut self, peer_id: Option<&#peer_id>, addr: &#multiaddr, error: &dyn std::error::Error) {
                #(#inject_addr_reach_failure_stmts);*
            }

            fn inject_dial_failure(&mut self, peer_id: &#peer_id) {
                #(#inject_dial_failure_stmts);*
            }

            fn inject_new_listen_addr(&mut self, addr: &#multiaddr) {
                #(#inject_new_listen_addr_stmts);*
            }

            fn inject_expired_listen_addr(&mut self, addr: &#multiaddr) {
                #(#inject_expired_listen_addr_stmts);*
            }

            fn inject_new_external_addr(&mut self, addr: &#multiaddr) {
                #(#inject_new_external_addr_stmts);*
            }

            fn inject_node_event(
                &mut self,
                peer_id: #peer_id,
                event: <<Self::ProtocolsHandler as #into_protocols_handler>::Handler as #protocols_handler>::OutEvent
            ) {
                match event {
                    #(#inject_node_event_stmts),*
                }
            }

            fn poll(&mut self, poll_params: &mut impl #poll_parameters) -> ::libp2p::futures::Async<#network_behaviour_action<<<Self::ProtocolsHandler as #into_protocols_handler>::Handler as #protocols_handler>::InEvent, Self::OutEvent>> {
                use libp2p::futures::prelude::*;
                #(#poll_stmts)*
                let f: ::libp2p::futures::Async<#network_behaviour_action<<<Self::ProtocolsHandler as #into_protocols_handler>::Handler as #protocols_handler>::InEvent, Self::OutEvent>> = #poll_method;
                f
            }
        }
    };

    final_quote.into()
}

fn get_meta_items(attr: &syn::Attribute) -> Option<Vec<syn::NestedMeta>> {
    if attr.path.segments.len() == 1 && attr.path.segments[0].ident == "behaviour" {
        match attr.interpret_meta() {
            Some(syn::Meta::List(ref meta)) => Some(meta.nested.iter().cloned().collect()),
            _ => {
                None
            }
        }
    } else {
        None
    }
}

/// Returns true if a field is marked as ignored by the user.
fn is_ignored(field: &syn::Field) -> bool {
    for meta_items in field.attrs.iter().filter_map(get_meta_items) {
        for meta_item in meta_items {
            match meta_item {
                syn::NestedMeta::Meta(syn::Meta::Word(ref m)) if m == "ignore" => {
                    return true;
                }
                _ => ()
            }
        }
    }

    false
}

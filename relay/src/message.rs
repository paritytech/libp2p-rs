// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(PartialEq,Clone,Default)]
pub struct CircuitRelay {
    // message fields
    field_type: ::std::option::Option<CircuitRelay_Type>,
    srcPeer: ::protobuf::SingularPtrField<CircuitRelay_Peer>,
    dstPeer: ::protobuf::SingularPtrField<CircuitRelay_Peer>,
    code: ::std::option::Option<CircuitRelay_Status>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CircuitRelay {}

impl CircuitRelay {
    pub fn new() -> CircuitRelay {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CircuitRelay {
        static mut instance: ::protobuf::lazy::Lazy<CircuitRelay> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CircuitRelay,
        };
        unsafe {
            instance.get(CircuitRelay::new)
        }
    }

    // optional .CircuitRelay.Type type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: CircuitRelay_Type) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type(&self) -> CircuitRelay_Type {
        self.field_type.unwrap_or(CircuitRelay_Type::HOP)
    }

    fn get_field_type_for_reflect(&self) -> &::std::option::Option<CircuitRelay_Type> {
        &self.field_type
    }

    fn mut_field_type_for_reflect(&mut self) -> &mut ::std::option::Option<CircuitRelay_Type> {
        &mut self.field_type
    }

    // optional .CircuitRelay.Peer srcPeer = 2;

    pub fn clear_srcPeer(&mut self) {
        self.srcPeer.clear();
    }

    pub fn has_srcPeer(&self) -> bool {
        self.srcPeer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_srcPeer(&mut self, v: CircuitRelay_Peer) {
        self.srcPeer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_srcPeer(&mut self) -> &mut CircuitRelay_Peer {
        if self.srcPeer.is_none() {
            self.srcPeer.set_default();
        }
        self.srcPeer.as_mut().unwrap()
    }

    // Take field
    pub fn take_srcPeer(&mut self) -> CircuitRelay_Peer {
        self.srcPeer.take().unwrap_or_else(|| CircuitRelay_Peer::new())
    }

    pub fn get_srcPeer(&self) -> &CircuitRelay_Peer {
        self.srcPeer.as_ref().unwrap_or_else(|| CircuitRelay_Peer::default_instance())
    }

    fn get_srcPeer_for_reflect(&self) -> &::protobuf::SingularPtrField<CircuitRelay_Peer> {
        &self.srcPeer
    }

    fn mut_srcPeer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<CircuitRelay_Peer> {
        &mut self.srcPeer
    }

    // optional .CircuitRelay.Peer dstPeer = 3;

    pub fn clear_dstPeer(&mut self) {
        self.dstPeer.clear();
    }

    pub fn has_dstPeer(&self) -> bool {
        self.dstPeer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_dstPeer(&mut self, v: CircuitRelay_Peer) {
        self.dstPeer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_dstPeer(&mut self) -> &mut CircuitRelay_Peer {
        if self.dstPeer.is_none() {
            self.dstPeer.set_default();
        }
        self.dstPeer.as_mut().unwrap()
    }

    // Take field
    pub fn take_dstPeer(&mut self) -> CircuitRelay_Peer {
        self.dstPeer.take().unwrap_or_else(|| CircuitRelay_Peer::new())
    }

    pub fn get_dstPeer(&self) -> &CircuitRelay_Peer {
        self.dstPeer.as_ref().unwrap_or_else(|| CircuitRelay_Peer::default_instance())
    }

    fn get_dstPeer_for_reflect(&self) -> &::protobuf::SingularPtrField<CircuitRelay_Peer> {
        &self.dstPeer
    }

    fn mut_dstPeer_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<CircuitRelay_Peer> {
        &mut self.dstPeer
    }

    // optional .CircuitRelay.Status code = 4;

    pub fn clear_code(&mut self) {
        self.code = ::std::option::Option::None;
    }

    pub fn has_code(&self) -> bool {
        self.code.is_some()
    }

    // Param is passed by value, moved
    pub fn set_code(&mut self, v: CircuitRelay_Status) {
        self.code = ::std::option::Option::Some(v);
    }

    pub fn get_code(&self) -> CircuitRelay_Status {
        self.code.unwrap_or(CircuitRelay_Status::SUCCESS)
    }

    fn get_code_for_reflect(&self) -> &::std::option::Option<CircuitRelay_Status> {
        &self.code
    }

    fn mut_code_for_reflect(&mut self) -> &mut ::std::option::Option<CircuitRelay_Status> {
        &mut self.code
    }
}

impl ::protobuf::Message for CircuitRelay {
    fn is_initialized(&self) -> bool {
        for v in &self.srcPeer {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.dstPeer {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_proto2_enum_with_unknown_fields_into(wire_type, is, &mut self.field_type, 1, &mut self.unknown_fields)?
                },
                2 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.srcPeer)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.dstPeer)?;
                },
                4 => {
                    ::protobuf::rt::read_proto2_enum_with_unknown_fields_into(wire_type, is, &mut self.code, 4, &mut self.unknown_fields)?
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.field_type {
            my_size += ::protobuf::rt::enum_size(1, v);
        }
        if let Some(ref v) = self.srcPeer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(ref v) = self.dstPeer.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        if let Some(v) = self.code {
            my_size += ::protobuf::rt::enum_size(4, v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            os.write_enum(1, v.value())?;
        }
        if let Some(ref v) = self.srcPeer.as_ref() {
            os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(ref v) = self.dstPeer.as_ref() {
            os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        if let Some(v) = self.code {
            os.write_enum(4, v.value())?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CircuitRelay {
    fn new() -> CircuitRelay {
        CircuitRelay::new()
    }

    fn descriptor_static(_: ::std::option::Option<CircuitRelay>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<CircuitRelay_Type>>(
                    "type",
                    CircuitRelay::get_field_type_for_reflect,
                    CircuitRelay::mut_field_type_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CircuitRelay_Peer>>(
                    "srcPeer",
                    CircuitRelay::get_srcPeer_for_reflect,
                    CircuitRelay::mut_srcPeer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<CircuitRelay_Peer>>(
                    "dstPeer",
                    CircuitRelay::get_dstPeer_for_reflect,
                    CircuitRelay::mut_dstPeer_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<CircuitRelay_Status>>(
                    "code",
                    CircuitRelay::get_code_for_reflect,
                    CircuitRelay::mut_code_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CircuitRelay>(
                    "CircuitRelay",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CircuitRelay {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_srcPeer();
        self.clear_dstPeer();
        self.clear_code();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for CircuitRelay {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for CircuitRelay {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct CircuitRelay_Peer {
    // message fields
    id: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    addrs: ::protobuf::RepeatedField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for CircuitRelay_Peer {}

impl CircuitRelay_Peer {
    pub fn new() -> CircuitRelay_Peer {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CircuitRelay_Peer {
        static mut instance: ::protobuf::lazy::Lazy<CircuitRelay_Peer> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CircuitRelay_Peer,
        };
        unsafe {
            instance.get(CircuitRelay_Peer::new)
        }
    }

    // required bytes id = 1;

    pub fn clear_id(&mut self) {
        self.id.clear();
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: ::std::vec::Vec<u8>) {
        self.id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_id(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.id.is_none() {
            self.id.set_default();
        }
        self.id.as_mut().unwrap()
    }

    // Take field
    pub fn take_id(&mut self) -> ::std::vec::Vec<u8> {
        self.id.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_id(&self) -> &[u8] {
        match self.id.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_id_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.id
    }

    // repeated bytes addrs = 2;

    pub fn clear_addrs(&mut self) {
        self.addrs.clear();
    }

    // Param is passed by value, moved
    pub fn set_addrs(&mut self, v: ::protobuf::RepeatedField<::std::vec::Vec<u8>>) {
        self.addrs = v;
    }

    // Mutable pointer to the field.
    pub fn mut_addrs(&mut self) -> &mut ::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        &mut self.addrs
    }

    // Take field
    pub fn take_addrs(&mut self) -> ::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        ::std::mem::replace(&mut self.addrs, ::protobuf::RepeatedField::new())
    }

    pub fn get_addrs(&self) -> &[::std::vec::Vec<u8>] {
        &self.addrs
    }

    fn get_addrs_for_reflect(&self) -> &::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        &self.addrs
    }

    fn mut_addrs_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<::std::vec::Vec<u8>> {
        &mut self.addrs
    }
}

impl ::protobuf::Message for CircuitRelay_Peer {
    fn is_initialized(&self) -> bool {
        if self.id.is_none() {
            return false;
        }
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.id)?;
                },
                2 => {
                    ::protobuf::rt::read_repeated_bytes_into(wire_type, is, &mut self.addrs)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.id.as_ref() {
            my_size += ::protobuf::rt::bytes_size(1, &v);
        }
        for value in &self.addrs {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.id.as_ref() {
            os.write_bytes(1, &v)?;
        }
        for v in &self.addrs {
            os.write_bytes(2, &v)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CircuitRelay_Peer {
    fn new() -> CircuitRelay_Peer {
        CircuitRelay_Peer::new()
    }

    fn descriptor_static(_: ::std::option::Option<CircuitRelay_Peer>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "id",
                    CircuitRelay_Peer::get_id_for_reflect,
                    CircuitRelay_Peer::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "addrs",
                    CircuitRelay_Peer::get_addrs_for_reflect,
                    CircuitRelay_Peer::mut_addrs_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CircuitRelay_Peer>(
                    "CircuitRelay_Peer",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CircuitRelay_Peer {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_addrs();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for CircuitRelay_Peer {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for CircuitRelay_Peer {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum CircuitRelay_Status {
    SUCCESS = 100,
    HOP_SRC_ADDR_TOO_LONG = 220,
    HOP_DST_ADDR_TOO_LONG = 221,
    HOP_SRC_MULTIADDR_INVALID = 250,
    HOP_DST_MULTIADDR_INVALID = 251,
    HOP_NO_CONN_TO_DST = 260,
    HOP_CANT_DIAL_DST = 261,
    HOP_CANT_OPEN_DST_STREAM = 262,
    HOP_CANT_SPEAK_RELAY = 270,
    HOP_CANT_RELAY_TO_SELF = 280,
    STOP_SRC_ADDR_TOO_LONG = 320,
    STOP_DST_ADDR_TOO_LONG = 321,
    STOP_SRC_MULTIADDR_INVALID = 350,
    STOP_DST_MULTIADDR_INVALID = 351,
    STOP_RELAY_REFUSED = 390,
    MALFORMED_MESSAGE = 400,
}

impl ::protobuf::ProtobufEnum for CircuitRelay_Status {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<CircuitRelay_Status> {
        match value {
            100 => ::std::option::Option::Some(CircuitRelay_Status::SUCCESS),
            220 => ::std::option::Option::Some(CircuitRelay_Status::HOP_SRC_ADDR_TOO_LONG),
            221 => ::std::option::Option::Some(CircuitRelay_Status::HOP_DST_ADDR_TOO_LONG),
            250 => ::std::option::Option::Some(CircuitRelay_Status::HOP_SRC_MULTIADDR_INVALID),
            251 => ::std::option::Option::Some(CircuitRelay_Status::HOP_DST_MULTIADDR_INVALID),
            260 => ::std::option::Option::Some(CircuitRelay_Status::HOP_NO_CONN_TO_DST),
            261 => ::std::option::Option::Some(CircuitRelay_Status::HOP_CANT_DIAL_DST),
            262 => ::std::option::Option::Some(CircuitRelay_Status::HOP_CANT_OPEN_DST_STREAM),
            270 => ::std::option::Option::Some(CircuitRelay_Status::HOP_CANT_SPEAK_RELAY),
            280 => ::std::option::Option::Some(CircuitRelay_Status::HOP_CANT_RELAY_TO_SELF),
            320 => ::std::option::Option::Some(CircuitRelay_Status::STOP_SRC_ADDR_TOO_LONG),
            321 => ::std::option::Option::Some(CircuitRelay_Status::STOP_DST_ADDR_TOO_LONG),
            350 => ::std::option::Option::Some(CircuitRelay_Status::STOP_SRC_MULTIADDR_INVALID),
            351 => ::std::option::Option::Some(CircuitRelay_Status::STOP_DST_MULTIADDR_INVALID),
            390 => ::std::option::Option::Some(CircuitRelay_Status::STOP_RELAY_REFUSED),
            400 => ::std::option::Option::Some(CircuitRelay_Status::MALFORMED_MESSAGE),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [CircuitRelay_Status] = &[
            CircuitRelay_Status::SUCCESS,
            CircuitRelay_Status::HOP_SRC_ADDR_TOO_LONG,
            CircuitRelay_Status::HOP_DST_ADDR_TOO_LONG,
            CircuitRelay_Status::HOP_SRC_MULTIADDR_INVALID,
            CircuitRelay_Status::HOP_DST_MULTIADDR_INVALID,
            CircuitRelay_Status::HOP_NO_CONN_TO_DST,
            CircuitRelay_Status::HOP_CANT_DIAL_DST,
            CircuitRelay_Status::HOP_CANT_OPEN_DST_STREAM,
            CircuitRelay_Status::HOP_CANT_SPEAK_RELAY,
            CircuitRelay_Status::HOP_CANT_RELAY_TO_SELF,
            CircuitRelay_Status::STOP_SRC_ADDR_TOO_LONG,
            CircuitRelay_Status::STOP_DST_ADDR_TOO_LONG,
            CircuitRelay_Status::STOP_SRC_MULTIADDR_INVALID,
            CircuitRelay_Status::STOP_DST_MULTIADDR_INVALID,
            CircuitRelay_Status::STOP_RELAY_REFUSED,
            CircuitRelay_Status::MALFORMED_MESSAGE,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<CircuitRelay_Status>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("CircuitRelay_Status", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for CircuitRelay_Status {
}

impl ::protobuf::reflect::ProtobufValue for CircuitRelay_Status {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum CircuitRelay_Type {
    HOP = 1,
    STOP = 2,
    STATUS = 3,
    CAN_HOP = 4,
}

impl ::protobuf::ProtobufEnum for CircuitRelay_Type {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<CircuitRelay_Type> {
        match value {
            1 => ::std::option::Option::Some(CircuitRelay_Type::HOP),
            2 => ::std::option::Option::Some(CircuitRelay_Type::STOP),
            3 => ::std::option::Option::Some(CircuitRelay_Type::STATUS),
            4 => ::std::option::Option::Some(CircuitRelay_Type::CAN_HOP),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [CircuitRelay_Type] = &[
            CircuitRelay_Type::HOP,
            CircuitRelay_Type::STOP,
            CircuitRelay_Type::STATUS,
            CircuitRelay_Type::CAN_HOP,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<CircuitRelay_Type>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("CircuitRelay_Type", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for CircuitRelay_Type {
}

impl ::protobuf::reflect::ProtobufValue for CircuitRelay_Type {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x11src/message.proto\"\xe3\x05\n\x0cCircuitRelay\x12&\n\x04type\x18\
    \x01\x20\x01(\x0e2\x12.CircuitRelay.TypeR\x04type\x12,\n\x07srcPeer\x18\
    \x02\x20\x01(\x0b2\x12.CircuitRelay.PeerR\x07srcPeer\x12,\n\x07dstPeer\
    \x18\x03\x20\x01(\x0b2\x12.CircuitRelay.PeerR\x07dstPeer\x12(\n\x04code\
    \x18\x04\x20\x01(\x0e2\x14.CircuitRelay.StatusR\x04code\x1a,\n\x04Peer\
    \x12\x0e\n\x02id\x18\x01\x20\x02(\x0cR\x02id\x12\x14\n\x05addrs\x18\x02\
    \x20\x03(\x0cR\x05addrs\"\xc2\x03\n\x06Status\x12\x0b\n\x07SUCCESS\x10d\
    \x12\x1a\n\x15HOP_SRC_ADDR_TOO_LONG\x10\xdc\x01\x12\x1a\n\x15HOP_DST_ADD\
    R_TOO_LONG\x10\xdd\x01\x12\x1e\n\x19HOP_SRC_MULTIADDR_INVALID\x10\xfa\
    \x01\x12\x1e\n\x19HOP_DST_MULTIADDR_INVALID\x10\xfb\x01\x12\x17\n\x12HOP\
    _NO_CONN_TO_DST\x10\x84\x02\x12\x16\n\x11HOP_CANT_DIAL_DST\x10\x85\x02\
    \x12\x1d\n\x18HOP_CANT_OPEN_DST_STREAM\x10\x86\x02\x12\x19\n\x14HOP_CANT\
    _SPEAK_RELAY\x10\x8e\x02\x12\x1b\n\x16HOP_CANT_RELAY_TO_SELF\x10\x98\x02\
    \x12\x1b\n\x16STOP_SRC_ADDR_TOO_LONG\x10\xc0\x02\x12\x1b\n\x16STOP_DST_A\
    DDR_TOO_LONG\x10\xc1\x02\x12\x1f\n\x1aSTOP_SRC_MULTIADDR_INVALID\x10\xde\
    \x02\x12\x1f\n\x1aSTOP_DST_MULTIADDR_INVALID\x10\xdf\x02\x12\x17\n\x12ST\
    OP_RELAY_REFUSED\x10\x86\x03\x12\x16\n\x11MALFORMED_MESSAGE\x10\x90\x03\
    \"2\n\x04Type\x12\x07\n\x03HOP\x10\x01\x12\x08\n\x04STOP\x10\x02\x12\n\n\
    \x06STATUS\x10\x03\x12\x0b\n\x07CAN_HOP\x10\x04\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}

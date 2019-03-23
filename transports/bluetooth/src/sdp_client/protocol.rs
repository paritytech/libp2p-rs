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

use byteorder::{ByteOrder, BigEndian, WriteBytesExt as _};
use bytes::BufMut;
use std::{cmp, io, io::Write as _, ops::RangeInclusive};

/// An SDP packet.
pub struct Packet {
    /// Identifier for the transaction.
    pub transaction_id: u16,
    /// Enum with all the possible packets.
    pub packet_ty: PacketTy,
}

/// Inner packet type.
pub enum PacketTy {
    /// Error information returned by the server.
    ErrorResponse(ErrorResponseCode),
    ServiceSearchRequest {
        service_search_pattern: Vec<Uuid>,
        maximum_service_record_count: u16,
        continuation_state: ContinuationState,
    },
    ServiceSearchResponse {
        total_service_record_count: u16,
        service_record_handle_list: Vec<u32>,
        continuation_state: ContinuationState,
    },
    ServiceAttributeRequest,
    ServiceAttributeResponse,
    ServiceSearchAttributeRequest {
        service_search_pattern: Vec<Uuid>,
        maximum_attribute_byte_count: u16,
        attribute_id_list: Vec<AttributeSearch>,
        continuation_state: ContinuationState,
    },
    ServiceSearchAttributeResponse {
        attribute_lists: Vec<Vec<(u16, Data)>>,
        continuation_state: ContinuationState,
    },
}

#[derive(Debug, Clone)]
pub struct ContinuationState(Vec<u8>);

impl ContinuationState {
    /// Builds an initial continuation state.
    pub fn zero() -> Self {
        ContinuationState(vec![0])
    }

    /// Returns true if the continuation state is zero, meaning that there's no continuation.
    pub fn is_zero(&self) -> bool {
        self.0 == &[0]
    }
}

/// List of attributes to search.
pub enum AttributeSearch {
    /// Single attribute.
    Attribute(u16),
    /// Range of attributes.
    Range(RangeInclusive<u16>),
}

/// Codec for the SDP protocol.
#[derive(Debug, Clone, Default)]
pub struct Codec {}

impl tokio_codec::Decoder for Codec {
    type Item = Packet;
    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 5 {
            return Ok(None);
        }

        let params_len = BigEndian::read_u16(&src[3..5]) as usize;
        let total_len = 5 + params_len;
        if src.len() < total_len {
            return Ok(None);
        }

        let packet = src.split_to(total_len);

        let packet_ty = PacketTy::from_parameters(packet[0], &packet[5..])?;
        let transaction_id = BigEndian::read_u16(&packet[1..3]);

        Ok(Some(Packet {
            packet_ty,
            transaction_id,
        }))
    }
}

impl tokio_codec::Encoder for Codec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        let (pdu_id, parameters) = item.packet_ty.into_id_params();

        if parameters.len() >= u16::max_value() as usize {
            panic!()        // TODO: return error
        }

        dst.reserve(5 + parameters.len());
        dst.put_u8(pdu_id);
        dst.put_u16_be(item.transaction_id);
        dst.put_u16_be(parameters.len() as u16);
        dst.put(parameters);

        Ok(())
    }
}

impl PacketTy {
    fn from_parameters(id: u8, params: &[u8]) -> Result<PacketTy, io::Error> {
        match id {
            0x1 => {
                assert_eq!(params.len(), 2);        // TODO: don't panic
                let code = BigEndian::read_u16(params);
                Ok(PacketTy::ErrorResponse(ErrorResponseCode(code)))
            },
            0x2 => {
                let (service_search_pattern, params) = Data::from_slice(params)?;
                let service_search_pattern = if let Data::Sequence(list) = service_search_pattern {
                    let mut out = Vec::with_capacity(list.len());
                    for elem in list {
                        if let Data::Uuid(uuid) = elem {
                            out.push(uuid);
                        } else {
                            panic!()        // TODO:
                        }
                    }
                    out
                } else {
                    panic!()        // TODO:
                };
                // TODO: length check
                let maximum_service_record_count = BigEndian::read_u16(params);
                let continuation_state = ContinuationState(params[2..].to_vec());
                Ok(PacketTy::ServiceSearchRequest {
                    service_search_pattern,
                    maximum_service_record_count,
                    continuation_state,
                })
            },
            0x3 => {
                assert!(params.len() >= 5);
                let total_service_record_count = BigEndian::read_u16(&params[0..2]);
                let cur_rec_count = BigEndian::read_u16(&params[2..4]) as usize;
                assert!(params.len() >= 5 + 4 * cur_rec_count);
                let service_record_handle_list = (0..cur_rec_count)
                    .map(|n| BigEndian::read_u32(&params[4 + n * 4 .. 4 + n * 4 + 4]))
                    .collect();
                let continuation_state = ContinuationState(params[4 + (cur_rec_count * 4)..].to_vec());
                Ok(PacketTy::ServiceSearchResponse {
                    total_service_record_count,
                    service_record_handle_list,
                    continuation_state,
                })
            },
            0x4 => Ok(PacketTy::ServiceAttributeRequest),
            0x5 => Ok(PacketTy::ServiceAttributeResponse),
            0x6 => {
                unimplemented!()
                //Ok(PacketTy::ServiceSearchAttributeRequest)
            },
            0x7 => {
                assert!(params.len() >= 3);
                let attrib_list_bytes_count = BigEndian::read_u16(&params[0..2]) as usize;
                assert!(params.len() >= 3 + attrib_list_bytes_count);
                let (data, remaining) = Data::from_slice(&params[2.. 2 + attrib_list_bytes_count]).unwrap();
                assert!(remaining.is_empty());
                let continuation_state = ContinuationState(params[2 + attrib_list_bytes_count..].to_vec());
                let attribute_lists = if let Data::Sequence(seq) = data {
                    seq.into_iter()
                        .map(|l| if let Data::Sequence(l) = l { l } else { panic!() })
                        .map(|l| l.chunks(2).map(|elems| {
                            let id = if let Data::Uint(v) = elems[0] { v as u16 } else { panic!() };
                            (id, elems[1].clone())
                        }).collect::<Vec<_>>())
                        .collect()
                } else { panic!() };
                Ok(PacketTy::ServiceSearchAttributeResponse {
                    attribute_lists,
                    continuation_state,
                })
            },
            _ => panic!()       // TODO: no
        }
    }

    fn into_id_params(self) -> (u8, Vec<u8>) {
        match self {
            PacketTy::ErrorResponse(code) => {
                let mut params = vec![0; 2];
                BigEndian::write_u16(&mut params, code.0);
                (0x1, params)
            },
            PacketTy::ServiceSearchRequest { service_search_pattern, maximum_service_record_count, continuation_state } => {
                let service_search_pattern = Data::Sequence(service_search_pattern
                    .into_iter()
                    .map(Data::Uuid)
                    .collect());
                let mut params = Vec::new();
                service_search_pattern.serialize(&mut params);      // TODO: result must be processed
                params.write_u16::<BigEndian>(maximum_service_record_count).expect("Writing to a Vec never fails");
                params.extend(continuation_state.0);
                (0x2, params)
            },
            PacketTy::ServiceSearchResponse { .. } => {
                unimplemented!()
                //(0x3, Vec::new())
            },
            PacketTy::ServiceAttributeRequest => (0x4, Vec::new()),
            PacketTy::ServiceAttributeResponse => (0x5, Vec::new()),
            PacketTy::ServiceSearchAttributeRequest { service_search_pattern, maximum_attribute_byte_count, attribute_id_list, continuation_state } => {
                let service_search_pattern = Data::Sequence(service_search_pattern
                    .into_iter()
                    .map(Data::Uuid)
                    .collect());
                let attribute_id_list = Data::Sequence(attribute_id_list
                    .into_iter()
                    .map(|attr| {
                        match attr {
                            AttributeSearch::Attribute(id) => Data::Uint(u128::from(id)),
                            AttributeSearch::Range(range) => {
                                let r = u32::from(*range.start()) << 16 | u32::from(*range.end());
                                Data::U32(r)
                            }
                        }
                    })
                    .collect());
                let mut params = Vec::new();
                service_search_pattern.serialize(&mut params);      // TODO: result must be processed
                params.write_u16::<BigEndian>(maximum_attribute_byte_count).expect("Writing to a Vec never fails");
                attribute_id_list.serialize(&mut params);      // TODO: result must be processed
                params.extend(continuation_state.0);
                (0x6, params)
            },
            PacketTy::ServiceSearchAttributeResponse { .. } => {
                unimplemented!()
                // TODO: (0x7, Vec::new())
            },
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ErrorResponseCode(u16);

impl ErrorResponseCode {
    pub fn message(&self) -> &'static str {
        match self.0 {
            0x0001 => "Invalid/unsupported SDP version",
            0x0002 => "Invalid Service Record Handle",
            0x0003 => "Invalid request syntax",
            0x0004 => "Invalid PDU Size",
            0x0005 => "Invalid Continuation State",
            0x0006 => "Insufficient Resources to satisfy Request",
            _ => panic!()       // TODO: no
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Data {
    Nil,
    // This variant is added because servers differentiate attributes from attribute ranges based on the size
    U32(u32),      // TODO: PartialEq impl should be adjusted; also
    Uint(u128),
    Sint(i128),
    Uuid(Uuid),
    Str(String),
    Bool(bool),
    Sequence(Vec<Data>),
    Alternative(Vec<Data>),
    Url(Vec<u8>),
}

impl Data {
    fn from_slice(data: &[u8]) -> Result<(Data, &[u8]), io::Error> {
        if data.is_empty() {
            panic!()        // TODO:
        }

        let (ty, len_code) = (data[0] >> 3, data[0] & 0b111);

        let (payload_len, header_len) = match (ty, len_code) {
            (0, 0) => (0, 0),
            (_, 0) => (1, 0),
            (_, 1) => (2, 0),
            (_, 2) => (4, 0),
            (_, 3) => (8, 0),
            (_, 4) => (16, 0),
            (_, 5) => (data[1] as usize, 1),       // TODO: check length of data
            (_, 6) => (BigEndian::read_u16(&data[1..3]) as usize, 2),      // TODO: check length of data
            (_, 7) => (BigEndian::read_u32(&data[1..5]) as usize, 4),      // TODO: check length of data
            _ => panic!()       // TODO:
        };

        if data.len() < 1 + header_len + payload_len {
            panic!()        // TODO:
        }
        let payload = &data[1 + header_len .. 1 + header_len + payload_len];

        let out = match (ty, len_code) {
            (0, 0) => Data::Nil,
            (1, 0) => Data::Uint(From::from(data[1])),
            (1, 1) => Data::Uint(From::from(BigEndian::read_u16(&data[1..]))),
            (1, 2) => Data::Uint(From::from(BigEndian::read_u32(&data[1..]))),
            (1, 3) => Data::Uint(From::from(BigEndian::read_u64(&data[1..]))),
            (1, 4) => Data::Uint(BigEndian::read_u128(&data[1..])),
            (2, 0) => Data::Sint(From::from(data[1])),
            (2, 1) => Data::Sint(From::from(BigEndian::read_i16(&data[1..]))),
            (2, 2) => Data::Sint(From::from(BigEndian::read_i32(&data[1..]))),
            (2, 3) => Data::Sint(From::from(BigEndian::read_i64(&data[1..]))),
            (2, 4) => Data::Sint(BigEndian::read_i128(&data[1..])),
            (3, 1) => Data::Uuid(Uuid::Uuid16(BigEndian::read_u16(&data[1..]))),
            (3, 2) => Data::Uuid(Uuid::Uuid32(BigEndian::read_u32(&data[1..]))),
            (3, 4) => Data::Uuid(Uuid::Uuid128(BigEndian::read_u128(&data[1..]))),
            (4, _) => Data::Str(String::from_utf8(payload.to_vec()).unwrap()),      // TODO: no unwrap
            (5, 0) => Data::Bool(data[1] != 0),
            (6, _) => Data::Sequence(Self::from_slice_multi(payload)?),
            (7, _) => Data::Alternative(Self::from_slice_multi(payload)?),
            (8, _) => Data::Url(payload.to_vec()),
            _ => panic!()       // TODO:
        };

        Ok((out, &data[1 + header_len + payload_len..]))
    }

    /// Expects that `data` contains multiple data elements one behind another, and returns a `Vec`
    /// containing these data elements.
    fn from_slice_multi(mut data: &[u8]) -> Result<Vec<Self>, io::Error> {
        let mut out = Vec::with_capacity(1);
        while !data.is_empty() {
            let (elem, new_data) = Self::from_slice(data)?;
            data = new_data;
            out.push(elem);
        }
        Ok(out)
    }

    fn serialize(&self, mut writer: impl io::Write) -> Result<(), io::Error> {
        fn serialize_bytes(mut writer: impl io::Write, ty: u8, bytes: &[u8]) -> io::Result<()> {
            let len = bytes.len();
            if len <= u8::max_value() as usize {
                writer.write_u8(ty << 3 | 5)?;
                writer.write_u8(len as u8)?;
                writer.write_all(bytes)?;
            } else if len <= u16::max_value() as usize {
                writer.write_u8(ty << 3 | 6)?;
                writer.write_u16::<BigEndian>(len as u16)?;
                writer.write_all(bytes)?;
            } else if len <= u32::max_value() as usize {
                writer.write_u8(ty << 3 | 7)?;
                writer.write_u32::<BigEndian>(len as u32)?;
                writer.write_all(bytes)?;
            } else {
                panic!()        // TODO:
            }
            Ok(())
        }

        match *self {
            Data::Nil => {
                writer.write_u8(0)?
            },
            Data::U32(val) => {
                writer.write_u8(1 << 3 | 2)?;
                writer.write_u32::<BigEndian>(val);
            },
            Data::Uint(val) => {
                if val <= u8::max_value() as u128 {
                    writer.write_u8(1 << 3 | 0)?;
                    writer.write_u8(val as u8);
                } else if val <= u16::max_value() as u128 {
                    writer.write_u8(1 << 3 | 1)?;
                    writer.write_u16::<BigEndian>(val as u16);
                } else if val <= u32::max_value() as u128 {
                    writer.write_u8(1 << 3 | 2)?;
                    writer.write_u32::<BigEndian>(val as u32);
                } else if val <= u64::max_value() as u128 {
                    writer.write_u8(1 << 3 | 3)?;
                    writer.write_u64::<BigEndian>(val as u64);
                } else {
                    writer.write_u8(1 << 3 | 4)?;
                    writer.write_u128::<BigEndian>(val);
                }
            },
            Data::Uint(val) => {
                if val <= u8::max_value() as u128 {
                    writer.write_u8(1 << 3 | 0)?;
                    writer.write_u8(val as u8);
                } else if val <= u16::max_value() as u128 {
                    writer.write_u8(1 << 3 | 1)?;
                    writer.write_u16::<BigEndian>(val as u16);
                } else if val <= u32::max_value() as u128 {
                    writer.write_u8(1 << 3 | 2)?;
                    writer.write_u32::<BigEndian>(val as u32);
                } else if val <= u64::max_value() as u128 {
                    writer.write_u8(1 << 3 | 3)?;
                    writer.write_u64::<BigEndian>(val as u64);
                } else {
                    writer.write_u8(1 << 3 | 4)?;
                    writer.write_u128::<BigEndian>(val);
                }
            },
            Data::Sint(val) => unimplemented!(),
            Data::Uuid(Uuid::Uuid16(val)) => {
                writer.write_u8(3 << 3 | 1)?;
                writer.write_u16::<BigEndian>(val);
            },
            Data::Uuid(Uuid::Uuid32(val)) => {
                writer.write_u8(3 << 3 | 2)?;
                writer.write_u32::<BigEndian>(val);
            },
            Data::Uuid(Uuid::Uuid128(val)) => {
                writer.write_u8(3 << 3 | 4)?;
                writer.write_u128::<BigEndian>(val);
            },
            Data::Str(ref string) => {
                serialize_bytes(writer, 4, string.as_bytes())?;
            },
            Data::Bool(val) => {
                writer.write_u8(5 << 3)?;
                writer.write_u8(if val { 1 } else { 0 })?;
            },
            Data::Sequence(ref list) => {
                let mut buf = Vec::new();
                for elem in list { elem.serialize(&mut buf)?; }
                serialize_bytes(writer, 6, &buf)?;
            },
            Data::Alternative(ref list) => {
                let mut buf = Vec::new();
                for elem in list { elem.serialize(&mut buf)?; }
                serialize_bytes(writer, 7, &buf)?;
            },
            Data::Url(ref url) => {
                serialize_bytes(writer, 8, &url)?
            },
        }

        Ok(())
    }
}

#[derive(Debug, Copy, Clone)]
pub enum Uuid {
    Uuid16(u16),
    Uuid32(u32),
    Uuid128(u128),
}

const BLUETOOTH_BASE_UUID: u128 = 0x00000000_0000_1000_8000_00805F9B34FB;

impl Uuid {
    /// Returns the `u128` corresponding to this UUID by widening the value if necessary.
    pub fn to_u128(&self) -> u128 {
        match self {
            Uuid::Uuid16(val) => (u128::from(*val) << 96) + BLUETOOTH_BASE_UUID,
            Uuid::Uuid32(val) => (u128::from(*val) << 96) + BLUETOOTH_BASE_UUID,
            Uuid::Uuid128(val) => *val,
        }
    }

    /// Simplifies the UUID if possible.
    pub fn simplify(&mut self) {
        match self {
            Uuid::Uuid16(_) => return,
            Uuid::Uuid32(val) => {
                if *val <= u16::max_value() as u32 {
                    *self = Uuid::Uuid16(*val as u16);
                }
            },
            Uuid::Uuid128(val) => {
                if let Some(v) = val.checked_sub(BLUETOOTH_BASE_UUID) {
                    if v & ((1 << 96) - 1) == 0 {
                        let v = v >> 96;
                        if v < u16::max_value() as u128 {
                            *self = Uuid::Uuid16(v as u16);
                        } else if v < u32::max_value() as u128 {
                            *self = Uuid::Uuid32(v as u32);
                        }
                    }
                }
            },
        }
    }
}

impl cmp::PartialEq for Uuid {
    fn eq(&self, other: &Uuid) -> bool {
        match (self, other) {
            (Uuid::Uuid16(a), Uuid::Uuid16(b)) => a == b,
            (Uuid::Uuid16(a), Uuid::Uuid32(b)) => u32::from(*a) == *b,
            (a @ Uuid::Uuid16(_), Uuid::Uuid128(b)) => a.to_u128() == *b,
            (Uuid::Uuid32(a), Uuid::Uuid16(b)) => *a == u32::from(*b),
            (Uuid::Uuid32(a), Uuid::Uuid32(b)) => a == b,
            (a @ Uuid::Uuid32(_), Uuid::Uuid128(b)) => a.to_u128() == *b,
            (Uuid::Uuid128(a), b @ Uuid::Uuid16(_)) => *a == b.to_u128(),
            (Uuid::Uuid128(a), b @ Uuid::Uuid32(_)) => *a == b.to_u128(),
            (Uuid::Uuid128(a), Uuid::Uuid128(b)) => a == b,
        }
    }
}

impl cmp::Eq for Uuid {
}

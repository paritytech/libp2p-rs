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

use std::os::raw::{c_char, c_int, c_ushort, c_long, c_void};

pub const BTPROTO_L2CAP: i32 = 0x0;
pub const BTPROTO_HCI: i32 = 0x1;
pub const BTPROTO_RFCOMM: i32 = 0x3;
pub const IREQ_CACHE_FLUSH: i64 = 0x1;
pub const PUBLIC_BROWSE_GROUP: u16 = 0x1002;
pub const SDP_ATTR_BROWSE_GRP_LIST: u16 = 0x0005;
pub const SDP_UINT8: u8 = 0x08;
pub const SDP_RETRY_IF_BUSY: u32 = 0x01;
pub const SDP_WAIT_ON_CLOSE: u32 = 0x02;
pub const SDP_NON_BLOCKING: u32 = 0x04;
pub const SDP_LARGE_MTU: u32 = 0x08;
pub const L2CAP_UUID: u16 = 0x0100;
pub const RFCOMM_UUID: u16 = 0x0003;
pub const BDADDR_ANY: bdaddr_t = bdaddr_t { b: [0, 0, 0, 0, 0, 0] };
pub const BDADDR_LOCAL: bdaddr_t = bdaddr_t { b: [0, 0, 0, 0xff, 0xff, 0xff] };
pub const HCISETSCAN: u64 = 0x400448dd;
pub const HCIGETDEVLIST: u64 = 0x800448d2;
pub const HCIINQUIRY: u64 = 0x800448f0;
pub const HCI_MAX_DEV: u16 = 16;
pub const SCAN_INQUIRY: u32 = 0x01;
pub const SCAN_PAGE: u32 = 0x02;
pub const HCI_UP: u32 = 0;

#[repr(C)]
#[derive(Copy, Debug, Clone)]
pub struct uint128_t {
    pub data: [u8; 16],
}

#[repr(C, packed)]
#[derive(Copy, Debug, Clone)]
pub struct bdaddr_t {
    pub b: [u8; 6]
}

#[repr(C)]
#[derive(Copy, Debug, Clone)]
pub struct sockaddr_rc {
    pub rc_family: libc::sa_family_t,
    pub rc_bdaddr: bdaddr_t,
    pub rc_channel: u8,
}

#[repr(C, packed)]
#[derive(Copy, Debug, Clone)]
pub struct inquiry_info {
    pub baddr: bdaddr_t,
    pub pscan_rep_mode: u8,
    pub pscan_period_mode: u8,
    pub pscan_mode: u8,
    pub dev_class: [u8; 3],
    pub clock_offset: u16,
}

#[repr(C)]
pub struct sdp_list_t {
    pub next: *const sdp_list_t,
    pub data: *const c_void,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct uuid_t {
    pub ty: u8,
    pub value: uuid_value,
}

#[repr(C)]
#[derive(Copy, Clone)]
pub union uuid_value {
    uuid16: u16,
    uuid32: u32,
    uuid128: uint128_t,
}

#[repr(C)]
pub struct sdp_record_t {
    pub handle: u32,
    pub pattern: *mut sdp_list_t,
    pub attrlist: *mut sdp_list_t,
    pub svclass: uuid_t,
}

#[repr(C)]
pub struct sdp_session_t {
    pub sock: c_int,
    pub state: c_int,
    pub local: c_int,
    pub flags: c_int,
    pub tid: u16,
    pub pri: *mut c_void,
}

#[repr(C)]
pub struct sdp_data_t {
    pub dtd: u8,
    pub attrId: u16,
    pub val: sdp_data_val,
    pub next: *mut sdp_data_t,
    pub unitSize: c_int,
}

#[repr(C)]
pub union sdp_data_val {
    int8: i8,
    int16: i16,
    int32: i32,
    int64: i64,
    int128: uint128_t,
    uint8: u8,
    uint16: u16,
    uint32: u32,
    uint64: u64,
    uint128: uint128_t,
    uuid: uuid_t,
    string: *const c_char,
    dataseq: *const sdp_data_t,
}

#[repr(C)]
pub struct hci_dev_req {
    pub dev_id: u16,
    pub dev_opt: u32,
}

#[repr(C)]
pub struct hci_inquiry_req {
    pub dev_id: u16,
    pub flags: u16,
    pub lap: [u8; 3],
    pub length: u8,
    pub num_rsp: u8,
}

#[repr(C)]
pub struct hci_dev_list_req {
    pub dev_num: u16,
    pub dev_req: [hci_dev_req; 0],
}

#[repr(C)]
pub struct hci_conn_list_req {
    pub dev_id: u16,
    pub conn_num: u16,
    pub conn_info: [hci_conn_info; 0],
}

#[repr(C)]
pub struct hci_conn_info {
    pub handle: u16,
    pub bdaddr: bdaddr_t,
    pub ty: u8,
    pub out: u8,
    pub state: u16,
    pub link_mode: u32,
}

#[repr(C)]
pub struct sockaddr_l2 {
	pub l2_family: libc::sa_family_t,
	pub l2_psm: c_ushort,
	pub l2_bdaddr: bdaddr_t,
	pub l2_cid: c_ushort,
	pub l2_bdaddr_type: u8,
}

// TODO: build script instead
// TODO: remove useless methods; eventually we can remove everything
#[link(name = "bluetooth")]
extern "C" {
    pub fn hci_get_route(addr: *mut bdaddr_t) -> c_int;
    pub fn hci_devid(addr: *const c_char) -> c_int;
    pub fn hci_open_dev(dev_id: c_int) -> c_int;
    pub fn hci_inquiry(dev_id: c_int, len: c_int, max_rsp: c_int, lap: *const u8, ii: *mut *mut inquiry_info, flags: c_long) -> c_int;
    pub fn hci_read_remote_name(hci_sock: c_int, addr: *const bdaddr_t, len: c_int, name: *mut c_char, timeout: c_int) -> c_int;
    pub fn sdp_record_alloc() -> *mut sdp_record_t;
    pub fn sdp_record_free(rec: *mut sdp_record_t);
    pub fn sdp_uuid16_create(uuid: *mut uuid_t, data: u16) -> *mut uuid_t;
    pub fn sdp_uuid32_create(uuid: *mut uuid_t, data: u32) -> *mut uuid_t;
    pub fn sdp_uuid128_create(uuid: *mut uuid_t, data: *const c_void) -> *mut uuid_t;
    pub fn sdp_set_service_id(rec: *mut sdp_record_t, uuid: uuid_t);
    pub fn sdp_list_append(list: *mut sdp_list_t, d: *mut c_void) -> *mut sdp_list_t;
    pub fn sdp_set_uuidseq_attr(rec: *mut sdp_record_t, attr: u16, seq: *mut sdp_list_t) -> c_int;
    pub fn sdp_data_alloc(dtd: u8, value: *const c_void) -> *mut sdp_data_t;
    pub fn sdp_set_access_protos(rec: *mut sdp_record_t, proto: *const sdp_list_t) -> c_int;
    pub fn sdp_set_info_attr(rec: *mut sdp_record_t, name: *const c_char, prov: *const c_char, desc: *const c_char);
    pub fn sdp_connect(src: *const bdaddr_t, dst: *const bdaddr_t, flags: u32) -> *mut sdp_session_t;
    pub fn sdp_record_register(session: *mut sdp_session_t, rec: *mut sdp_record_t, flags: u8) -> c_int;
    pub fn sdp_data_free(data: *mut sdp_data_t);
    pub fn sdp_list_free(list: *mut sdp_list_t, f: Option<extern "C" fn(*mut c_void)>);
    pub fn sdp_close(session: *mut sdp_session_t) -> c_int;
}

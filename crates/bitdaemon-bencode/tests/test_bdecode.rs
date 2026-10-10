// Conformance tests ported from libtorrent `test/test_bdecode.cpp`.

use bitdaemon_bencode::{
    BdecodeErrorKind, BdecodeNode, Entry, NodeType, SoftError, bdecode, bdecode_checked,
    bdecode_limits, bencode, print_node,
};

fn err_kind(e: &BdecodeNode<'_>) -> Option<BdecodeErrorKind> {
    e.error().map(|e| e.kind)
}
fn err_pos(e: &BdecodeNode<'_>) -> Option<usize> {
    e.error().map(|e| e.position)
}
fn soft_msg(e: &BdecodeNode<'_>) -> Option<&'static str> {
    e.has_soft_error().map(SoftError::message)
}

#[test]
fn integer() {
    let b = b"i12453e";
    let e = bdecode(b);
    assert!(e.error().is_none());
    assert_eq!(e.data_section(), b);
    assert_eq!(e.node_type(), NodeType::Int);
    assert_eq!(e.int_value(), 12453);
}

#[test]
fn construct_entry() {
    assert_eq!(Entry::from("abc123").string().unwrap(), b"abc123");
    assert_eq!(
        Entry::from(b"abc123".as_slice()).string().unwrap(),
        b"abc123"
    );
    assert_eq!(Entry::from(4i64).integer().unwrap(), 4);
}

#[test]
fn string() {
    let b = b"26:abcdefghijklmnopqrstuvwxyz";
    let e = bdecode(b);
    assert!(e.error().is_none());
    assert_eq!(e.data_section(), b.as_slice());
    assert_eq!(e.node_type(), NodeType::String);
    assert_eq!(e.string_value(), b"abcdefghijklmnopqrstuvwxyz");
    assert_eq!(e.string_length(), 26);
}

#[test]
fn string_prefix1() {
    // edge-case: string that's nearly too long for the short header
    let mut test = vec![0u8; 1_000_000 + 8];
    test[..8].copy_from_slice(b"1000000:");
    let e = bdecode(&test);
    assert!(e.error().is_none());
    assert_eq!(e.data_section(), test.as_slice());
    assert_eq!(e.node_type(), NodeType::String);
    assert_eq!(e.string_length(), 1_000_000);
    assert_eq!(e.string_offset(), Some(8));
}

#[test]
fn list() {
    let b = b"li12453e3:aaae";
    let e = bdecode(b);
    assert!(e.error().is_none());
    assert_eq!(e.data_section(), b.as_slice());
    assert_eq!(e.node_type(), NodeType::List);
    assert_eq!(e.list_size(), 2);
    assert_eq!(e.list_at(0).node_type(), NodeType::Int);
    assert_eq!(e.list_at(1).node_type(), NodeType::String);
    assert_eq!(e.list_at(0).int_value(), 12453);
    assert_eq!(e.list_at(1).string_value(), b"aaa");
    assert_eq!(e.list_at(1).string_length(), 3);
    assert_eq!(e.list_at(1).data_section(), b"3:aaa");
}

#[test]
fn dict() {
    let b = b"d1:ai12453e1:b3:aaa1:c3:bbb1:X10:0123456789e";
    let e = bdecode(b);
    assert!(e.error().is_none());
    assert_eq!(e.data_section(), b.as_slice());
    assert_eq!(e.node_type(), NodeType::Dict);
    assert_eq!(e.dict_size(), 4);
    assert_eq!(e.dict_find(b"a").node_type(), NodeType::Int);
    assert_eq!(e.dict_find(b"a").int_value(), 12453);
    assert_eq!(e.dict_find(b"b").node_type(), NodeType::String);
    assert_eq!(e.dict_find(b"b").string_value(), b"aaa");
    assert_eq!(e.dict_find(b"b").string_length(), 3);
    assert_eq!(e.dict_find(b"c").string_value(), b"bbb");
    assert_eq!(e.dict_find_string_value(b"X", b""), b"0123456789");
    assert_eq!(soft_msg(&e), Some("unsorted dictionary key"));
}

#[test]
fn dict_key_novalue() {
    let e = bdecode(b"d1:ai1e1:be");
    assert_eq!(err_pos(&e), Some(10));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedValue));
}

#[test]
fn dict_nonstring_key() {
    let e = bdecode(b"di5e1:ae");
    assert_eq!(err_pos(&e), Some(1));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn dict_null_key() {
    let e = bdecode(b"d3:a\0bi1ee");
    assert_eq!(e.dict_size(), 1);
    let d = e.dict_find(b"a\0b");
    assert_eq!(d.node_type(), NodeType::Int);
    assert_eq!(d.int_value(), 1);
}

#[test]
fn dict_unordered_keys() {
    for b in [
        b"d2:abi1e2:aai2ee".as_slice(),
        b"d2:bai1e2:aai2ee",
        b"d2:aai1e1:ai2ee",
    ] {
        let e = bdecode(b);
        assert_eq!(soft_msg(&e), Some("unsorted dictionary key"), "{b:?}");
    }
    for b in [b"d1:ai1e2:aai2ee".as_slice(), b"d2:aai1e1:bi2ee"] {
        let e = bdecode(b);
        assert_eq!(e.has_soft_error(), None, "{b:?}");
    }
}

#[test]
fn dict_duplicate_key() {
    let e = bdecode(b"d2:aai1e2:aai2ee");
    assert_eq!(soft_msg(&e), Some("duplicate dictionary key"));
}

#[test]
fn premature_e() {
    let e = bdecode(b"e");
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn negative_length_prefix() {
    let e = bdecode(b"-10:foobar");
    assert_eq!(err_pos(&e), Some(0));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedValue));
}

#[test]
fn overflow_length_prefix() {
    let e = bdecode(b"18446744073709551615:foobar");
    assert_eq!(err_pos(&e), Some(19));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::Overflow));
}

#[test]
fn close_overflow_length_prefix() {
    let e = bdecode(b"99999999:foobar");
    assert_eq!(err_pos(&e), Some(8));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn leading_zero_length_prefix() {
    let e = bdecode(b"06:foobar");
    assert_eq!(soft_msg(&e), Some("leading zero in string length"));
    let e = bdecode(b"0:");
    assert_eq!(e.has_soft_error(), None);
}

#[test]
fn nodigit_int() {
    let e = bdecode(b"ie");
    assert_eq!(err_pos(&e), Some(1));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn minus_int() {
    let e = bdecode(b"i-e");
    assert_eq!(err_pos(&e), Some(2));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn interior_minus_int() {
    let e = bdecode(b"i35412-5633e");
    assert_eq!(err_pos(&e), Some(6));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn int_overflow() {
    // 20 digits fit the check; the lazy int_value() overflows → 0
    let e = bdecode(b"i18446744073709551615e");
    assert!(e.error().is_none());
    assert_eq!(e.int_value(), 0);
}

#[test]
fn int_overflow2() {
    let e = bdecode(b"i184467440737095516154e");
    assert_eq!(err_pos(&e), Some(22));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::Overflow));
}

#[test]
fn int_truncated() {
    let e = bdecode(b"i-");
    assert_eq!(err_pos(&e), Some(2));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn int_leading_zero() {
    let e = bdecode(b"i01e");
    assert_eq!(soft_msg(&e), Some("leading zero in integer"));
    let e = bdecode(b"i0e");
    assert_eq!(e.has_soft_error(), None);
}

#[test]
fn error_messages() {
    assert_eq!(BdecodeErrorKind::Overflow.message(), "integer overflow");
}

#[test]
fn int_64bit() {
    let e = bdecode(b"i9223372036854775807e");
    assert_eq!(e.int_value(), 9223372036854775807);
    let e = bdecode(b"i-9223372036854775807e");
    assert_eq!(e.int_value(), -9223372036854775807);
}

#[test]
fn int_invalid_digit() {
    let e = bdecode(b"i92337203t854775807e");
    assert_eq!(err_pos(&e), Some(9));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn invalid_encoding() {
    let buf: &[u8] = &[
        0x64, 0x31, 0x3a, 0x61, 0x64, 0x32, 0x3a, 0x69, 0x64, 0x32, 0x30, 0x3a, 0x2a, 0x21, 0x19,
        0x89, 0x9f, 0xcd, 0x5f, 0xc9, 0xbc, 0x80, 0xc1, 0x76, 0xfe, 0xe0, 0xc6, 0x84, 0x2d, 0xf6,
        0xfc, 0xb8, 0x39, 0x3a, 0x69, 0x6e, 0x66, 0x6f, 0x5f, 0x68, 0x61, 0xae, 0x68, 0x32, 0x30,
        0x3a, 0x14, 0x78, 0xd5, 0xb0, 0xdc, 0xf6, 0x82, 0x42, 0x32, 0xa0, 0xd6, 0x88, 0xeb, 0x48,
        0x57, 0x01, 0x89, 0x40, 0x4e, 0xbc, 0x65, 0x31, 0x3a, 0x71, 0x39, 0x3a, 0x67, 0x65, 0x74,
        0x5f, 0x70, 0x65, 0x65, 0x72, 0x78, 0xff, 0x3a, 0x74, 0x38, 0x3a, 0xaa, 0xd4, 0xa1, 0x88,
        0x7a, 0x8d, 0xc3, 0xd6, 0x31, 0x3a, 0x79, 0x31, 0xae, 0x71, 0x65, 0x65,
    ];
    let e = bdecode(buf);
    assert!(e.error().is_some());
}

#[test]
fn depth_limit() {
    let mut b = vec![b'l'; 1024];
    b.extend(std::iter::repeat_n(b'e', 1024));
    let e = bdecode_limits(&b, 100, 2_000_000);
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::DepthExceeded));
}

#[test]
fn item_limit() {
    let mut b = vec![b'l'];
    for _ in 0..5119 {
        b.extend_from_slice(b"0:");
    }
    b.push(b'e');
    let e = bdecode_limits(&b, 1000, 1000);
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::LimitExceeded));
}

#[test]
fn unexpected_eof() {
    let e = bdecode(&b"l2:.."[..5]);
    assert_eq!(err_pos(&e), Some(5));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn unexpected_eof2() {
    let e = bdecode(&b"l2:..0"[..6]);
    assert_eq!(err_pos(&e), Some(6));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn expected_string() {
    // dict keys must be strings
    let e = bdecode(b"di2ei0ee");
    assert_eq!(err_pos(&e), Some(1));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn unexpected_eof_dict_key() {
    let e = bdecode(b"d1000:..e");
    assert_eq!(err_pos(&e), Some(5));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
    let e = bdecode(b"d1000:");
    assert_eq!(err_pos(&e), Some(5));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn expected_string_dict_key2() {
    let e = bdecode(b"df00:");
    assert_eq!(err_pos(&e), Some(1));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedDigit));
}

#[test]
fn unexpected_eof_int() {
    let e = bdecode(&b"i"[..1]);
    assert_eq!(err_pos(&e), Some(1));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
    let e = bdecode(&b"i10"[..3]);
    assert_eq!(err_pos(&e), Some(3));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn expected_colon_dict() {
    let e = bdecode(b"d1000");
    assert_eq!(err_pos(&e), Some(5));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::ExpectedColon));
}

#[test]
fn empty_string() {
    let e = bdecode(b"");
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn partial_string() {
    let e = bdecode(b"100:..");
    assert_eq!(err_pos(&e), Some(3));
    assert_eq!(err_kind(&e), Some(BdecodeErrorKind::UnexpectedEof));
}

#[test]
fn list_ints() {
    let mut buf = String::from("l");
    for i in 0..1000 {
        buf += &format!("i{i}e");
    }
    buf += "e";
    let e = bdecode(buf.as_bytes());
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::List);
    assert_eq!(e.list_size(), 1000);
    for i in 0..1000 {
        assert_eq!(e.list_int_value_at(i, -1), i as i64);
    }
}

#[test]
fn dict_ints() {
    let mut buf = String::from("d");
    for i in 0..1000 {
        buf += &format!("4:{i:04}i{i}e");
    }
    buf += "e";
    let e = bdecode(buf.as_bytes());
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::Dict);
    assert_eq!(e.dict_size(), 1000);
    for i in 0..1000 {
        let key = format!("{i:04}");
        assert_eq!(e.dict_find_int_value(key.as_bytes(), -1), i as i64);
    }
}

#[test]
fn dict_at() {
    let e = bdecode(b"d3:fooi1e3:bari2ee");
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::Dict);
    assert_eq!(e.dict_size(), 2);
    assert_eq!(e.dict_at(0).0, b"foo");
    assert_eq!(e.dict_at(0).1.node_type(), NodeType::Int);
    assert_eq!(e.dict_at(0).1.int_value(), 1);
    assert_eq!(e.dict_at(1).0, b"bar");
    assert_eq!(e.dict_at(1).1.node_type(), NodeType::Int);
    assert_eq!(e.dict_at(1).1.int_value(), 2);
}

#[test]
fn string_ptr() {
    let b = b"l3:fooe";
    let e = bdecode(b);
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::List);
    assert_eq!(e.list_size(), 1);
    assert_eq!(e.list_at(0).node_type(), NodeType::String);
    assert_eq!(e.list_at(0).string_offset(), Some(3));
    assert_eq!(e.list_at(0).string_length(), 3);
}

#[test]
fn parse_length_overflow() {
    for buf in [
        b"d1:a1919191010:11111".as_slice(),
        b"d2143289344:a4:aaaae",
        b"d214328934114:a4:aaaae",
        b"d9205357638345293824:a4:aaaae",
        b"d1:a9205357638345293824:11111",
    ] {
        let e = bdecode(buf);
        assert_eq!(
            err_kind(&e),
            Some(BdecodeErrorKind::UnexpectedEof),
            "{buf:?}"
        );
    }
}

#[test]
fn dict_find_funs() {
    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee");
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::Dict);

    assert_eq!(e.dict_find_int_value(b"a", -1), 1);
    assert_eq!(e.dict_find_int(b"a").node_type(), NodeType::Int);
    assert_eq!(e.dict_find_int_value(b"b", -10), -10);
    assert_eq!(e.dict_find_int_value(b"x", -10), -10);
    assert_eq!(e.dict_find_int(b"b").node_type(), NodeType::None);
    assert_eq!(e.dict_find_int(b"x").node_type(), NodeType::None);

    assert_eq!(e.dict_find_string_value(b"b", b""), b"foo");
    assert_eq!(e.dict_find_string(b"b").node_type(), NodeType::String);
    assert_eq!(e.dict_find_string_value(b"c", b"blah"), b"blah");
    assert_eq!(e.dict_find_string_value(b"x", b"blah"), b"blah");
    assert_eq!(e.dict_find_string(b"c").node_type(), NodeType::None);
    assert_eq!(e.dict_find_string(b"x").node_type(), NodeType::None);

    assert!(e.dict_find_list(b"c").is_valid());
    assert_eq!(e.dict_find_list(b"c").list_size(), 2);
    assert_eq!(e.dict_find_list(b"c").list_int_value_at(0, -1), 1);
    assert_eq!(e.dict_find_list(b"c").list_int_value_at(1, -1), 2);
    assert!(!e.dict_find_list(b"d").is_valid());

    assert!(e.dict_find_dict(b"d").is_valid());
    assert_eq!(e.dict_find_dict(b"d").dict_find_int_value(b"x", -1), 1);
    assert_eq!(e.dict_find_dict(b"d").dict_find_int_value(b"y", -10), -10);
    assert!(!e.dict_find_dict(b"c").is_valid());
    assert!(!e.dict_find_dict(b"x").is_valid());

    assert_eq!(e.dict_size(), 4);

    assert_eq!(e.dict_at(0).0, b"a");
    assert_eq!(e.dict_at(0).1.int_value(), 1);
    assert_eq!(e.dict_at(1).0, b"b");
    assert_eq!(e.dict_at(1).1.string_value(), b"foo");
    assert_eq!(e.dict_at(2).0, b"c");
    assert_eq!(e.dict_at(2).1.node_type(), NodeType::List);
    assert_eq!(e.dict_at(3).0, b"d");
    assert_eq!(e.dict_at(3).1.node_type(), NodeType::Dict);
}

#[test]
fn list_at_funs() {
    let e = bdecode(b"li1e3:fooli1ei2eed1:xi1eee");
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::List);

    assert_eq!(e.list_int_value_at(0, -1), 1);
    assert_eq!(e.list_int_value_at(1, -10), -10);

    assert_eq!(e.list_string_value_at(1, b""), b"foo");
    assert_eq!(e.list_string_value_at(2, b"blah"), b"blah");

    assert_eq!(e.list_at(2).node_type(), NodeType::List);
    assert_eq!(e.list_at(2).list_size(), 2);
    assert_eq!(e.list_at(2).list_int_value_at(0, -1), 1);
    assert_eq!(e.list_at(2).list_int_value_at(1, -1), 2);

    assert_eq!(e.list_at(3).node_type(), NodeType::Dict);
    assert_eq!(e.list_at(3).dict_size(), 1);
    assert_eq!(e.list_at(3).dict_find_int_value(b"x", -1), 1);
    assert_eq!(e.list_at(3).dict_find_int_value(b"y", -10), -10);

    assert_eq!(e.list_size(), 4);
}

#[test]
fn list_at_reverse() {
    let e = bdecode(b"li1e3:fooli1ei2eed1:xi1eee");
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::List);

    assert_eq!(e.list_at(3).node_type(), NodeType::Dict);
    assert_eq!(e.list_at(2).node_type(), NodeType::List);
    assert_eq!(e.list_string_value_at(1, b""), b"foo");
    assert_eq!(e.list_int_value_at(0, -1), 1);

    assert_eq!(e.list_size(), 4);
}

#[test]
fn dict_find_funs2() {
    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee");
    assert!(e.error().is_none());
    // finding the last item skips all the others
    assert_eq!(e.dict_find(b"d").node_type(), NodeType::Dict);
}

#[test]
fn print_entry() {
    let e = bdecode(b"li1e3:fooli1ei2eed1:xi1eee");
    assert!(e.error().is_none());
    assert_eq!(
        print_node(&e, false, 0),
        "[ 1, 'foo', [ 1, 2 ], { 'x': 1 } ]"
    );
}

#[test]
fn print_entry2() {
    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee");
    assert!(e.error().is_none());
    assert_eq!(
        print_node(&e, false, 0),
        "{ 'a': 1, 'b': 'foo', 'c': [ 1, 2 ], 'd': { 'x': 1 } }"
    );
}

#[test]
fn swap() {
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let b2 = b"i1e";
    let mut e1 = bdecode(b1);
    let mut e2 = bdecode(b2);
    assert!(e1.error().is_none() && e2.error().is_none());

    let str1 = print_node(&e1, false, 0);
    let str2 = print_node(&e2, false, 0);
    assert_eq!(e1.node_type(), NodeType::Dict);
    assert_eq!(e2.node_type(), NodeType::Int);

    std::mem::swap(&mut e1, &mut e2);
    assert_eq!(e1.node_type(), NodeType::Int);
    assert_eq!(e2.node_type(), NodeType::Dict);
    assert_eq!(print_node(&e1, false, 0), str2);
    assert_eq!(print_node(&e2, false, 0), str1);

    std::mem::swap(&mut e1, &mut e2);
    assert_eq!(e1.node_type(), NodeType::Dict);
    assert_eq!(e2.node_type(), NodeType::Int);
    assert_eq!(print_node(&e1, false, 0), str1);
    assert_eq!(print_node(&e2, false, 0), str2);
}

#[test]
fn swap_root() {
    // one node is the root of the other
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let mut e1 = bdecode(b1);
    let mut e2 = e1.dict_find(b"c").list_at(0);

    let str1 = print_node(&e1, false, 0);
    let str2 = print_node(&e2, false, 0);
    assert_eq!(e1.node_type(), NodeType::Dict);
    assert_eq!(e2.node_type(), NodeType::Int);

    std::mem::swap(&mut e1, &mut e2);
    assert_eq!(e1.node_type(), NodeType::Int);
    assert_eq!(e2.node_type(), NodeType::Dict);
    assert_eq!(print_node(&e1, false, 0), str2);
    assert_eq!(print_node(&e2, false, 0), str1);

    std::mem::swap(&mut e1, &mut e2);
    assert_eq!(e1.node_type(), NodeType::Dict);
    assert_eq!(e2.node_type(), NodeType::Int);
}

#[test]
fn swap_disjoint() {
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let b2 = b"li1e3:fooli1ei2eed1:xi1eee";
    let e1_root = bdecode(b1);
    let e2_root = bdecode(b2);
    let mut e1 = e1_root.dict_find(b"c").list_at(0);
    let mut e2 = e2_root.list_at(1);

    let str1 = print_node(&e1, false, 0);
    let str2 = print_node(&e2, false, 0);
    assert_eq!(e1.node_type(), NodeType::Int);
    assert_eq!(e2.node_type(), NodeType::String);

    std::mem::swap(&mut e1, &mut e2);
    assert_eq!(e1.node_type(), NodeType::String);
    assert_eq!(e2.node_type(), NodeType::Int);
    assert_eq!(print_node(&e1, false, 0), str2);
    assert_eq!(print_node(&e2, false, 0), str1);
}

#[test]
fn swap_root_disjoint() {
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let b2 = b"li1e3:fooli1ei2eed1:xi1eee";
    let e1_root = bdecode(b1);
    let mut e2 = bdecode(b2);
    let mut e1 = e1_root.dict_find(b"d");

    let str1 = print_node(&e1, false, 0);
    let str2 = print_node(&e2, false, 0);
    assert_eq!(e1.node_type(), NodeType::Dict);
    assert_eq!(e2.node_type(), NodeType::List);

    std::mem::swap(&mut e1, &mut e2);
    assert_eq!(e1.node_type(), NodeType::List);
    assert_eq!(e2.node_type(), NodeType::Dict);
    assert_eq!(print_node(&e1, false, 0), str2);
    assert_eq!(print_node(&e2, false, 0), str1);
}

#[test]
fn clear() {
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let b2 = b"li1ei2ee";
    let mut e = bdecode(b1);
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::Dict);
    assert_eq!(e.dict_size(), 4);
    assert_eq!(e.dict_at(1).0, b"b");

    e = bdecode(b2);
    assert!(e.error().is_none());
    assert_eq!(e.node_type(), NodeType::List);
    assert_eq!(e.list_size(), 2);
    assert_eq!(e.list_int_value_at(1, -1), 2);
}

#[test]
fn copy_root() {
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let mut e1 = bdecode(b1);
    assert!(e1.error().is_none());

    let e2 = e1.clone();
    let e3 = e1.clone();

    e1.clear();

    assert_eq!(e2.node_type(), NodeType::Dict);
    assert_eq!(e2.dict_size(), 4);
    assert_eq!(e2.dict_at(1).0, b"b");
    assert_eq!(e3.node_type(), NodeType::Dict);
    assert_eq!(e3.dict_at(1).0, b"b");
}

#[test]
fn non_owning_refs() {
    // children share the token storage — stronger than upstream's
    // non_owning(): they stay valid even after the root is cleared/dropped
    let b1 = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let e2;
    {
        let mut e1 = bdecode(b1);
        assert!(e1.error().is_none());
        e2 = e1.dict_find(b"d");
        e1.clear();
        assert_eq!(e2.node_type(), NodeType::Dict);
    }
    assert_eq!(e2.dict_find_int_value(b"x", -1), 1);
}

#[test]
fn partial_parse() {
    // a partial parse is still printable up to where it failed
    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1-eee");
    assert_eq!(err_pos(&e), Some(35));
    assert_eq!(e.node_type(), NodeType::Dict);
    assert_eq!(
        print_node(&e, false, 0),
        "{ 'a': 1, 'b': 'foo', 'c': [ 1, 2 ], 'd': { 'x': {} } }"
    );

    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1ei2ee1:d-d1:xi1eee");
    assert_eq!(err_pos(&e), Some(29));
    assert_eq!(
        print_node(&e, false, 0),
        "{ 'a': 1, 'b': 'foo', 'c': [ 1, 2 ], 'd': {} }"
    );

    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1ei2ee-1:dd1:xi1eee");
    assert_eq!(err_pos(&e), Some(26));
    assert_eq!(
        print_node(&e, false, 0),
        "{ 'a': 1, 'b': 'foo', 'c': [ 1, 2 ] }"
    );

    let e = bdecode(b"d1:ai1e1:b3:foo1:cli1e-i2ee1:dd1:xi1eee");
    assert_eq!(err_pos(&e), Some(22));
    assert_eq!(
        print_node(&e, false, 0),
        "{ 'a': 1, 'b': 'foo', 'c': [ 1 ] }"
    );
}

#[test]
fn partial_parse_string() {
    let e = bdecode(b"55");
    assert!(e.error().is_some());
    assert_eq!(err_pos(&e), Some(2));
}

#[test]
fn switch_buffer() {
    let b1 = b"d1:ai1e1:b3:foo1:cli1e-i2ee1:dd1:xi1eee";
    let b2 = b"d1:ai1e1:b3:foo1:cli1e-i2ee1:dd1:xi1eee";
    let mut e = bdecode(b1);
    assert_eq!(err_pos(&e), Some(22));
    assert_eq!(e.node_type(), NodeType::Dict);

    let s1 = print_node(&e, false, 0);
    e.switch_underlying_buffer(b2);
    let s2 = print_node(&e, false, 0);
    assert_eq!(s1, s2);
}

#[test]
fn long_string_99999999() {
    let mut input = Vec::with_capacity(9 + 99_999_999);
    input.extend_from_slice(b"99999999:");
    input.resize(9 + 99_999_999, b'_');
    let e = bdecode(&input);
    assert_eq!(e.node_type(), NodeType::String);
    assert_eq!(e.string_value(), &input[9..]);
}

#[test]
fn long_string_100000000() {
    let mut input = Vec::with_capacity(10 + 100_000_000);
    input.extend_from_slice(b"100000000:");
    input.resize(10 + 100_000_000, b'_');
    let e = bdecode(&input);
    assert_eq!(e.node_type(), NodeType::String);
    assert_eq!(e.string_value(), &input[10..]);
}

#[test]
fn data_offset() {
    let e = bdecode(b"li1e3:fooli1ei2eed1:xi1eee");
    assert!(e.error().is_none());
    assert_eq!(e.data_offset(), Some(0));
    assert_eq!(e.list_at(0).data_offset(), Some(1));
    assert_eq!(e.list_at(1).data_offset(), Some(4));
    assert_eq!(e.list_at(2).data_offset(), Some(9));
    assert_eq!(e.list_at(3).data_offset(), Some(17));
}

#[test]
fn string_offset() {
    let e = bdecode(b"l3:foo3:bare");
    assert!(e.error().is_none());
    assert_eq!(e.list_at(0).string_offset(), Some(3));
    assert_eq!(e.list_at(1).string_offset(), Some(8));
}

#[test]
fn dict_at_node() {
    let e = bdecode(b"d3:foo3:bar4:test4:teste");
    assert!(e.error().is_none());
    let (k0, v0) = e.dict_at_node(0);
    assert_eq!(k0.string_offset(), Some(3));
    assert_eq!(v0.string_offset(), Some(8));
    let (k1, v1) = e.dict_at_node(1);
    assert_eq!(k1.string_offset(), Some(13));
    assert_eq!(v1.string_offset(), Some(19));
}

// --- entry + encoder round-trips (upstream test_bdecode / entry tests) ---

#[test]
fn entry_roundtrip() {
    let src = b"d1:ai1e1:b3:foo1:cli1ei2ee1:dd1:xi1eee";
    let node = bdecode(src);
    let e = Entry::from_node(&node);
    assert_eq!(bencode(&e), src.as_slice());
}

#[test]
fn entry_accessors() {
    let mut d = bitdaemon_bencode::Dictionary::new();
    d.insert(b"a".to_vec(), Entry::Int(42));
    d.insert(b"b".to_vec(), Entry::String(b"foo".to_vec()));
    let e = Entry::Dict(d);
    assert_eq!(e.find_key(b"a").unwrap().integer().unwrap(), 42);
    assert_eq!(e.find_key(b"b").unwrap().string().unwrap(), b"foo");
    assert!(e.find_key(b"x").is_none());
}

#[test]
fn bencode_checked_ok_and_err() {
    assert!(bdecode_checked(b"i5e").is_ok());
    let err = bdecode_checked(b"ie").unwrap_err();
    assert_eq!(err.kind, BdecodeErrorKind::ExpectedDigit);
    assert_eq!(err.position, 1);
}

/// Round-trips every `.torrent` under `$LIBTORRENT_TEST_DIR`
/// (e.g. `D:\Projet\libtorrent\libtorrent\test\test_torrents`).
/// Re-encoding through `Entry` canonicalizes dict order, so files with
/// soft errors are expected to differ — they're exercised for panics only.
#[test]
fn roundtrip_upstream_torrents() {
    let Ok(dir) = std::env::var("LIBTORRENT_TEST_DIR") else {
        return; // fixture checkout not configured — skip
    };
    let mut parsed = 0usize;
    let mut identical = 0usize;
    let mut soft = 0usize;
    let mut failed = 0usize;
    for entry in std::fs::read_dir(&dir).expect("LIBTORRENT_TEST_DIR readable") {
        let path = entry.unwrap().path();
        if path.extension().and_then(|e| e.to_str()) != Some("torrent") {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        let node = bdecode(&bytes);
        if node.error().is_some() {
            failed += 1;
            continue;
        }
        parsed += 1;
        if node.has_soft_error().is_some() {
            soft += 1;
            continue;
        }
        // compare against data_section(), not the file — upstream ignores
        // trailing bytes after the root item (e.g. a final CRLF)
        let re = bencode(&Entry::from_node(&node));
        assert_eq!(
            re,
            node.data_section(),
            "round-trip mismatch: {}",
            path.display()
        );
        identical += 1;
    }
    eprintln!(
        "{dir}: {parsed} parsed ({identical} byte-identical), {soft} soft-error, {failed} failed"
    );
    assert!(parsed > 0, "no torrents parsed from {dir}");
}

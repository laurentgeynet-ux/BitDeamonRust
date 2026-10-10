// This file is part of OnionBit.
// Copyright (C) 2026 Laurent Geynet
// SPDX-License-Identifier: GPL-3.0-or-later

//! Port of `kademlia/msg.cpp` — `verify_message_impl`: schema-driven
//! extraction of DHT message fields, with child-dict traversal.

use bitdaemon_bencode::{BdecodeNode, NodeType};

/// `key_desc_t` flags.
pub mod key_flags {
    /// `optional` — parsing doesn't fail when absent.
    pub const OPTIONAL: u8 = 1;
    /// `parse_children` — following entries are children of this
    /// dict, up to and including the next `last_child`.
    pub const PARSE_CHILDREN: u8 = 2;
    /// `last_child` — ends a `parse_children` group.
    pub const LAST_CHILD: u8 = 4;
    /// `size_divisible` — `size` is a modulus, not an exact size.
    pub const SIZE_DIVISIBLE: u8 = 8;
}

/// `key_desc_t` — one expected key.
#[derive(Debug, Clone, Copy)]
pub struct KeyDesc {
    /// `name`.
    pub name: &'static [u8],
    /// expected node type; `None` accepts any (`none_t`).
    pub ty: Option<NodeType>,
    /// required string size (or modulus with `SIZE_DIVISIBLE`); 0
    /// disables the check.
    pub size: usize,
    /// `key_flags::*`.
    pub flags: u8,
}

/// `verify_message_impl` — walks `desc` against `message`, filling
/// `ret` with the located nodes. `err` gets a short reason on
/// failure (upstream fills a `span<char>`).
pub fn verify_message<'a>(
    msg: &BdecodeNode<'a>,
    desc: &[KeyDesc],
    ret: &mut [BdecodeNode<'a>],
    err: &mut String,
) -> bool {
    ret.iter_mut().for_each(|n| *n = BdecodeNode::none());

    if msg.node_type() != NodeType::Dict {
        *err = "not a dictionary".into();
        return false;
    }
    // stack of parent dicts (max depth upstream is 5)
    let mut stack: Vec<BdecodeNode<'a>> = vec![msg.clone()];
    let mut msg = msg.clone();
    let mut i = 0usize;
    while i < desc.len() && i < ret.len() {
        let k = &desc[i];
        ret[i] = msg.dict_find(k.name);
        if ret[i].node_type() != NodeType::None {
            if let Some(want) = k.ty {
                if ret[i].node_type() != want {
                    ret[i] = BdecodeNode::none();
                }
            }
        }
        if ret[i].node_type() == NodeType::None && (k.flags & key_flags::OPTIONAL) == 0 {
            *err = format!("missing '{}' key", String::from_utf8_lossy(k.name));
            return false;
        }
        if k.size > 0 && ret[i].node_type() == NodeType::String && k.ty == Some(NodeType::String) {
            let len = ret[i].string_value().len();
            let invalid = if (k.flags & key_flags::SIZE_DIVISIBLE) != 0 {
                len % k.size != 0
            } else {
                len != k.size
            };
            if invalid {
                ret[i] = BdecodeNode::none();
                if (k.flags & key_flags::OPTIONAL) == 0 {
                    *err = format!("invalid value for '{}'", String::from_utf8_lossy(k.name));
                    return false;
                }
            }
        }
        if (k.flags & key_flags::PARSE_CHILDREN) != 0 {
            if ret[i].node_type() != NodeType::None {
                msg = ret[i].clone();
                stack.push(msg.clone());
            } else {
                // skip all children
                while i < desc.len() && (desc[i].flags & key_flags::LAST_CHILD) == 0 {
                    i += 1;
                }
            }
        } else if (k.flags & key_flags::LAST_CHILD) != 0 {
            if stack.len() <= 1 {
                return false;
            }
            stack.pop();
            msg = stack.last().expect("stack").clone();
        }
        i += 1;
    }
    true
}

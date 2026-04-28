//! B+Tree node (page) layout and codec.
//!
//! A node is either:
//! - **Leaf**: holds `(key, rid)` [`IndexEntry`] values and `prev/next` leaf pointers for range
//!   scans.
//! - **Internal**: holds separator keys and child [`PageNumber`] pointers.
//!
//! ## On-disk format
//!
//! The format is a small, self-describing header followed by a kind-specific tail:
//!
//! - `common`: `kind(u8)` | `parent(Option<PageNumber>)` | `entry_count(u32)` | `key_type(Type)`
//! - `leaf`: `prev(Option<PageNumber>)` | `next(Option<PageNumber>)` | `entry_count × IndexEntry`
//! - `internal`: `first_child(PageNumber)` | `entry_count × (Value, PageNumber)`
//!
//! `Option<PageNumber>` uses the shared `NIL` sentinel codec from `crate::index` at the disk
//! boundary.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    PageNumber, Type, Value,
    codec::{CodecError, Decode, Encode},
    index::IndexEntry,
};

/// Node discriminant byte for internal pages.
const KIND_INTERNAL: u8 = 0x01;
/// Node discriminant byte for leaf pages.
const KIND_LEAF: u8 = 0x02;

/// One logical B+Tree node stored in a single page.
///
/// This enum is the codec boundary: `Encode`/`Decode` for `BTreeNode` define the on-disk format.
#[derive(Debug, Clone)]
pub enum BTreeNode {
    /// Leaf node (holds keys and row pointers).
    Leaf(LeafNode),
    /// Internal node (holds separator keys and child pointers).
    Internal(InternalNode),
}

/// Convert a `usize` length into the `u32` we put on disk, with a uniform error.
fn count_u32(n: usize) -> Result<u32, CodecError> {
    u32::try_from(n).map_err(|_| CodecError::NumericDoesNotFit {
        value: u64::try_from(n).unwrap_or(u64::MAX),
        target: "u32",
    })
}

impl Encode for BTreeNode {
    /// Encodes a single B+Tree node into its on-disk binary format.
    ///
    /// This writes the common header first:
    /// `kind(u8)` | `parent(Option<PageNumber>)` | `entry_count(u32)` | `key_type(Type)`,
    /// followed by a kind-specific tail:
    ///
    /// - **Leaf**: `prev(Option<PageNumber>)` | `next(Option<PageNumber>)` | `entry_count ×
    ///   IndexEntry`
    /// - **Internal**: `first_child(PageNumber)` | `entry_count × (Value, PageNumber)`
    ///
    /// The `Option<PageNumber>` codec is shared across the index subsystem and uses the `NIL`
    /// sentinel at the disk boundary.
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        match self {
            BTreeNode::Leaf(l) => {
                writer.write_all(&[KIND_LEAF])?;
                l.parent.encode(writer)?;
                writer.write_u32::<LittleEndian>(count_u32(l.entries.len())?)?;
                l.key_type.encode(writer)?;

                l.prev.encode(writer)?;
                l.next.encode(writer)?;
                for entry in &l.entries {
                    entry.encode(writer)?;
                }
                Ok(())
            }
            BTreeNode::Internal(i) => {
                writer.write_all(&[KIND_INTERNAL])?;
                i.parent.encode(writer)?;
                writer.write_u32::<LittleEndian>(count_u32(i.separators.len())?)?;
                i.key_type.encode(writer)?;

                i.first_child.encode(writer)?;
                for (sep, child) in &i.separators {
                    sep.encode(writer)?;
                    child.encode(writer)?;
                }
                Ok(())
            }
        }
    }
}

impl Decode for BTreeNode {
    /// Decodes one B+Tree node written by [`Encode for BTreeNode`].
    ///
    /// The first byte is a kind discriminant. We validate it immediately so corrupted data fails
    /// fast with [`CodecError::UnknownDiscriminant`] rather than producing confusing downstream
    /// I/O errors.
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let kind = reader.read_u8()?;
        if kind != KIND_LEAF && kind != KIND_INTERNAL {
            return Err(CodecError::UnknownDiscriminant(kind));
        }

        let parent = Option::<PageNumber>::decode(reader)?;
        let entry_count = reader.read_u32::<LittleEndian>()?;
        let key_type = Type::decode(reader)?;

        match kind {
            KIND_LEAF => {
                let prev = Option::<PageNumber>::decode(reader)?;
                let next = Option::<PageNumber>::decode(reader)?;
                let entries = (0..entry_count)
                    .map(|_| IndexEntry::decode(reader))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BTreeNode::Leaf(LeafNode {
                    parent,
                    prev,
                    next,
                    key_type,
                    entries,
                }))
            }
            KIND_INTERNAL => {
                let first_child = PageNumber::decode(reader)?;
                let separators = (0..entry_count)
                    .map(|_| -> Result<(Value, PageNumber), CodecError> {
                        let sep = Value::decode(reader)?;
                        let child = PageNumber::decode(reader)?;
                        Ok((sep, child))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(BTreeNode::Internal(InternalNode {
                    parent,
                    key_type,
                    first_child,
                    separators,
                }))
            }
            _ => Err(CodecError::UnknownDiscriminant(kind)),
        }
    }
}

/// Leaf node payload.
///
/// Invariants:
/// - `entries` are sorted by key (ascending).
/// - `prev`/`next` link leaves in key order; internal nodes do not participate in this chain.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// Parent page, or `None` if this leaf is the root.
    pub parent: Option<PageNumber>,
    /// Previous leaf in key order, if any.
    pub prev: Option<PageNumber>,
    /// Next leaf in key order, if any.
    pub next: Option<PageNumber>,
    /// Declared key type for this node (all keys in this node have this type).
    pub key_type: Type,
    /// Leaf entries, sorted by key (ascending).
    pub entries: Vec<IndexEntry>,
}

/// Internal node payload.
///
/// Invariants:
/// - `separators` are sorted by the separator key (ascending).
/// - Each separator key is the minimum key present in the subtree rooted at its corresponding
///   child.
/// - The node has `separators.len() + 1` children total: `first_child` plus one per separator.
#[derive(Debug, Clone)]
pub struct InternalNode {
    /// Parent page, or `None` if this internal node is the root.
    pub parent: Option<PageNumber>,

    /// Declared key type for this node (all separators in this node have this type).
    pub key_type: Type,

    /// The leftmost child (no separator key in front of it).
    pub first_child: PageNumber,

    /// Every entry is (separator, child) where `separator` is the minimum key
    /// present anywhere under `child`. Invariant: separators are ascending.
    pub separators: Vec<(Value, PageNumber)>,
}

#[cfg(test)]
mod tests {
    //! Codec round-trips for `BTreeNode`. The interesting cases are:
    //!  - leaf vs internal both round-trip independently,
    //!  - leaf-only fields (`prev`, `next`) don't appear on internals,
    //!  - the `Option<PageNumber>` ↔ disk-sentinel boundary holds for `None`,
    //!  - the kind discriminant is rejected when unknown.
    //!
    //! These run against the in-memory codec only — no buffer pool, no disk.
    //! Tree-shape behavior (split, search) lives elsewhere; the bugs we're
    //! guarding against here are layout drift between encode and decode.
    use super::*;
    use crate::{
        codec::{Decode, Encode},
        index::{CompositeKey, IndexEntry},
        primitives::{FileId, SlotId},
    };

    fn rid(file: u64, page: u32, slot: u16) -> crate::primitives::RecordId {
        crate::primitives::RecordId::new(
            FileId::new(file),
            PageNumber::new(page),
            SlotId::new(slot).unwrap(),
        )
    }

    fn entry(k: i32, slot: u16) -> IndexEntry {
        IndexEntry::new(CompositeKey::single(Value::Int32(k)), rid(1, 0, slot))
    }

    fn roundtrip(node: &BTreeNode) -> BTreeNode {
        let mut buf = Vec::new();
        node.encode(&mut buf).unwrap();
        BTreeNode::decode(&mut buf.as_slice()).unwrap()
    }

    #[test]
    fn leaf_roundtrips_with_neighbors_and_entries() {
        let leaf = BTreeNode::Leaf(LeafNode {
            parent: Some(PageNumber::new(7)),
            prev: Some(PageNumber::new(2)),
            next: Some(PageNumber::new(4)),
            key_type: Type::Int32,
            entries: vec![entry(10, 1), entry(20, 2), entry(30, 3)],
        });

        match roundtrip(&leaf) {
            BTreeNode::Leaf(decoded) => {
                assert_eq!(decoded.parent, Some(PageNumber::new(7)));
                assert_eq!(decoded.prev, Some(PageNumber::new(2)));
                assert_eq!(decoded.next, Some(PageNumber::new(4)));
                assert_eq!(decoded.key_type, Type::Int32);
                assert_eq!(decoded.entries.len(), 3);
                assert_eq!(decoded.entries[0], entry(10, 1));
                assert_eq!(decoded.entries[2], entry(30, 3));
            }
            BTreeNode::Internal(_) => panic!("expected leaf, decoded as internal"),
        }
    }

    #[test]
    fn empty_leaf_roundtrips() {
        // Newly-allocated leaves before any insert: no entries, no siblings.
        // The header path must still produce something the decoder accepts.
        let leaf = BTreeNode::Leaf(LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_type: Type::String,
            entries: vec![],
        });

        match roundtrip(&leaf) {
            BTreeNode::Leaf(d) => {
                assert!(d.parent.is_none());
                assert!(d.prev.is_none());
                assert!(d.next.is_none());
                assert!(d.entries.is_empty());
                assert_eq!(d.key_type, Type::String);
            }
            BTreeNode::Internal(_) => panic!("expected leaf"),
        }
    }

    #[test]
    fn leaf_with_string_keys_roundtrips() {
        // Variable-width keys go through the same path as fixed-width ones,
        // but they're the case most likely to expose a length-prefix bug.
        let entries = vec![
            IndexEntry::new(
                CompositeKey::single(Value::String("alice".into())),
                rid(1, 0, 1),
            ),
            IndexEntry::new(
                CompositeKey::single(Value::String("bob".into())),
                rid(1, 0, 2),
            ),
        ];
        let leaf = BTreeNode::Leaf(LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_type: Type::String,
            entries: entries.clone(),
        });

        match roundtrip(&leaf) {
            BTreeNode::Leaf(d) => assert_eq!(d.entries, entries),
            BTreeNode::Internal(_) => panic!("expected leaf"),
        }
    }

    #[test]
    fn internal_roundtrips_with_separators() {
        let separators = vec![
            (Value::Int32(40), PageNumber::new(11)),
            (Value::Int32(70), PageNumber::new(12)),
        ];
        let internal = BTreeNode::Internal(InternalNode {
            parent: Some(PageNumber::new(1)),
            key_type: Type::Int32,
            first_child: PageNumber::new(10),
            separators: separators.clone(),
        });

        match roundtrip(&internal) {
            BTreeNode::Internal(d) => {
                assert_eq!(d.parent, Some(PageNumber::new(1)));
                assert_eq!(d.key_type, Type::Int32);
                assert_eq!(d.first_child, PageNumber::new(10));
                assert_eq!(d.separators, separators);
            }
            BTreeNode::Leaf(_) => panic!("expected internal, decoded as leaf"),
        }
    }

    #[test]
    fn internal_with_no_separators_roundtrips() {
        // Right after a "new root" promotion, an internal node has just one
        // child and no separators yet. The codec must handle entry_count = 0.
        let internal = BTreeNode::Internal(InternalNode {
            parent: None,
            key_type: Type::Int64,
            first_child: PageNumber::new(5),
            separators: vec![],
        });

        match roundtrip(&internal) {
            BTreeNode::Internal(d) => {
                assert!(d.parent.is_none());
                assert_eq!(d.first_child, PageNumber::new(5));
                assert!(d.separators.is_empty());
            }
            BTreeNode::Leaf(_) => panic!("expected internal"),
        }
    }

    #[test]
    fn unknown_kind_byte_is_rejected() {
        // Anything that isn't KIND_LEAF (0x02) or KIND_INTERNAL (0x01) should
        // surface as `UnknownDiscriminant`, not a panic and not a silent
        // mis-decode.
        let bytes = [0xFFu8, 0, 0, 0, 0];
        let err = BTreeNode::decode(&mut bytes.as_slice()).unwrap_err();
        assert!(matches!(err, CodecError::UnknownDiscriminant(0xFF)));
    }

    #[test]
    fn leaf_and_internal_have_different_lengths() {
        // Internal pages don't carry prev/next, so for the same parent and
        // entry count they must encode strictly smaller than a leaf would.
        // This is the regression test for the old bug where internal nodes
        // wrote two None sibling sentinels just to keep the layout uniform.
        let leaf = BTreeNode::Leaf(LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_type: Type::Int32,
            entries: vec![],
        });
        let internal = BTreeNode::Internal(InternalNode {
            parent: None,
            key_type: Type::Int32,
            first_child: PageNumber::new(0),
            separators: vec![],
        });

        let mut leaf_bytes = Vec::new();
        leaf.encode(&mut leaf_bytes).unwrap();
        let mut internal_bytes = Vec::new();
        internal.encode(&mut internal_bytes).unwrap();

        // Leaf adds two Option<PageNumber> (8 bytes); internal adds one
        // PageNumber (4 bytes). So the leaf must be exactly 4 bytes larger.
        assert_eq!(leaf_bytes.len(), internal_bytes.len() + 4);
    }
}

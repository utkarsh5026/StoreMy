//! B+Tree node (page) layout and codec.
//!
//! A node is either:
//! - **Leaf**: holds `(key, rid)` [`IndexEntry`] values and `prev/next` leaf pointers for range
//!   scans.
//! - **Internal**: holds separator keys and child [`PageNumber`] pointers.
//!
//! ## On-disk format
//!
//! Each B+Tree page is wrapped in the shared index-page envelope (`PageKind` tag + CRC,
//! see [`crate::index::access`]). The envelope's `BTreeLeaf` / `BTreeInternal` discriminator
//! tells the page-level decoder which variant to build, so the body itself does not carry a
//! redundant kind byte — same convention as `HashBucket`.
//!
//! Body layouts (everything after the 5-byte envelope header). Every `Vec<T>` is encoded by
//! the blanket `impl<T: Encode> Encode for Vec<T>` in `crate::codec` as a `u32` length prefix
//! followed by each element back-to-back, so the count is always adjacent to its data:
//!
//! - `leaf`: `parent(Option<PageNumber>)` | `key_types(Vec<Type>)` | `prev(Option<PageNumber>)` |
//!   `next(Option<PageNumber>)` | `entries(Vec<IndexEntry>)`
//! - `internal`: `parent(Option<PageNumber>)` | `key_types(Vec<Type>)` | `first_child(PageNumber)`
//!   | `separators(Vec<Separator>)` where each `Separator` is `key(CompositeKey)` |
//!   `child(PageNumber)`
//!
//! Both kinds carry the index's full key shape (`Vec<Type>`) so a page is decodable without
//! consulting the catalog. `Option<PageNumber>` uses the shared `NIL` sentinel codec from
//! `crate::index`.

use std::cmp::Ordering;

use crate::{
    PageNumber, Type,
    codec::{CodecError, Decode, Encode},
    index::{
        CompositeKey, ENVELOPE_HEADER_SIZE, IndexEntry, IndexError, PageKind, decode_index_page,
        encode_index_page,
    },
    storage::PAGE_SIZE,
};

/// Bytes a single `Type` occupies on disk (it encodes as a fixed-width `u32` tag).
const TYPE_ENCODED_SIZE: usize = 4;

/// Bytes the `Vec<Type>` arity prefix consumes (a `u32`).
const KEY_TYPES_PREFIX: usize = 4;

const PAGE_NUMBER_ENCODED_SIZE: usize = 4;

/// Bytes a `Vec<Type>` body of the given arity occupies on disk.
const fn key_types_size(arity: usize) -> usize {
    KEY_TYPES_PREFIX + arity * TYPE_ENCODED_SIZE
}

/// One logical B+Tree node stored in a single page.
///
/// The envelope tags pages as `BTreeLeaf` or `BTreeInternal`, so the body codec lives on the
/// concrete `LeafNode` / `InternalNode` types. This wrapper exists for code that wants to
/// hold "either kind of node" and dispatch later.
#[derive(Debug, Clone)]
pub enum BTreeNode {
    Leaf(LeafNode),
    Internal(InternalNode),
}

impl BTreeNode {
    pub fn new_leaf(
        key_types: Vec<Type>,
        parent: Option<PageNumber>,
        entries: Vec<IndexEntry>,
    ) -> Self {
        BTreeNode::Leaf(LeafNode {
            parent,
            prev: None,
            next: None,
            key_types,
            entries,
        })
    }

    /// Wraps the node body in the index-page envelope and returns a full
    /// `PAGE_SIZE` buffer ready to hand to `PageGuard::write`.
    ///
    /// Trailing bytes past the body stay zero per the envelope contract.
    /// Mirror of [`crate::index::hash::HashIndex::write_bucket`].
    pub fn to_bytes(&self) -> Result<[u8; PAGE_SIZE], CodecError> {
        let kind = match self {
            BTreeNode::Leaf(_) => PageKind::BTreeLeaf,
            BTreeNode::Internal(_) => PageKind::BTreeInternal,
        };
        encode_index_page(kind, |body| {
            let mut cursor = std::io::Cursor::new(body);
            match self {
                BTreeNode::Leaf(l) => l.encode(&mut cursor),
                BTreeNode::Internal(i) => i.encode(&mut cursor),
            }
        })
    }

    /// Verifies the envelope, then decodes the body as the variant the
    /// envelope's `PageKind` indicates. Returns [`IndexError::CorruptIndex`]
    /// for a non-B+Tree page kind.
    pub fn from_bytes(bytes: &[u8; PAGE_SIZE]) -> Result<Self, IndexError> {
        let (kind, payload) = decode_index_page(bytes)?;
        let mut reader: &[u8] = payload;
        match kind {
            PageKind::BTreeLeaf => Ok(BTreeNode::Leaf(LeafNode::decode(&mut reader)?)),
            PageKind::BTreeInternal => Ok(BTreeNode::Internal(InternalNode::decode(&mut reader)?)),
            PageKind::HashBucket => Err(IndexError::CorruptIndex(
                "expected BTree page, found HashBucket",
            )),
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
    /// Per-column declared types, in column order. Length equals the index's arity.
    pub key_types: Vec<Type>,
    /// Leaf entries, sorted by key (ascending).
    pub entries: Vec<IndexEntry>,
}

impl LeafNode {
    /// Bytes consumed by the page envelope and the leaf header (everything
    /// before the first [`IndexEntry`]).
    ///
    /// Equal to:
    /// - envelope (`ENVELOPE_HEADER_SIZE`) +
    /// - parent (4) + `entry_count` (4) +
    /// - `key_types` (4 + arity × 4) +
    /// - prev (4) + next (4)
    ///
    /// The arity dependence is why this is a method, not a constant: a
    /// composite-index leaf has a slightly larger header than a single-column
    /// one. Used by [`Self::has_space_for`] to decide whether a new entry fits
    /// before a split is required.
    pub const fn header_size(&self) -> usize {
        ENVELOPE_HEADER_SIZE
            + PAGE_NUMBER_ENCODED_SIZE
            + PAGE_NUMBER_ENCODED_SIZE
            + key_types_size(self.key_types.len())
            + PAGE_NUMBER_ENCODED_SIZE * 2
    }

    /// Bytes this leaf would occupy on disk if encoded right now (envelope +
    /// header + every entry).
    pub fn used_bytes(&self) -> usize {
        self.header_size()
            + self
                .entries
                .iter()
                .map(IndexEntry::encoded_size)
                .sum::<usize>()
    }

    /// Bytes still available before this leaf must split.
    #[inline]
    pub fn free_bytes(&self) -> usize {
        PAGE_SIZE.saturating_sub(self.used_bytes())
    }

    /// Whether `entry` will fit in this leaf without a split.
    ///
    /// The size estimate is exact because [`IndexEntry::encoded_size`] mirrors
    /// [`IndexEntry::encode`] branch-for-branch.
    pub fn has_space_for(&self, entry: &IndexEntry) -> bool {
        entry.encoded_size() <= self.free_bytes()
    }

    /// Returns the insertion position that keeps `entries` sorted, and whether
    /// a duplicate (same key AND same rid) was found at that position.
    ///
    /// # Errors
    ///
    /// Returns [`IndexError::CorruptIndex`] if any existing key in this leaf is not comparable to
    /// `entry.key`. In a well-formed tree all keys in a given node share the same declared types,
    /// so comparisons are total.
    pub fn locate_insert(&self, entry: &IndexEntry) -> Result<(usize, bool), IndexError> {
        let pos = self.binary_search_insert(entry)?;

        let mut i = pos;
        let n = self.entries.len();
        while i < n && self.entries[i].key == entry.key {
            if self.entries[i].rid == entry.rid {
                return Ok((i, true));
            }
            i += 1;
        }
        Ok((i, false))
    }

    /// Performs a binary search to find the position where `entry` should be inserted
    /// in order to keep `entries` sorted by key in ascending order.
    fn binary_search_insert(&self, entry: &IndexEntry) -> Result<usize, IndexError> {
        let mut lo = 0usize;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let ord = self.entries[mid]
                .key
                .partial_cmp(&entry.key)
                .ok_or(IndexError::CorruptIndex("btree leaf has incomparable keys"))?;
            match ord {
                Ordering::Less => lo = mid + 1,
                Ordering::Equal | Ordering::Greater => hi = mid,
            }
        }
        Ok(lo)
    }

    pub(super) fn split(
        &mut self,
        right_pn: PageNumber,
        leaf_pn: PageNumber,
    ) -> (Self, CompositeKey) {
        let mid = self.entries.len() / 2;
        let right_entries = self.entries.drain(mid..).collect::<Vec<_>>();
        let sep_key = right_entries[0].key.clone();

        let old_next = self.next;
        self.next = Some(right_pn);

        let right = Self {
            parent: self.parent,
            prev: Some(leaf_pn),
            next: old_next,
            key_types: self.key_types.clone(),
            entries: right_entries,
        };

        (right, sep_key)
    }
}

/// Body-only codec: the envelope (`kind` + `crc`) is handled by [`BTreeNode::to_bytes`] /
/// [`BTreeNode::from_bytes`] via `encode_index_page` / `decode_index_page` from the index
/// access layer. This impl writes only the leaf body.
///
/// The blanket `impl<T: Encode> Encode for Vec<T>` (in `crate::codec`) emits a `u32` length
/// prefix before each list, so `key_types` and `entries` carry their own counts — no manual
/// length writes here.
impl Encode for LeafNode {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.parent.encode(writer)?;
        self.key_types.encode(writer)?;
        self.prev.encode(writer)?;
        self.next.encode(writer)?;
        self.entries.encode(writer)?;
        Ok(())
    }
}

impl Decode for LeafNode {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let parent = Option::<PageNumber>::decode(reader)?;
        let key_types = Vec::<Type>::decode(reader)?;
        let prev = Option::<PageNumber>::decode(reader)?;
        let next = Option::<PageNumber>::decode(reader)?;
        let entries = Vec::<IndexEntry>::decode(reader)?;
        Ok(LeafNode {
            parent,
            prev,
            next,
            key_types,
            entries,
        })
    }
}

/// One separator entry in an [`InternalNode`]: a key plus the child whose subtree
/// contains every entry `>= key`.
///
/// Named pair (rather than `(CompositeKey, PageNumber)`) so call sites read as
/// `sep.key` / `sep.child` instead of `.0` / `.1`. Codec impls come from the
/// `#[derive(Encode, Decode)]` macros — the smoke test for the in-house
/// `storemy-codec-derive` crate.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Separator {
    pub key: CompositeKey,
    pub child: PageNumber,
}

impl Separator {
    pub fn new(key: CompositeKey, child: PageNumber) -> Self {
        Self { key, child }
    }

    /// Bytes this separator occupies on disk (variable-width `key` + fixed `child`).
    pub fn encoded_size(&self) -> usize {
        self.key.encoded_size() + PAGE_NUMBER_ENCODED_SIZE
    }
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

    /// Per-column declared types, in column order. Length equals the index's arity.
    pub key_types: Vec<Type>,

    /// The leftmost child (no separator key in front of it).
    pub first_child: PageNumber,

    /// Every separator's `key` is the minimum key present anywhere under its `child`.
    /// Invariant: separators are sorted ascending by `key`.
    pub separators: Vec<Separator>,
}

impl InternalNode {
    /// Bytes consumed by the page envelope and the internal-node header
    /// (everything before the first separator pair).
    ///
    /// Equal to:
    /// - envelope (`ENVELOPE_HEADER_SIZE`) +
    /// - parent (4) + `entry_count` (4) +
    /// - `key_types` (4 + arity × 4) +
    /// - `first_child` (4)
    pub fn header_size(&self) -> usize {
        ENVELOPE_HEADER_SIZE
            + PAGE_NUMBER_ENCODED_SIZE
            + PAGE_NUMBER_ENCODED_SIZE
            + key_types_size(self.key_types.len())
            + PAGE_NUMBER_ENCODED_SIZE
    }

    /// Bytes this node would occupy on disk if encoded right now.
    pub fn used_bytes(&self) -> usize {
        self.header_size()
            + self
                .separators
                .iter()
                .map(Separator::encoded_size)
                .sum::<usize>()
    }

    /// Bytes still available before this internal node must split.
    #[inline]
    pub fn free_bytes(&self) -> usize {
        PAGE_SIZE.saturating_sub(self.used_bytes())
    }

    /// Whether one more separator (with the given key) will fit without a split.
    ///
    /// The child pointer is fixed-width; only the separator key is variable.
    #[inline]
    pub fn has_space_for_separator(&self, key: &CompositeKey) -> bool {
        key.encoded_size() + PAGE_NUMBER_ENCODED_SIZE <= self.free_bytes()
    }

    pub fn insert(&mut self, key: CompositeKey, child: PageNumber) -> bool {
        if !self.has_space_for_separator(&key) {
            return false;
        }

        let pos = self
            .separators
            .partition_point(|s| s.key.partial_cmp(&key).unwrap().is_lt());
        self.separators.insert(pos, Separator::new(key, child));
        true
    }

    /// Finds the child page number that should contain the given key.
    ///
    /// - If `key` is strictly less than the first separator, returns `first_child`.
    /// - Otherwise, returns the child to the right of the largest separator `<= key`.
    pub fn find_child_for(&self, key: &CompositeKey) -> PageNumber {
        let pos = self.separators.partition_point(|s| &s.key <= key);
        if pos == 0 {
            self.first_child
        } else {
            self.separators[pos - 1].child
        }
    }

    pub fn split(&mut self) -> (Self, CompositeKey) {
        let mid = self.separators.len() / 2;
        let right_separators: Vec<_> = self.separators.drain(mid + 1..).collect();
        let Separator {
            key: push_up_key,
            child,
        } = self.separators.pop().unwrap();

        let right = Self {
            parent: self.parent,
            key_types: self.key_types.clone(),
            first_child: child,
            separators: right_separators,
        };

        (right, push_up_key)
    }
}

/// Body-only codec, same convention as [`LeafNode`]: the blanket [`Vec<T>`] impl in
/// `crate::codec` handles the count prefix, and each `Separator` carries its own
/// `Encode`/`Decode` impl above.
impl Encode for InternalNode {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), CodecError> {
        self.parent.encode(writer)?;
        self.key_types.encode(writer)?;
        self.first_child.encode(writer)?;
        self.separators.encode(writer)?;
        Ok(())
    }
}

impl Decode for InternalNode {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, CodecError> {
        let parent = Option::<PageNumber>::decode(reader)?;
        let key_types = Vec::<Type>::decode(reader)?;
        let first_child = PageNumber::decode(reader)?;
        let separators = Vec::<Separator>::decode(reader)?;
        Ok(InternalNode {
            parent,
            key_types,
            first_child,
            separators,
        })
    }
}

#[cfg(test)]
mod tests {
    //! Tests fall in three groups:
    //!  1. Body codec round-trips for `LeafNode` and `InternalNode` directly (no envelope).
    //!  2. Page-level round-trips via `BTreeNode::to_bytes` / `from_bytes` — these go through the
    //!     envelope and exercise the full `PAGE_SIZE` buffer.
    //!  3. Header / `has_space_for` math, including a regression that ties `used_bytes` to the
    //!     actual encoded size byte-for-byte.
    //!
    //! Constructor tests for `new_leaf` / `new_internal` live alongside the codec tests so
    //! "build a fresh node, encode it, decode it" runs as one chain.
    use super::*;
    use crate::{
        Value,
        codec::{Decode, Encode},
        primitives::{FileId, RecordId, SlotId},
    };

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(
            FileId::new(file),
            PageNumber::new(page),
            SlotId::new(slot).unwrap(),
        )
    }

    fn entry(k: i32, slot: u16) -> IndexEntry {
        IndexEntry::new(CompositeKey::single(Value::Int32(k)), rid(1, 0, slot))
    }

    fn name_key(last: &str, first: &str) -> CompositeKey {
        CompositeKey::new(vec![
            Value::String(last.into()),
            Value::String(first.into()),
        ])
    }

    fn body_roundtrip_leaf(node: &LeafNode) -> LeafNode {
        let mut buf = Vec::new();
        node.encode(&mut buf).unwrap();
        LeafNode::decode(&mut buf.as_slice()).unwrap()
    }

    fn body_roundtrip_internal(node: &InternalNode) -> InternalNode {
        let mut buf = Vec::new();
        node.encode(&mut buf).unwrap();
        InternalNode::decode(&mut buf.as_slice()).unwrap()
    }

    #[test]
    fn new_leaf_starts_empty_with_no_neighbors() {
        let node = BTreeNode::new_leaf(vec![Type::Int32], None, vec![]);
        match node {
            BTreeNode::Leaf(l) => {
                assert!(l.parent.is_none());
                assert!(l.prev.is_none());
                assert!(l.next.is_none());
                assert_eq!(l.key_types, vec![Type::Int32]);
                assert!(l.entries.is_empty());
            }
            BTreeNode::Internal(_) => panic!("expected leaf"),
        }
    }

    #[test]
    fn new_leaf_records_parent() {
        let parent = Some(PageNumber::new(7));
        let node = BTreeNode::new_leaf(vec![Type::String, Type::Int32], parent, vec![]);
        let BTreeNode::Leaf(l) = node else {
            panic!("expected leaf");
        };
        assert_eq!(l.parent, parent);
        assert_eq!(l.key_types, vec![Type::String, Type::Int32]);
    }

    #[test]
    fn leaf_body_roundtrips_with_neighbors_and_entries() {
        let leaf = LeafNode {
            parent: Some(PageNumber::new(7)),
            prev: Some(PageNumber::new(2)),
            next: Some(PageNumber::new(4)),
            key_types: vec![Type::Int32],
            entries: vec![entry(10, 1), entry(20, 2), entry(30, 3)],
        };

        let decoded = body_roundtrip_leaf(&leaf);
        assert_eq!(decoded.parent, leaf.parent);
        assert_eq!(decoded.prev, leaf.prev);
        assert_eq!(decoded.next, leaf.next);
        assert_eq!(decoded.key_types, leaf.key_types);
        assert_eq!(decoded.entries, leaf.entries);
    }

    #[test]
    fn empty_leaf_body_roundtrips() {
        let leaf = LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_types: vec![Type::String],
            entries: vec![],
        };
        let decoded = body_roundtrip_leaf(&leaf);
        assert!(decoded.entries.is_empty());
        assert_eq!(decoded.key_types, leaf.key_types);
    }

    #[test]
    fn leaf_body_with_composite_keys_roundtrips() {
        let entries = vec![
            IndexEntry::new(name_key("Smith", "Ada"), rid(1, 0, 1)),
            IndexEntry::new(name_key("Smith", "Bob"), rid(1, 0, 2)),
            IndexEntry::new(name_key("Turing", "Alan"), rid(1, 0, 3)),
        ];
        let leaf = LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_types: vec![Type::String, Type::String],
            entries: entries.clone(),
        };
        let decoded = body_roundtrip_leaf(&leaf);
        assert_eq!(decoded.entries, entries);
    }

    #[test]
    fn internal_body_roundtrips_with_separators() {
        let separators = vec![
            Separator::new(CompositeKey::single(Value::Int32(40)), PageNumber::new(11)),
            Separator::new(CompositeKey::single(Value::Int32(70)), PageNumber::new(12)),
        ];
        let internal = InternalNode {
            parent: Some(PageNumber::new(1)),
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(10),
            separators: separators.clone(),
        };
        let decoded = body_roundtrip_internal(&internal);
        assert_eq!(decoded.first_child, PageNumber::new(10));
        assert_eq!(decoded.separators, separators);
    }

    #[test]
    fn internal_body_with_composite_separators_roundtrips() {
        let separators = vec![
            Separator::new(name_key("Smith", "Ada"), PageNumber::new(11)),
            Separator::new(name_key("Turing", "Alan"), PageNumber::new(12)),
        ];
        let internal = InternalNode {
            parent: None,
            key_types: vec![Type::String, Type::String],
            first_child: PageNumber::new(10),
            separators: separators.clone(),
        };
        let decoded = body_roundtrip_internal(&internal);
        assert_eq!(decoded.separators, separators);
    }

    // ── Page-level (envelope) codec ─────────────────────────────────────────

    #[test]
    fn leaf_page_roundtrips_through_envelope() {
        let leaf = BTreeNode::Leaf(LeafNode {
            parent: Some(PageNumber::new(7)),
            prev: Some(PageNumber::new(2)),
            next: Some(PageNumber::new(4)),
            key_types: vec![Type::Int32],
            entries: vec![entry(10, 1), entry(20, 2)],
        });
        let bytes = leaf.to_bytes().unwrap();
        assert_eq!(bytes.len(), PAGE_SIZE);

        match BTreeNode::from_bytes(&bytes).unwrap() {
            BTreeNode::Leaf(d) => {
                assert_eq!(d.entries.len(), 2);
                assert_eq!(d.next, Some(PageNumber::new(4)));
            }
            BTreeNode::Internal(_) => panic!("envelope kind dispatched to wrong variant"),
        }
    }

    #[test]
    fn internal_page_roundtrips_through_envelope() {
        let internal = BTreeNode::Internal(InternalNode {
            parent: None,
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(10),
            separators: vec![Separator::new(
                CompositeKey::single(Value::Int32(40)),
                PageNumber::new(11),
            )],
        });
        let bytes = internal.to_bytes().unwrap();

        match BTreeNode::from_bytes(&bytes).unwrap() {
            BTreeNode::Internal(d) => {
                assert_eq!(d.first_child, PageNumber::new(10));
                assert_eq!(d.separators.len(), 1);
            }
            BTreeNode::Leaf(_) => panic!("envelope kind dispatched to wrong variant"),
        }
    }

    #[test]
    fn page_decode_rejects_hash_envelope() {
        // Stamp a HashBucket envelope with empty body, then try to decode as
        // a B+Tree page. Must fail with CorruptIndex, not silently mis-decode.
        let bytes = encode_index_page(PageKind::HashBucket, |_| Ok(())).unwrap();
        match BTreeNode::from_bytes(&bytes) {
            Err(IndexError::CorruptIndex(_)) => {}
            other => panic!("expected CorruptIndex, got {other:?}"),
        }
    }

    #[test]
    fn page_decode_propagates_crc_corruption() {
        // The envelope's CRC error must surface as IndexError (via the
        // From<StorageError> impl), not be silently swallowed by from_bytes.
        let leaf = BTreeNode::new_leaf(vec![Type::Int32], None, vec![]);
        let mut bytes = leaf.to_bytes().unwrap();
        bytes[ENVELOPE_HEADER_SIZE] ^= 0x01; // flip a payload bit
        match BTreeNode::from_bytes(&bytes) {
            Err(IndexError::Storage(_)) => {}
            other => panic!("expected Storage(ChecksumMismatch), got {other:?}"),
        }
    }

    // ── Space accounting ────────────────────────────────────────────────────

    #[test]
    fn leaf_header_size_matches_layout() {
        // arity 1: envelope(5) + parent(4) + count(4) + key_types(4 + 1*4) + prev(4) + next(4) = 29
        let l = LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_types: vec![Type::Int32],
            entries: vec![],
        };
        assert_eq!(l.header_size(), ENVELOPE_HEADER_SIZE + 4 + 4 + 8 + 4 + 4);
        assert_eq!(l.header_size(), 29);
    }

    #[test]
    fn leaf_used_bytes_matches_actual_encoded_length() {
        // The most important regression test for the size math: encode the
        // node, wrap in the envelope, and confirm the byte count we report
        // equals the byte count we'd actually write to a page header + body
        // section. If `IndexEntry::encoded_size` ever drifts from `encode`,
        // this catches it.
        let leaf = LeafNode {
            parent: Some(PageNumber::new(1)),
            prev: None,
            next: None,
            key_types: vec![Type::Int32],
            entries: vec![entry(10, 1), entry(20, 2), entry(30, 3)],
        };
        let mut body = Vec::new();
        leaf.encode(&mut body).unwrap();
        assert_eq!(leaf.used_bytes(), ENVELOPE_HEADER_SIZE + body.len());
    }

    #[test]
    fn leaf_has_space_until_full_then_does_not() {
        // Pack entries one by one. While `has_space_for` is true, pushing
        // is safe; the moment it flips to false, the next push would
        // exceed PAGE_SIZE — i.e. the leaf must split.
        let mut leaf = LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_types: vec![Type::Int32],
            entries: vec![],
        };
        let probe = entry(0, 0);
        let mut count = 0;
        while leaf.has_space_for(&probe) {
            leaf.entries.push(probe.clone());
            count += 1;
        }
        assert!(count > 100, "expected many entries to fit, got {count}");
        // After filling, the page must be at most PAGE_SIZE — never over.
        assert!(leaf.used_bytes() <= PAGE_SIZE);
        // And one more entry must not fit.
        assert!(!leaf.has_space_for(&probe));
    }

    #[test]
    fn leaf_header_grows_with_arity() {
        // Composite indexes pay extra for their key_types vec on every page.
        let single = LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_types: vec![Type::Int32],
            entries: vec![],
        };
        let composite = LeafNode {
            key_types: vec![Type::Int32, Type::String],
            ..single.clone()
        };
        // One extra Type at 4 bytes apiece.
        assert_eq!(composite.header_size(), single.header_size() + 4);
    }

    #[test]
    fn internal_header_size_matches_layout() {
        // arity 1: envelope(5) + parent(4) + count(4) + key_types(8) + first_child(4) = 25
        let i = InternalNode {
            parent: None,
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(0),
            separators: vec![],
        };
        assert_eq!(i.header_size(), ENVELOPE_HEADER_SIZE + 4 + 4 + 8 + 4);
        assert_eq!(i.header_size(), 25);
    }

    #[test]
    fn internal_used_bytes_matches_actual_encoded_length() {
        let internal = InternalNode {
            parent: None,
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(10),
            separators: vec![
                Separator::new(CompositeKey::single(Value::Int32(40)), PageNumber::new(11)),
                Separator::new(CompositeKey::single(Value::Int32(70)), PageNumber::new(12)),
            ],
        };
        let mut body = Vec::new();
        internal.encode(&mut body).unwrap();
        assert_eq!(internal.used_bytes(), ENVELOPE_HEADER_SIZE + body.len());
    }

    #[test]
    fn internal_has_space_for_separator_until_full() {
        let mut internal = InternalNode {
            parent: None,
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(0),
            separators: vec![],
        };
        let sep = CompositeKey::single(Value::Int32(0));
        let mut count = 0;
        while internal.has_space_for_separator(&sep) {
            internal
                .separators
                .push(Separator::new(sep.clone(), PageNumber::new(count + 1)));
            count += 1;
        }
        assert!(count > 100, "expected many separators to fit, got {count}");
        assert!(internal.used_bytes() <= PAGE_SIZE);
        assert!(!internal.has_space_for_separator(&sep));
    }

    #[test]
    fn internal_with_no_separators_uses_just_header() {
        // Right after a "new root" promotion: one child, no separators yet.
        let internal = InternalNode {
            parent: None,
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(5),
            separators: vec![],
        };
        assert_eq!(internal.used_bytes(), internal.header_size());
    }

    #[test]
    fn leaf_and_internal_headers_differ_by_layout() {
        // Both kinds carry parent + entry_count + key_types(arity 1).
        // Leaf adds prev + next (8 bytes); internal adds first_child (4 bytes).
        // So a leaf header is exactly 4 bytes larger than an internal one
        // for the same arity. Regression test for the old layout where
        // internal nodes wrote two None sibling sentinels just to keep the
        // layout uniform.
        let leaf = LeafNode {
            parent: None,
            prev: None,
            next: None,
            key_types: vec![Type::Int32],
            entries: vec![],
        };
        let internal = InternalNode {
            parent: None,
            key_types: vec![Type::Int32],
            first_child: PageNumber::new(0),
            separators: vec![],
        };
        assert_eq!(leaf.header_size(), internal.header_size() + 4);
    }
}

//! B+Tree node (page) layout and codec.
//!
//! A node is either:
//! - **Leaf**: holds `(key, rid)` [`IndexEntry`] values and `prev/next` leaf pointers for range
//!   scans.
//! - **Internal**: holds separator keys and child [`PageNumber`] pointers.
//!
//! ## On-disk format
//!
//! Each B+Tree page uses the universal on-disk header managed by [`TypedPage`]
//! (13 bytes: `PageKind` tag + CRC32 + `page_lsn`), then the node-specific
//! header (`H`), then the variable body (`B`).
//!
//! The `BTreeLeaf` / `BTreeInternal` discriminator in the universal header tells
//! [`BTreeNode::from_bytes`] which header and body types to decode.
//!
//! Body layouts (fields after the 13-byte universal header). Every `Vec<T>` is
//! encoded by the blanket `impl<T: Encode> Encode for Vec<T>` in `crate::codec`
//! as a `u32` length prefix followed by each element:
//!
//! - **leaf** `H = LeafNodeHeader`: `parent(4) | key_types(4 + arity×4) | prev(4) | next(4)` `B =
//!   Vec<IndexEntry>`: `count(4) | entries`
//! - **internal** `H = InternalNodeHeader`: `parent(4) | key_types(4 + arity×4) | first_child(4)`
//!   `B = Vec<Separator>`: `count(4) | separators`
//!
//! `Option<PageNumber>` uses the shared `NIL` sentinel codec from `crate::index`.

use std::cmp::Ordering;

use crate::{
    Lsn, PageNumber, Type,
    codec::{CodecError, Decode, Encode},
    index::{CompositeKey, IndexEntry, IndexError, PageKind},
    storage::{PAGE_SIZE, TypedPage, UNIVERSAL_HEADER_SIZE},
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
/// The universal header tags pages as `BTreeLeaf` or `BTreeInternal`, so
/// [`BTreeNode::from_bytes`] dispatches on the kind byte to select the right
/// [`TypedPage`] variant. This wrapper exists for code that wants to hold
/// "either kind of node" and dispatch later.
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
        BTreeNode::Leaf(LeafNode::new_leaf(parent, key_types, None, None, entries))
    }

    /// Serialises the node into a full `PAGE_SIZE` buffer via [`TypedPage::to_page_bytes`].
    ///
    /// The universal header (kind + CRC + `page_lsn`) is stamped by `TypedPage`; the
    /// node-specific header and body follow immediately. Trailing bytes stay zero.
    pub fn to_bytes(&self) -> Result<[u8; PAGE_SIZE], CodecError> {
        match self {
            BTreeNode::Leaf(l) => l.to_page_bytes(),
            BTreeNode::Internal(i) => i.page.to_page_bytes(),
        }
    }

    /// Verifies the CRC, reads the kind byte, then decodes the matching
    /// [`TypedPage`] variant. Returns [`IndexError::CorruptIndex`] for a
    /// non-B+Tree page kind.
    pub fn from_bytes(bytes: &[u8; PAGE_SIZE]) -> Result<Self, IndexError> {
        // Peek at the kind byte first so we know which TypedPage<H, B> to decode into.
        // The CRC check runs inside from_page_bytes, after the variant is selected.
        let kind = PageKind::try_from(bytes[0])?;
        match kind {
            PageKind::BTreeLeaf => {
                let page = TypedPage::<LeafNodeHeader, Vec<IndexEntry>>::from_page_bytes(bytes)?;
                Ok(BTreeNode::Leaf(page))
            }
            PageKind::BTreeInternal => {
                let page = TypedPage::<InternalNodeHeader, Vec<Separator>>::from_page_bytes(bytes)?;
                Ok(BTreeNode::Internal(InternalNode { page }))
            }
            _ => Err(IndexError::CorruptIndex(
                "expected BTree page, found non-BTree page kind",
            )),
        }
    }
}

/// Fixed per-page metadata for a B+Tree leaf node.
///
/// Stored in the [`TypedPage`] `header` slot, immediately after the 13-byte
/// universal header. On-disk: `parent(4) | key_types(4+arity×4) | prev(4) | next(4)`.
#[derive(Debug, Clone, Encode, Decode)]
pub struct LeafNodeHeader {
    /// Parent page, or `None` if this leaf is the root.
    pub parent: Option<PageNumber>,
    /// Per-column declared types, in column order.
    pub key_types: Vec<Type>,
    /// Previous leaf in key order, if any.
    pub prev: Option<PageNumber>,
    /// Next leaf in key order, if any.
    pub next: Option<PageNumber>,
}

/// Leaf node payload, backed by [`TypedPage`]`<`[`LeafNodeHeader`]`, Vec<`[`IndexEntry`]`>>`.
///
/// Invariants:
/// - `body` (entries) are sorted by key (ascending).
/// - `header.prev` / `header.next` link leaves in key order.
pub type LeafNode = TypedPage<LeafNodeHeader, Vec<IndexEntry>>;

impl TypedPage<LeafNodeHeader, Vec<IndexEntry>> {
    /// Constructs a new leaf node with the given metadata and entries.
    pub fn new_leaf(
        parent: Option<PageNumber>,
        key_types: Vec<Type>,
        prev: Option<PageNumber>,
        next: Option<PageNumber>,
        entries: Vec<IndexEntry>,
    ) -> Self {
        TypedPage::new(
            PageKind::BTreeLeaf,
            Lsn::INVALID,
            LeafNodeHeader {
                parent,
                key_types,
                prev,
                next,
            },
            entries,
        )
    }

    /// Bytes consumed by the universal header, leaf header fields, and the
    /// entries `Vec` count prefix — i.e. everything before the first
    /// [`IndexEntry`]'s bytes.
    ///
    /// Equal to:
    /// - universal header (`UNIVERSAL_HEADER_SIZE`) +
    /// - parent (4) + `key_types` (4 + arity × 4) + prev (4) + next (4) +
    /// - entry count prefix (4)
    ///
    /// The arity dependence is why this is a method, not a constant.
    pub fn header_size(&self) -> usize {
        UNIVERSAL_HEADER_SIZE
            + PAGE_NUMBER_ENCODED_SIZE // parent
            + PAGE_NUMBER_ENCODED_SIZE // entry count prefix
            + key_types_size(self.header.key_types.len())
            + PAGE_NUMBER_ENCODED_SIZE * 2 // prev + next
    }

    /// Bytes this leaf would occupy on disk if encoded right now.
    pub fn used_bytes(&self) -> usize {
        self.header_size()
            + self
                .body
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
    pub fn has_space_for(&self, entry: &IndexEntry) -> bool {
        entry.encoded_size() <= self.free_bytes()
    }

    /// Returns the insertion position that keeps `page.body` sorted, and whether
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
        let n = self.body.len();
        while i < n && self.body[i].key == entry.key {
            if self.body[i].rid == entry.rid {
                return Ok((i, true));
            }
            i += 1;
        }
        Ok((i, false))
    }

    fn binary_search_insert(&self, entry: &IndexEntry) -> Result<usize, IndexError> {
        let mut lo = 0usize;
        let mut hi = self.body.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let ord = self.body[mid]
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
        let mid = self.body.len() / 2;
        let right_entries = self.body.drain(mid..).collect::<Vec<_>>();
        let sep_key = right_entries[0].key.clone();
        tracing::debug!(left = ?leaf_pn, right = ?right_pn, midpoint_key = ?sep_key, "btree: leaf split");

        let old_next = self.header.next;
        self.header.next = Some(right_pn);

        let right = LeafNode::new_leaf(
            self.header.parent,
            self.header.key_types.clone(),
            Some(leaf_pn),
            old_next,
            right_entries,
        );

        (right, sep_key)
    }
}

// ── InternalNode ──────────────────────────────────────────────────────────────

/// Fixed per-page metadata for a B+Tree internal node.
///
/// Stored in the [`TypedPage`] `header` slot, immediately after the 13-byte
/// universal header. On-disk: `parent(4) | key_types(4+arity×4) | first_child(4)`.
#[derive(Debug, Clone, Encode, Decode)]
pub struct InternalNodeHeader {
    /// Parent page, or `None` if this internal node is the root.
    pub parent: Option<PageNumber>,
    /// Per-column declared types, in column order.
    pub key_types: Vec<Type>,
    /// The leftmost child (no separator key in front of it).
    pub first_child: PageNumber,
}

/// One separator entry in an [`InternalNode`]: a key plus the child whose subtree
/// contains every entry `>= key`.
///
/// Codec impls come from `#[derive(Encode, Decode)]`.
#[derive(Debug, Clone, PartialEq, Encode, Decode)]
pub struct Separator {
    pub key: CompositeKey,
    pub child: PageNumber,
}

impl Separator {
    pub fn new(key: CompositeKey, child: PageNumber) -> Self {
        Self { key, child }
    }

    /// Bytes this separator occupies on disk.
    pub fn encoded_size(&self) -> usize {
        self.key.encoded_size() + PAGE_NUMBER_ENCODED_SIZE
    }
}

/// Internal node payload, backed by
/// [`TypedPage`]`<`[`InternalNodeHeader`]`, Vec<`[`Separator`]`>>`.
///
/// Invariants:
/// - `page.body` (separators) are sorted by key (ascending).
/// - Each separator key is the minimum key in the subtree rooted at its child.
/// - The node has `page.body.len() + 1` children: `page.header.first_child` plus one per separator.
#[derive(Debug, Clone)]
pub struct InternalNode {
    pub page: TypedPage<InternalNodeHeader, Vec<Separator>>,
}

impl InternalNode {
    /// Constructs a new internal node with the given metadata and separators.
    pub fn new(
        parent: Option<PageNumber>,
        key_types: Vec<Type>,
        first_child: PageNumber,
        separators: Vec<Separator>,
    ) -> Self {
        Self {
            page: TypedPage::new(
                PageKind::BTreeInternal,
                Lsn::INVALID,
                InternalNodeHeader {
                    parent,
                    key_types,
                    first_child,
                },
                separators,
            ),
        }
    }

    /// Bytes consumed by the universal header, internal-node header fields, and
    /// the separators `Vec` count prefix — everything before the first
    /// [`Separator`]'s bytes.
    ///
    /// Equal to:
    /// - universal header (`UNIVERSAL_HEADER_SIZE`) +
    /// - parent (4) + `key_types` (4 + arity × 4) + `first_child` (4) +
    /// - separator count prefix (4)
    pub fn header_size(&self) -> usize {
        UNIVERSAL_HEADER_SIZE
            + PAGE_NUMBER_ENCODED_SIZE // parent
            + PAGE_NUMBER_ENCODED_SIZE // separator count prefix
            + key_types_size(self.page.header.key_types.len())
            + PAGE_NUMBER_ENCODED_SIZE // first_child
    }

    /// Bytes this node would occupy on disk if encoded right now.
    pub fn used_bytes(&self) -> usize {
        self.header_size()
            + self
                .page
                .body
                .iter()
                .map(Separator::encoded_size)
                .sum::<usize>()
    }

    /// Bytes still available before this internal node must split.
    #[inline]
    pub fn free_bytes(&self) -> usize {
        PAGE_SIZE.saturating_sub(self.used_bytes())
    }

    /// Whether one more separator with the given key will fit without a split.
    #[inline]
    pub fn has_space_for_separator(&self, key: &CompositeKey) -> bool {
        key.encoded_size() + PAGE_NUMBER_ENCODED_SIZE <= self.free_bytes()
    }

    pub fn insert(&mut self, key: CompositeKey, child: PageNumber) -> bool {
        if !self.has_space_for_separator(&key) {
            return false;
        }

        let pos = self
            .page
            .body
            .partition_point(|s| s.key.partial_cmp(&key).unwrap().is_lt());
        self.page.body.insert(pos, Separator::new(key, child));
        true
    }

    /// Finds the child page number that should contain the given key.
    ///
    /// - If `key` is strictly less than the first separator, returns `first_child`.
    /// - Otherwise, returns the child to the right of the largest separator `<= key`.
    pub fn find_child_for(&self, key: &CompositeKey) -> PageNumber {
        let pos = self.page.body.partition_point(|s| &s.key <= key);
        if pos == 0 {
            self.page.header.first_child
        } else {
            self.page.body[pos - 1].child
        }
    }

    pub fn split(&mut self) -> (Self, CompositeKey) {
        let mid = self.page.body.len() / 2;
        let right_separators: Vec<_> = self.page.body.drain(mid + 1..).collect();
        let Separator {
            key: push_up_key,
            child,
        } = self.page.body.pop().unwrap();
        tracing::debug!(promoted_key = ?push_up_key, "btree: internal node split");

        let right = InternalNode::new(
            self.page.header.parent,
            self.page.header.key_types.clone(),
            child,
            right_separators,
        );

        (right, push_up_key)
    }
}

#[cfg(test)]
mod tests {
    //! Tests fall in three groups:
    //!  1. Page-level round-trips via `BTreeNode::to_bytes` / `from_bytes`.
    //!  2. Header / `has_space_for` math, including a regression tying `used_bytes` to the actual
    //!     encoded byte count.
    //!  3. Structural / constructor tests.
    use super::*;
    use crate::{
        Value,
        index::encode_index_page,
        primitives::{FileId, RecordId, SlotId},
        storage::StorageError,
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

    /// Round-trips a `LeafNode` through `BTreeNode::to_bytes` / `from_bytes`.
    fn page_roundtrip_leaf(node: &LeafNode) -> LeafNode {
        let bytes = BTreeNode::Leaf(node.clone()).to_bytes().unwrap();
        match BTreeNode::from_bytes(&bytes).unwrap() {
            BTreeNode::Leaf(l) => l,
            BTreeNode::Internal(_) => panic!("expected leaf variant"),
        }
    }

    /// Round-trips an `InternalNode` through `BTreeNode::to_bytes` / `from_bytes`.
    fn page_roundtrip_internal(node: &InternalNode) -> InternalNode {
        let bytes = BTreeNode::Internal(node.clone()).to_bytes().unwrap();
        match BTreeNode::from_bytes(&bytes).unwrap() {
            BTreeNode::Internal(i) => i,
            BTreeNode::Leaf(_) => panic!("expected internal variant"),
        }
    }

    #[test]
    fn new_leaf_starts_empty_with_no_neighbors() {
        let node = BTreeNode::new_leaf(vec![Type::Int32], None, vec![]);
        match node {
            BTreeNode::Leaf(l) => {
                assert!(l.header.parent.is_none());
                assert!(l.header.prev.is_none());
                assert!(l.header.next.is_none());
                assert_eq!(l.header.key_types, vec![Type::Int32]);
                assert!(l.body.is_empty());
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
        assert_eq!(l.header.parent, parent);
        assert_eq!(l.header.key_types, vec![Type::String, Type::Int32]);
    }

    #[test]
    fn leaf_page_roundtrips_with_neighbors_and_entries() {
        let leaf = LeafNode::new_leaf(
            Some(PageNumber::new(7)),
            vec![Type::Int32],
            Some(PageNumber::new(2)),
            Some(PageNumber::new(4)),
            vec![entry(10, 1), entry(20, 2), entry(30, 3)],
        );

        let decoded = page_roundtrip_leaf(&leaf);
        assert_eq!(decoded.header.parent, leaf.header.parent);
        assert_eq!(decoded.header.prev, leaf.header.prev);
        assert_eq!(decoded.header.next, leaf.header.next);
        assert_eq!(decoded.header.key_types, leaf.header.key_types);
        assert_eq!(decoded.body, leaf.body);
    }

    #[test]
    fn empty_leaf_page_roundtrips() {
        let leaf = LeafNode::new_leaf(None, vec![Type::String], None, None, vec![]);
        let decoded = page_roundtrip_leaf(&leaf);
        assert!(decoded.body.is_empty());
        assert_eq!(decoded.header.key_types, leaf.header.key_types);
    }

    #[test]
    fn leaf_page_with_composite_keys_roundtrips() {
        let entries = vec![
            IndexEntry::new(name_key("Smith", "Ada"), rid(1, 0, 1)),
            IndexEntry::new(name_key("Smith", "Bob"), rid(1, 0, 2)),
            IndexEntry::new(name_key("Turing", "Alan"), rid(1, 0, 3)),
        ];
        let leaf = LeafNode::new_leaf(
            None,
            vec![Type::String, Type::String],
            None,
            None,
            entries.clone(),
        );
        let decoded = page_roundtrip_leaf(&leaf);
        assert_eq!(decoded.body, entries);
    }

    #[test]
    fn internal_page_roundtrips_with_separators() {
        let separators = vec![
            Separator::new(CompositeKey::single(Value::Int32(40)), PageNumber::new(11)),
            Separator::new(CompositeKey::single(Value::Int32(70)), PageNumber::new(12)),
        ];
        let internal = InternalNode::new(
            Some(PageNumber::new(1)),
            vec![Type::Int32],
            PageNumber::new(10),
            separators.clone(),
        );
        let decoded = page_roundtrip_internal(&internal);
        assert_eq!(decoded.page.header.first_child, PageNumber::new(10));
        assert_eq!(decoded.page.body, separators);
    }

    #[test]
    fn internal_page_with_composite_separators_roundtrips() {
        let separators = vec![
            Separator::new(name_key("Smith", "Ada"), PageNumber::new(11)),
            Separator::new(name_key("Turing", "Alan"), PageNumber::new(12)),
        ];
        let internal = InternalNode::new(
            None,
            vec![Type::String, Type::String],
            PageNumber::new(10),
            separators.clone(),
        );
        let decoded = page_roundtrip_internal(&internal);
        assert_eq!(decoded.page.body, separators);
    }

    // ── Page-level (envelope) codec ─────────────────────────────────────────

    #[test]
    fn leaf_page_roundtrips_through_envelope() {
        let leaf = BTreeNode::Leaf(LeafNode::new_leaf(
            Some(PageNumber::new(7)),
            vec![Type::Int32],
            Some(PageNumber::new(2)),
            Some(PageNumber::new(4)),
            vec![entry(10, 1), entry(20, 2)],
        ));
        let bytes = leaf.to_bytes().unwrap();
        assert_eq!(bytes.len(), PAGE_SIZE);

        match BTreeNode::from_bytes(&bytes).unwrap() {
            BTreeNode::Leaf(d) => {
                assert_eq!(d.body.len(), 2);
                assert_eq!(d.header.next, Some(PageNumber::new(4)));
            }
            BTreeNode::Internal(_) => panic!("envelope kind dispatched to wrong variant"),
        }
    }

    #[test]
    fn internal_page_roundtrips_through_envelope() {
        let internal = BTreeNode::Internal(InternalNode::new(
            None,
            vec![Type::Int32],
            PageNumber::new(10),
            vec![Separator::new(
                CompositeKey::single(Value::Int32(40)),
                PageNumber::new(11),
            )],
        ));
        let bytes = internal.to_bytes().unwrap();

        match BTreeNode::from_bytes(&bytes).unwrap() {
            BTreeNode::Internal(d) => {
                assert_eq!(d.page.header.first_child, PageNumber::new(10));
                assert_eq!(d.page.body.len(), 1);
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
        // The CRC error from TypedPage::from_page_bytes must surface as
        // IndexError::Storage, not be silently swallowed.
        let leaf = BTreeNode::new_leaf(vec![Type::Int32], None, vec![]);
        let mut bytes = leaf.to_bytes().unwrap();
        bytes[UNIVERSAL_HEADER_SIZE] ^= 0x01; // flip a non-CRC payload bit
        match BTreeNode::from_bytes(&bytes) {
            Err(IndexError::Storage(_)) => {}
            other => panic!("expected Storage(ChecksumMismatch), got {other:?}"),
        }
    }

    // ── Space accounting ────────────────────────────────────────────────────

    #[test]
    fn leaf_header_size_matches_layout() {
        // arity 1: universal(13) + parent(4) + entry_count(4) + key_types(4+1×4) + prev(4) +
        // next(4) = 37
        let l = LeafNode::new_leaf(None, vec![Type::Int32], None, None, vec![]);
        assert_eq!(l.header_size(), UNIVERSAL_HEADER_SIZE + 4 + 4 + 8 + 4 + 4);
        assert_eq!(l.header_size(), 37);
    }

    #[test]
    fn leaf_used_bytes_matches_actual_encoded_length() {
        // Encode H and B separately and verify their combined length matches
        // used_bytes() minus the universal header.
        let leaf = LeafNode::new_leaf(
            Some(PageNumber::new(1)),
            vec![Type::Int32],
            None,
            None,
            vec![entry(10, 1), entry(20, 2), entry(30, 3)],
        );
        let mut h_buf = Vec::new();
        leaf.header.encode(&mut h_buf).unwrap();
        let mut b_buf = Vec::new();
        leaf.body.encode(&mut b_buf).unwrap();
        assert_eq!(
            leaf.used_bytes(),
            UNIVERSAL_HEADER_SIZE + h_buf.len() + b_buf.len()
        );
    }

    #[test]
    fn leaf_has_space_until_full_then_does_not() {
        let mut leaf = LeafNode::new_leaf(None, vec![Type::Int32], None, None, vec![]);
        let probe = entry(0, 0);
        let mut count = 0;
        while leaf.has_space_for(&probe) {
            leaf.body.push(probe.clone());
            count += 1;
        }
        assert!(count > 100, "expected many entries to fit, got {count}");
        assert!(leaf.used_bytes() <= PAGE_SIZE);
        assert!(!leaf.has_space_for(&probe));
    }

    #[test]
    fn leaf_header_grows_with_arity() {
        let single = LeafNode::new_leaf(None, vec![Type::Int32], None, None, vec![]);
        let composite =
            LeafNode::new_leaf(None, vec![Type::Int32, Type::String], None, None, vec![]);
        // One extra Type at 4 bytes apiece.
        assert_eq!(composite.header_size(), single.header_size() + 4);
    }

    #[test]
    fn internal_header_size_matches_layout() {
        // arity 1: universal(13) + parent(4) + sep_count(4) + key_types(8) + first_child(4) = 33
        let i = InternalNode::new(None, vec![Type::Int32], PageNumber::new(0), vec![]);
        assert_eq!(i.header_size(), UNIVERSAL_HEADER_SIZE + 4 + 4 + 8 + 4);
        assert_eq!(i.header_size(), 33);
    }

    #[test]
    fn internal_used_bytes_matches_actual_encoded_length() {
        let internal = InternalNode::new(None, vec![Type::Int32], PageNumber::new(10), vec![
            Separator::new(CompositeKey::single(Value::Int32(40)), PageNumber::new(11)),
            Separator::new(CompositeKey::single(Value::Int32(70)), PageNumber::new(12)),
        ]);
        let mut h_buf = Vec::new();
        internal.page.header.encode(&mut h_buf).unwrap();
        let mut b_buf = Vec::new();
        internal.page.body.encode(&mut b_buf).unwrap();
        assert_eq!(
            internal.used_bytes(),
            UNIVERSAL_HEADER_SIZE + h_buf.len() + b_buf.len()
        );
    }

    #[test]
    fn internal_has_space_for_separator_until_full() {
        let mut internal = InternalNode::new(None, vec![Type::Int32], PageNumber::new(0), vec![]);
        let sep = CompositeKey::single(Value::Int32(0));
        let mut count = 0;
        while internal.has_space_for_separator(&sep) {
            internal
                .page
                .body
                .push(Separator::new(sep.clone(), PageNumber::new(count + 1)));
            count += 1;
        }
        assert!(count > 100, "expected many separators to fit, got {count}");
        assert!(internal.used_bytes() <= PAGE_SIZE);
        assert!(!internal.has_space_for_separator(&sep));
    }

    #[test]
    fn internal_with_no_separators_uses_just_header() {
        let internal = InternalNode::new(None, vec![Type::Int32], PageNumber::new(5), vec![]);
        assert_eq!(internal.used_bytes(), internal.header_size());
    }

    #[test]
    fn leaf_and_internal_headers_differ_by_layout() {
        // Leaf: universal + parent + entry_count + key_types(1) + prev + next = 37
        // Internal: universal + parent + sep_count + key_types(1) + first_child = 33
        // Leaf header is 4 bytes larger (prev+next = 8 vs first_child = 4).
        let leaf = LeafNode::new_leaf(None, vec![Type::Int32], None, None, vec![]);
        let internal = InternalNode::new(None, vec![Type::Int32], PageNumber::new(0), vec![]);
        assert_eq!(leaf.header_size(), internal.header_size() + 4);
    }

    #[test]
    fn page_decode_rejects_blank_page() {
        let blank = [0u8; PAGE_SIZE];
        // Blank page kind byte is 0x00 = unknown → CorruptIndex or Storage error.
        assert!(BTreeNode::from_bytes(&blank).is_err());
    }

    #[test]
    fn page_decode_rejects_crc_mismatch() {
        let leaf = BTreeNode::new_leaf(vec![Type::Int32], None, vec![entry(1, 0)]);
        let mut bytes = leaf.to_bytes().unwrap();
        bytes[PAGE_SIZE - 1] ^= 0xFF; // flip last byte → CRC mismatch
        match BTreeNode::from_bytes(&bytes) {
            Err(IndexError::Storage(StorageError::ChecksumMismatch { .. })) => {}
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
    }
}

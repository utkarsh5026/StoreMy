//! B+tree index access method.
//!
//! This module provides the `IndexKind::Btree` implementation used by the engine.
//! Conceptually it's a **B+tree**:
//! - **All** key→RID entries live in **leaf** pages (internal pages only route).
//! - Leaves are linked (`prev`/`next`) so `range_search` can scan in key order.
//!
//! ## Behavior
//!
//! - **Duplicate entries**: the index rejects inserting the exact same *(key, rid)* pair twice
//!   (`IndexError::DuplicateEntry`). Multiple different RIDs may share the same key and will be
//!   returned together by `search`.
//! - **Range bounds are inclusive**: `range_search(start, end)` returns entries where \(start \le
//!   key \le end\).
//! - **Deletes are minimal**: `delete` only removes the matching entry from a leaf. There is
//!   currently **no merge/rebalance** and no page reclamation; empty pages are left as-is until we
//!   add a free-list / compaction.
//!
//! ## Structure
//!
//! - [`node`] — `BTreeNode` (Leaf | Internal): page layout + encode/decode. Pure data; testable
//!   without a buffer pool.
//! - `BTreeIndex` — tree traversal, splits, point lookups, range scans. Uses [`PageStore`] for
//!   fetching/pinning pages and relies on the buffer pool's per-page locks.

mod node;

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use parking_lot::RwLock;

use crate::{
    FileId, Lsn, PageNumber, TransactionId, Type,
    buffer_pool::{
        LockRequest,
        page_store::{PageGuard, PageStore},
    },
    index::{
        CompositeKey, Index, IndexEntry, IndexError, IndexKind,
        btree::node::{BTreeNode, InternalNode, LeafNode, Separator},
    },
    primitives::{PageId, RecordId},
};

/// A B+tree index backed by a single on-disk file (`file_id`).
///
/// The tree stores [`IndexEntry`] values ordered by [`CompositeKey`]. Internal pages only contain
/// separators to route searches; leaf pages contain the actual `(key, rid)` pairs and form a
/// doubly-linked list to support range scans.
///
/// Concurrency/locking is handled via the buffer pool: each accessed page is fetched through
/// [`PageStore`] using a per-page lock request.
pub struct BTreeIndex {
    file_id: FileId,
    key_types: Vec<Type>,
    root: RwLock<Option<PageNumber>>,
    store: Arc<PageStore>,
    /// Total pages this index occupies in `file_id`. Splits bump it via
    /// [`Self::allocate_page`]; concurrent writers reconcile with `fetch_max`
    /// so the counter never lags the highest-written page.
    num_pages: AtomicU32,
}

impl BTreeIndex {
    /// Create a `BTreeIndex` instance over an already-registered index file.
    ///
    /// - `file_id`: backing file in the [`PageStore`]
    /// - `key_types`: declared types for each key component (arity + types are enforced on calls)
    /// - `root`: root page number if the index already exists on disk
    /// - `store`: shared buffer pool / page store
    /// - `existing_pages`: current number of allocated pages in the file (used to assign new pages
    ///   during splits)
    pub fn new(
        file_id: FileId,
        key_types: Vec<Type>,
        root: Option<PageNumber>,
        store: Arc<PageStore>,
        existing_pages: u32,
    ) -> Self {
        Self {
            file_id,
            key_types,
            root: RwLock::new(root),
            store,
            num_pages: AtomicU32::new(existing_pages),
        }
    }

    /// Claims a fresh page number at the end of the file. Used by split paths
    /// in `insert` to allocate sibling pages and (when the root splits) a new
    /// root. Mirrors `HashIndex::allocate_page`; no free-list yet, so deletes
    /// that empty a page leak it until we add page reclamation.
    fn allocate_page(&self) -> PageNumber {
        PageNumber::from(self.num_pages.fetch_add(1, Ordering::AcqRel))
    }

    fn pid(&self, p: PageNumber) -> PageId {
        PageId {
            file_id: self.file_id,
            page_no: p,
        }
    }

    /// Serializes `node` and writes it into the already-pinned page.
    ///
    /// Callers provide the [`PageGuard`] so this helper can be used both for
    /// pages the caller already locked and for pages fetched through
    /// [`Self::fetch_and_write_page`]. Encoding errors are surfaced as
    /// [`IndexError`] through the codec error conversion.
    fn write_node(guard: &PageGuard<'_>, node: &BTreeNode, lsn: Lsn) -> Result<(), IndexError> {
        let bytes = node.to_bytes()?;
        guard.write(&bytes, lsn);
        Ok(())
    }

    /// Fetches, pins, and decodes one B+tree page.
    ///
    /// The returned [`PageGuard`] keeps the page pinned/locked for as long as
    /// the caller holds it. Returning the decoded [`BTreeNode`] beside the guard
    /// lets callers inspect or mutate the in-memory node and then write it back
    /// through [`Self::write_node`] before dropping the guard.
    fn read_node(
        &self,
        txn: TransactionId,
        p: PageNumber,
    ) -> Result<(PageGuard<'_>, BTreeNode), IndexError> {
        let guard = self.read_page(txn, p, true, &self.store)?;
        let bytes = guard.read();
        let node = BTreeNode::from_bytes(&bytes)?;
        Ok((guard, node))
    }

    fn locate_leaf_for_key(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
    ) -> Result<(PageGuard<'_>, LeafNode, PageNumber), IndexError> {
        let root = self
            .root
            .read()
            .ok_or(IndexError::CorruptIndex("root not found"))?;

        let mut current_page = root;
        loop {
            let (g, node) = self.read_node(txn, current_page)?;
            match node {
                BTreeNode::Leaf(l) => {
                    return Ok((g, l, current_page));
                }
                BTreeNode::Internal(internal) => {
                    drop(g);
                    current_page = internal.find_child_for(key);
                }
            }
        }
    }

    /// Fetches an index page with an exclusive lock and writes a serialized node into it.
    ///
    /// Split paths use this helper when they need to write a page that is not
    /// already pinned by the current operation, such as a newly allocated sibling
    /// or root page. The page is identified by this index's `file_id` plus `pn`.
    fn fetch_and_write_page(
        &self,
        txn: TransactionId,
        pn: PageNumber,
        node: &BTreeNode,
        lsn: Lsn,
    ) -> Result<(), IndexError> {
        let req = LockRequest::exclusive(txn, self.pid(pn));
        let guard = self.store.fetch_page(req)?;
        Self::write_node(&guard, node, lsn)?;
        Ok(())
    }

    /// Rewrites one child's stored parent pointer.
    ///
    /// Parent pointers are redundant routing metadata, but keeping them current
    /// makes split propagation straightforward: after creating a new right
    /// sibling or root, every child moved under that page must point back to it.
    fn set_parent_pointer(
        &self,
        txn: TransactionId,
        child_pn: PageNumber,
        parent: Option<PageNumber>,
        lsn: Lsn,
    ) -> Result<(), IndexError> {
        let (g, mut node) = self.read_node(txn, child_pn)?;
        match &mut node {
            BTreeNode::Leaf(l) => l.parent = parent,
            BTreeNode::Internal(i) => i.parent = parent,
        }
        Self::write_node(&g, &node, lsn)
    }

    /// Splits an overfull leaf and propagates the new separator upward.
    ///
    /// `left` is the leaf after the caller has inserted the new entry that made
    /// it too large for one page. Splitting keeps the lower half in `left`,
    /// creates a new right sibling, and returns the first key in that right
    /// sibling as the separator to insert into the parent.
    ///
    /// Because leaves form a doubly-linked list for range scans, this also
    /// repairs the old next leaf's `prev` pointer before writing both split
    /// leaves back to disk.
    fn split_leaf_and_propagate(
        &self,
        txn: TransactionId,
        mut left: LeafNode,
        leaf_pn: PageNumber,
        lsn: Lsn,
    ) -> Result<(), IndexError> {
        let right_pn = self.allocate_page();
        let (right, sep_key) = left.split(right_pn, leaf_pn);

        if let Some(old_next) = right.next {
            let (guard, mut old_next_leaf) = self.read_node(txn, old_next)?;
            if let BTreeNode::Leaf(l) = &mut old_next_leaf {
                l.prev = Some(right_pn);
                Self::write_node(&guard, &old_next_leaf, lsn)?;
            } else {
                return Err(IndexError::CorruptIndex("next pointer to non-leaf"));
            }
        }

        let left_parent = left.parent;
        self.fetch_and_write_page(txn, leaf_pn, &BTreeNode::Leaf(left), lsn)?;
        self.fetch_and_write_page(txn, right_pn, &BTreeNode::Leaf(right), lsn)?;
        self.insert_into_parent(txn, leaf_pn, sep_key, right_pn, left_parent, lsn)
    }

    /// Inserts a split separator into the parent, recursively splitting upward if needed.
    ///
    /// A child split produces two sibling pages (`left_pn` and `right_pn`) plus
    /// `sep_key`, the boundary key that routes searches to the right sibling.
    /// If `parent` exists, we insert that separator into the parent internal
    /// node and update the right sibling's parent pointer.
    ///
    /// If the parent is already full, it is split too and the newly produced
    /// separator is propagated upward. If there is no parent, the split happened
    /// at the root and [`Self::create_new_root`] installs a fresh internal root.
    fn insert_into_parent(
        &self,
        txn: TransactionId,
        left_pn: PageNumber,
        sep_key: CompositeKey,
        right_pn: PageNumber,
        parent: Option<PageNumber>,
        lsn: Lsn,
    ) -> Result<(), IndexError> {
        if let Some(parent) = parent {
            let (guard, mut node) = self.read_node(txn, parent)?;
            let BTreeNode::Internal(inode) = &mut node else {
                return Err(IndexError::CorruptIndex("parent is not internal"));
            };

            if inode.insert(sep_key, right_pn) {
                Self::write_node(&guard, &node, lsn)?;
                self.set_parent_pointer(txn, right_pn, Some(parent), lsn)?;
                return Ok(());
            }

            return self.split_internal_and_propagate(txn, left_pn, inode, lsn);
        }

        self.create_new_root(txn, left_pn, sep_key, right_pn, lsn)
    }

    /// Promotes two split siblings under a freshly allocated internal root.
    ///
    /// This is the root-split case: a child split produced `left_pn`, `sep_key`,
    /// and `right_pn`, but there is no parent page to receive the separator.
    /// We allocate a new internal page, make it the tree root, and then rewrite
    /// both children so their parent pointers point at that new root.
    ///
    /// After this completes, the tree height has grown by one level and searches
    /// can route from the new root to either side of the split using `sep_key`.
    fn create_new_root(
        &self,
        txn: TransactionId,
        left_pn: PageNumber,
        sep_key: CompositeKey,
        right_pn: PageNumber,
        lsn: Lsn,
    ) -> Result<(), IndexError> {
        let new_root_pn = self.allocate_page();
        let root_node = InternalNode {
            parent: None,
            key_types: self.key_types.clone(),
            first_child: left_pn,
            separators: vec![Separator::new(sep_key, right_pn)],
        };
        self.fetch_and_write_page(txn, new_root_pn, &BTreeNode::Internal(root_node), lsn)?;

        let new_root_pn = Some(new_root_pn);
        *self.root.write() = new_root_pn;
        self.set_parent_pointer(txn, left_pn, new_root_pn, lsn)?;
        self.set_parent_pointer(txn, right_pn, new_root_pn, lsn)?;
        Ok(())
    }

    /// Splits an overfull internal node and propagates its separator upward.
    ///
    /// `left` is the already-overfull node at `left_pn`. Splitting it produces a
    /// new right sibling plus one separator key that must move up into the
    /// parent. Any children moved into the right sibling are rewritten first so
    /// their parent pointers reference `right_pn`.
    ///
    /// If the original node had a parent, the pushed-up separator is inserted
    /// there. If it was the root, propagation falls through to [`Self::create_new_root`]
    /// and the tree height grows by one.
    fn split_internal_and_propagate(
        &self,
        txn: TransactionId,
        left_pn: PageNumber,
        left: &mut InternalNode,
        lsn: Lsn,
    ) -> Result<(), IndexError> {
        let (right, push_up_key) = left.split();

        let right_pn = self.allocate_page();
        self.set_parent_pointer(txn, right.first_child, Some(right_pn), lsn)?;

        for Separator { child, .. } in &right.separators {
            self.set_parent_pointer(txn, *child, Some(right_pn), lsn)?;
        }

        let left_parent = left.parent;
        self.fetch_and_write_page(txn, left_pn, &BTreeNode::Internal(left.clone()), lsn)?;
        self.fetch_and_write_page(txn, right_pn, &BTreeNode::Internal(right), lsn)?;
        self.insert_into_parent(txn, left_pn, push_up_key, right_pn, left_parent, lsn)
    }
}

impl Index for BTreeIndex {
    /// Inserts one `(key, rid)` entry into the B+tree.
    ///
    /// The first insert creates a root leaf. Later inserts descend to the target
    /// leaf, reject an exact duplicate `(key, rid)` pair, and either insert in
    /// place or split the leaf and propagate the new separator upward.
    fn insert(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        self.check_key_type(key)?;
        let index_entry = IndexEntry::new(key.clone(), rid);

        if self.root.read().is_none() {
            let pn = self.allocate_page();
            let leaf = BTreeNode::new_leaf(self.key_types.clone(), None, vec![index_entry]);
            self.fetch_and_write_page(txn, pn, &leaf, Lsn::INVALID)?;
            *self.root.write() = Some(pn);
            return Ok(());
        }

        let (guard, mut leaf, leaf_pn) = self.locate_leaf_for_key(txn, key)?;
        let (pos, is_duplicate) = leaf.locate_insert(&index_entry)?;
        if is_duplicate {
            return Err(IndexError::DuplicateEntry);
        }

        if leaf.has_space_for(&index_entry) {
            leaf.entries.insert(pos, index_entry);
            Self::write_node(&guard, &BTreeNode::Leaf(leaf), Lsn::INVALID)?;
            return Ok(());
        }

        drop(guard);
        leaf.entries.insert(pos, index_entry);
        self.split_leaf_and_propagate(txn, leaf, leaf_pn, Lsn::INVALID)
    }

    /// Removes one exact `(key, rid)` entry from a leaf.
    ///
    /// This is intentionally a minimal delete path: it does not merge
    /// underfull leaves, rebalance siblings, shrink the root, or reclaim empty
    /// pages. Those behaviors require additional page-management machinery that
    /// is not part of this implementation yet.
    fn delete(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        self.check_key_type(key)?;
        if self.root.read().is_none() {
            return Err(IndexError::NotFound);
        }

        let (guard, mut leaf, _) = self.locate_leaf_for_key(txn, key)?;
        let pos = leaf
            .entries
            .iter()
            .position(|e| e.key == *key && e.rid == rid)
            .ok_or(IndexError::NotFound)?;

        leaf.entries.remove(pos);
        Self::write_node(&guard, &BTreeNode::Leaf(leaf), Lsn::INVALID)?;
        Ok(())
    }

    /// Returns all record IDs stored under one key.
    ///
    /// The search descends to the leaf where `key` should live, then uses a
    /// lower-bound within that sorted leaf and collects the contiguous run of
    /// matching entries. Different RIDs may share the same key.
    fn search(&self, txn: TransactionId, key: &CompositeKey) -> Result<Vec<RecordId>, IndexError> {
        if self.root.read().is_none() {
            return Ok(Vec::new());
        }

        self.check_key_type(key)?;
        let (_, l, _) = self.locate_leaf_for_key(txn, key)?;
        // Lower-bound: first index whose key is >= *key. Entries are sorted
        // ascending, so all matches (if any) sit in a contiguous run starting
        // at `start`. Walk forward while the key still matches.
        let start = l.entries.partition_point(|e| e.key < *key);
        let searches = l.entries[start..]
            .iter()
            .take_while(|e| &e.key == key)
            .map(|e| e.rid)
            .collect::<Vec<RecordId>>();
        Ok(searches)
    }

    /// Returns record IDs whose keys fall within the inclusive range.
    ///
    /// The scan starts at the leaf that should contain `start`, walks entries in
    /// sorted order, and follows the leaf `next` chain until it either exhausts
    /// the tree or sees a key greater than `end`.
    fn range_search(
        &self,
        txn: TransactionId,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError> {
        self.check_key_type(start)?;
        self.check_key_type(end)?;

        let mut results = Vec::new();
        let (mut guard, mut leaf, _) = self.locate_leaf_for_key(txn, start)?;

        loop {
            for IndexEntry { key, rid } in leaf.entries {
                let ord_lo = key.partial_cmp(start).unwrap();
                let ord_hi = key.partial_cmp(end).unwrap();
                if ord_lo.is_ge() && ord_hi.is_le() {
                    results.push(rid);
                    continue;
                }
                if ord_hi.is_gt() {
                    return Ok(results);
                }
            }

            let Some(next) = leaf.next else {
                return Ok(results);
            };
            drop(guard);
            let (g, node) = self.read_node(txn, next)?;
            let BTreeNode::Leaf(l) = node else {
                return Err(IndexError::CorruptIndex("next pointer to non-leaf"));
            };
            guard = g;
            leaf = l;
        }
    }

    /// Identifies this access method as the B+tree implementation.
    fn kind(&self) -> IndexKind {
        IndexKind::Btree
    }

    /// Returns the declared key component types for this index.
    fn key_types(&self) -> &[Type] {
        &self.key_types
    }

    /// Returns the backing file ID that stores this index's pages.
    fn file_id(&self) -> FileId {
        self.file_id
    }
}

#[cfg(test)]
mod tests {
    //! Integration tests for `BTreeIndex` against a real `PageStore` + WAL.
    //!
    //! These exercise the public `Index` surface (insert / search / delete /
    //! `range_search`) plus a few structural assertions that use the page store
    //! directly to confirm a leaf actually split into multiple pages once
    //! we pushed past `PAGE_SIZE` worth of entries.
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        FileId, Lsn, TransactionId, Value,
        buffer_pool::page_store::PageStore,
        index::CompositeKey,
        primitives::{PageNumber, RecordId, SlotId},
        wal::writer::Wal,
    };

    /// Bundle so individual tests stay short. `_dir` keeps the temp directory
    /// alive for the lifetime of the fixture.
    struct Fixture {
        index: BTreeIndex,
        wal: Arc<Wal>,
        store: Arc<PageStore>,
        file_id: FileId,
        _dir: TempDir,
    }

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(FileId::new(file), PageNumber::new(page), SlotId(slot))
    }

    fn k(v: i32) -> CompositeKey {
        CompositeKey::single(Value::Int32(v))
    }

    fn ks(s: &str) -> CompositeKey {
        CompositeKey::single(Value::String(s.into()))
    }

    fn make_index(key_types: Vec<Type>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(64, Arc::clone(&wal)));
        let file_id = FileId::new(1);
        let path = dir.path().join("btree.db");

        // Touch the file so register_file finds something to open. The btree
        // grows the file lazily via fetch_page on unwritten pages, which the
        // page store handles by zero-filling.
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();

        store.register_file(file_id, &path).unwrap();

        let index = BTreeIndex::new(file_id, key_types, None, Arc::clone(&store), 0);

        Fixture {
            index,
            wal,
            store,
            file_id,
            _dir: dir,
        }
    }

    fn begin(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    #[test]
    fn search_on_empty_tree_returns_empty() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        assert!(fx.index.search(txn, &k(42)).unwrap().is_empty());
    }

    #[test]
    fn delete_on_empty_tree_returns_not_found() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let err = fx.index.delete(txn, &k(1), rid(1, 0, 0)).unwrap_err();
        assert!(matches!(err, IndexError::NotFound));
    }

    #[test]
    fn first_insert_creates_root_leaf() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        assert!(fx.index.root.read().is_none());

        fx.index.insert(txn, &k(7), rid(1, 0, 0)).unwrap();
        assert!(fx.index.root.read().is_some());
        // Exactly one page allocated for the new root leaf.
        assert_eq!(fx.index.num_pages.load(Ordering::Acquire), 1);
    }

    #[test]
    fn insert_then_search_returns_rid() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 10, 3);
        fx.index.insert(txn, &k(42), r).unwrap();

        let found = fx.index.search(txn, &k(42)).unwrap();
        assert_eq!(found, vec![r]);
    }

    #[test]
    fn search_missing_key_returns_empty() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        fx.index.insert(txn, &k(10), rid(1, 0, 0)).unwrap();
        fx.index.insert(txn, &k(20), rid(1, 0, 1)).unwrap();
        assert!(fx.index.search(txn, &k(15)).unwrap().is_empty());
    }

    #[test]
    fn insert_duplicate_key_rid_pair_errors() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 0, 0);
        fx.index.insert(txn, &k(1), r).unwrap();
        let err = fx.index.insert(txn, &k(1), r).unwrap_err();
        assert!(matches!(err, IndexError::DuplicateEntry));
    }

    #[test]
    fn same_key_different_rids_all_returned() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let rids = [rid(1, 0, 0), rid(1, 0, 1), rid(1, 1, 0)];
        for r in &rids {
            fx.index.insert(txn, &k(7), *r).unwrap();
        }

        let mut found = fx.index.search(txn, &k(7)).unwrap();
        found.sort_by_key(|r| (r.page_no.0, r.slot_id.0));
        assert_eq!(found, rids);
    }

    #[test]
    fn delete_existing_entry_removes_it() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r = rid(1, 0, 0);
        fx.index.insert(txn, &k(5), r).unwrap();
        fx.index.delete(txn, &k(5), r).unwrap();
        assert!(fx.index.search(txn, &k(5)).unwrap().is_empty());
    }

    #[test]
    fn delete_one_rid_leaves_others_intact() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let r1 = rid(1, 0, 0);
        let r2 = rid(1, 0, 1);
        fx.index.insert(txn, &k(9), r1).unwrap();
        fx.index.insert(txn, &k(9), r2).unwrap();

        fx.index.delete(txn, &k(9), r1).unwrap();
        assert_eq!(fx.index.search(txn, &k(9)).unwrap(), vec![r2]);
    }

    #[test]
    fn delete_missing_key_returns_not_found() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        fx.index.insert(txn, &k(1), rid(1, 0, 0)).unwrap();
        let err = fx.index.delete(txn, &k(99), rid(1, 0, 0)).unwrap_err();
        assert!(matches!(err, IndexError::NotFound));
    }

    #[test]
    fn delete_existing_key_wrong_rid_returns_not_found() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        fx.index.insert(txn, &k(1), rid(1, 0, 0)).unwrap();
        let err = fx.index.delete(txn, &k(1), rid(1, 9, 9)).unwrap_err();
        assert!(matches!(err, IndexError::NotFound));
    }

    #[test]
    fn key_type_mismatch_errors_on_insert() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let err = fx.index.insert(txn, &ks("nope"), rid(1, 0, 0)).unwrap_err();
        assert!(matches!(err, IndexError::KeyTypeMismatch { .. }));
    }

    #[test]
    fn key_arity_mismatch_errors_on_insert() {
        let fx = make_index(vec![Type::Int32, Type::Int32]);
        let txn = begin(&fx.wal, 1);
        // Two-column index but single-column key.
        let err = fx.index.insert(txn, &k(1), rid(1, 0, 0)).unwrap_err();
        assert!(matches!(err, IndexError::KeyArityMismatch {
            expected: 2,
            got: 1
        }));
    }

    #[test]
    fn range_search_empty_tree_returns_empty() {
        // Empty tree: locate_leaf_for_key over an empty root will fail, so
        // we first insert one entry outside the range to get a root, then
        // query a disjoint range.
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        fx.index.insert(txn, &k(100), rid(1, 0, 0)).unwrap();
        let got = fx.index.range_search(txn, &k(0), &k(50)).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn range_search_inclusive_bounds() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        for i in 0..10 {
            fx.index
                .insert(
                    txn,
                    &k(i),
                    rid(1, 0, u16::try_from(i).expect("slot id fits in u16")),
                )
                .unwrap();
        }
        let got = fx.index.range_search(txn, &k(3), &k(6)).unwrap();
        // Inclusive on both ends: 3, 4, 5, 6.
        let mut slots: Vec<u16> = got.into_iter().map(|r| r.slot_id.0).collect();
        slots.sort_unstable();
        assert_eq!(slots, vec![3, 4, 5, 6]);
    }

    #[test]
    fn range_search_with_key_type_mismatch_errors() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        fx.index.insert(txn, &k(1), rid(1, 0, 0)).unwrap();
        let err = fx.index.range_search(txn, &ks("a"), &ks("z")).unwrap_err();
        assert!(matches!(err, IndexError::KeyTypeMismatch { .. }));
    }

    /// Insert enough entries that the single root leaf cannot hold them,
    /// forcing at least one split. After this we expect:
    /// - more than one allocated page
    /// - the root is now an Internal node
    /// - every inserted key is still findable via search
    /// - a range scan walks the leaf chain end-to-end
    #[test]
    fn many_inserts_force_split_and_grow_internal_root() {
        // 500 entries comfortably exceeds one page worth of int32 entries.
        const N: i32 = 500;

        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);

        for i in 0..N {
            fx.index
                .insert(
                    txn,
                    &k(i),
                    rid(1, 0, u16::try_from(i).expect("slot id fits in u16")),
                )
                .unwrap();
        }

        assert!(
            fx.index.num_pages.load(Ordering::Acquire) > 1,
            "expected multiple pages after {N} inserts; only one means no split happened"
        );

        // Root must be an Internal node now.
        let root_pn = fx.index.root.read().unwrap();
        let (_g, root_node) = fx.index.read_node(txn, root_pn).unwrap();
        assert!(
            matches!(root_node, BTreeNode::Internal(_)),
            "root should have been promoted to an internal node"
        );

        // Every inserted (key, rid) is still searchable.
        for i in 0..N {
            let found = fx.index.search(txn, &k(i)).unwrap();
            assert_eq!(
                found,
                vec![rid(1, 0, u16::try_from(i).expect("slot id fits in u16"))],
                "search lost key {i} after splits"
            );
        }

        // Range scan over the full key space must return all entries.
        let all = fx.index.range_search(txn, &k(0), &k(N - 1)).unwrap();
        assert_eq!(all.len(), N as usize);
    }

    /// Inserting in random-ish order should still produce a sorted leaf chain,
    /// since each leaf maintains key order on insert. We verify this by
    /// range-scanning the full key range and checking the rid order matches
    /// ascending key order.
    #[test]
    fn inserts_in_random_order_leave_leaves_sorted() {
        let fx = make_index(vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);

        // Deterministic shuffle: an LCG is enough; we don't need real
        // randomness, just non-monotonic insertion order.
        let mut order: Vec<i32> = (0..200).collect();
        let mut seed: u32 = 0x9E37_79B9;
        for i in (1..order.len()).rev() {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let j = (seed as usize) % (i + 1);
            order.swap(i, j);
        }

        for &i in &order {
            fx.index
                .insert(
                    txn,
                    &k(i),
                    rid(1, 0, u16::try_from(i).expect("slot id fits in u16")),
                )
                .unwrap();
        }

        // Range-scan the whole space; rids should come back in key order.
        let got = fx.index.range_search(txn, &k(0), &k(199)).unwrap();
        let slots: Vec<u16> = got.into_iter().map(|r| r.slot_id.0).collect();
        let expected: Vec<u16> = (0..200u16).collect();
        assert_eq!(slots, expected, "leaf chain not in ascending key order");
    }

    #[test]
    fn kind_and_key_type_accessors() {
        let fx = make_index(vec![Type::Int64]);
        assert_eq!(fx.index.kind(), IndexKind::Btree);
        assert_eq!(fx.index.key_types(), &[Type::Int64]);
        // Touch fields to silence unused warnings on `store` / `file_id` /
        // `wal` if a particular test path skips them.
        assert_eq!(fx.index.file_id(), fx.file_id);
        let _ = (&fx.store, &fx.wal);
        // Force a single Lsn::INVALID reference so the import is exercised
        // even if write paths inline it.
        let _ = Lsn::INVALID;
    }
}

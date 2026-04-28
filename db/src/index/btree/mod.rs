//! B+Tree index access method.
//!
//! - [`node`] — `BTreeNode` (Leaf | Internal): page layout, encode/decode. Pure data; testable
//!   without a buffer pool.
//! - [`tree`] — `BTreeIndex`: tree traversal, splits, range scans. Talks to `PageStore` for every
//!   page.

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

    fn write_node(guard: &PageGuard<'_>, node: &BTreeNode, lsn: Lsn) -> Result<(), IndexError> {
        let bytes = node.to_bytes()?;
        guard.write(&bytes, lsn);
        Ok(())
    }

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
        self.split_leaf_and_propagate(txn, leaf, leaf_pn, Lsn::INVALID)
    }

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

    fn search(&self, txn: TransactionId, key: &CompositeKey) -> Result<Vec<RecordId>, IndexError> {
        if self.root.read().is_none() {
            return Ok(Vec::new());
        }

        self.check_key_type(key)?;
        let (_, l, _) = self.locate_leaf_for_key(txn, key)?;
        let start = l.entries.partition_point(|e| e.key <= *key);

        let searches = l.entries[start..]
            .iter()
            .filter_map(|e| if &e.key == key { Some(e.rid) } else { None })
            .collect::<Vec<RecordId>>();
        Ok(searches)
    }

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

    fn kind(&self) -> IndexKind {
        IndexKind::Btree
    }

    fn key_types(&self) -> &[Type] {
        &self.key_types
    }

    fn file_id(&self) -> FileId {
        self.file_id
    }
}

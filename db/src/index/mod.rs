//! Secondary index access methods.
//!
//! This is the home for every index family the database knows about
//! (B+Tree, hash, and any future kinds like LSM or R-Tree). It exposes:
//!
//! - The [`Index`] trait — the abstraction every access method implements. The `IndexManager` and
//!   the executor talk to indexes only through this trait, so adding a new family doesn't ripple
//!   through unrelated code.
//! - [`IndexKind`] — the closed catalog tag (`Hash | Btree`) stored on disk so the executor knows
//!   which `Index` impl to instantiate for a declared index.
//! - [`IndexEntry`] — the leaf-level `(key, rid)` pair shared by all index families.
//! - [`IndexError`] — the unified error type returned by index operations.
//!
//! The per-family submodules ([`btree`], [`hash`]) hold the actual page
//! layouts and algorithms. They depend on this module; nothing in this
//! module depends on them.

use std::sync::Arc;

use thiserror::Error;

use crate::{
    FileId, PageNumber, TransactionId, Type,
    buffer_pool::page_store::PageStoreError,
    codec::{CodecError, Decode, Encode},
    primitives::RecordId,
    storage::StorageError,
};

pub mod access;
pub mod btree;
pub mod hash;
pub mod key;

pub use access::{ENVELOPE_HEADER_SIZE, Index, PageKind, decode_index_page, encode_index_page};
pub use key::{CompositeKey, IndexEntry};

/// Sentinel used on disk when a `PageNumber` field is absent (root's parent,
/// last leaf's next, etc.). We only need the sentinel at the disk boundary —
/// in memory we use `Option<PageNumber>`.
pub(super) const NIL: PageNumber = PageNumber(u32::MAX);

impl Encode for Option<PageNumber> {
    fn encode<W: std::io::Write>(&self, writer: &mut W) -> Result<(), crate::codec::CodecError> {
        match self {
            Some(page_number) => page_number.encode(writer),
            None => NIL.encode(writer),
        }
    }
}

impl Decode for Option<PageNumber> {
    fn decode<R: std::io::Read>(reader: &mut R) -> Result<Self, crate::codec::CodecError> {
        let page_number = PageNumber::decode(reader)?;
        if page_number == NIL {
            Ok(None)
        } else {
            Ok(Some(page_number))
        }
    }
}

/// Which kind of secondary index to build.
///
/// Closed catalog tag — stored in the on-disk catalog so the executor
/// knows which access method ([`Index`] impl) to instantiate for a given
/// declared index. The set of variants is fixed; adding one is a schema
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexKind {
    Hash,
    Btree,
}

impl TryFrom<&str> for IndexKind {
    type Error = &'static str;

    /// Parses an index kind from its SQL spelling.
    ///
    /// Accepts `"hash"` and `"btree"`; anything else returns an error.
    ///
    /// # Errors
    ///
    /// Returns `Err("invalid index type")` for any unrecognized string.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "hash" => Ok(IndexKind::Hash),
            "btree" => Ok(IndexKind::Btree),
            _ => Err("invalid index type"),
        }
    }
}

impl From<IndexKind> for &'static str {
    fn from(value: IndexKind) -> Self {
        match value {
            IndexKind::Hash => "hash",
            IndexKind::Btree => "btree",
        }
    }
}

impl TryFrom<u32> for IndexKind {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(IndexKind::Hash),
            1 => Ok(IndexKind::Btree),
            _ => Err("invalid index type"),
        }
    }
}

impl From<IndexKind> for u32 {
    fn from(value: IndexKind) -> Self {
        match value {
            IndexKind::Hash => 0,
            IndexKind::Btree => 1,
        }
    }
}

/// Errors that index operations can return.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("key arity mismatch: index expects {expected} columns, got {got}")]
    KeyArityMismatch { expected: usize, got: usize },

    #[error("key type mismatch at position {position}: expected {expected:?}, got {got:?}")]
    KeyTypeMismatch {
        position: usize,
        expected: Type,
        got: Option<Type>,
    },

    #[error("duplicate entry: (key, rid) already present")]
    DuplicateEntry,

    #[error("an index named {0:?} is already registered")]
    DuplicateName(String),

    #[error("index kind {0:?} is not yet implemented")]
    UnsupportedKind(IndexKind),

    #[error("entry not found for delete")]
    NotFound,

    #[error("index structural invariant violated: {0}")]
    CorruptIndex(&'static str),

    #[error(transparent)]
    PageStore(#[from] PageStoreError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Codec(#[from] CodecError),
}

pub enum AnyIndex {
    Hash(hash::HashIndex),
    Btree(btree::BTreeIndex),
}

impl AnyIndex {
    /// Starts building a hash index access method.
    ///
    /// Using a typed builder keeps the construction call sites readable and prevents accidentally
    /// passing hash-only parameters (like `num_buckets`) for B+Tree indexes.
    pub fn hash() -> HashIndexBuilder {
        HashIndexBuilder::default()
    }

    /// Starts building a B+Tree index access method.
    pub fn btree() -> BtreeIndexBuilder {
        BtreeIndexBuilder::default()
    }

    /// One-time initialization for a freshly created index file.
    ///
    /// Stamps any access-method-specific bootstrap state onto disk
    /// (e.g. `HashIndex` writes empty bucket headers). Call exactly once,
    /// inside the same transaction that allocated the file. Calling it on
    /// a re-opened index would clobber existing data.
    pub fn init(&self, txn: TransactionId) -> Result<(), IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.init(txn),
            AnyIndex::Btree(_idx) => {
                let _ = txn;
                Ok(())
            }
        }
    }

    /// The catalog tag for this index — i.e. what gets persisted on disk so
    /// the next database open knows which family to reconstruct.
    pub fn kind(&self) -> IndexKind {
        match self {
            AnyIndex::Hash(_) => IndexKind::Hash,
            AnyIndex::Btree(_) => IndexKind::Btree,
        }
    }

    /// Per-column declared types, in declaration order. Length is the
    /// index's arity.
    pub fn key_types(&self) -> &[Type] {
        match self {
            AnyIndex::Hash(idx) => idx.key_types(),
            AnyIndex::Btree(idx) => idx.key_types(),
        }
    }

    /// See [`Index::insert`].
    pub fn insert(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.insert(txn, key, rid),
            AnyIndex::Btree(idx) => idx.insert(txn, key, rid),
        }
    }

    /// See [`Index::delete`].
    pub fn delete(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
        rid: RecordId,
    ) -> Result<(), IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.delete(txn, key, rid),
            AnyIndex::Btree(idx) => idx.delete(txn, key, rid),
        }
    }

    /// See [`Index::search`].
    pub fn search(
        &self,
        txn: TransactionId,
        key: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.search(txn, key),
            AnyIndex::Btree(idx) => idx.search(txn, key),
        }
    }

    /// See [`Index::range_search`]. For hash indexes this degrades to a full
    /// scan; the planner shouldn't pick a hash index for ranges.
    pub fn range_search(
        &self,
        txn: TransactionId,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Result<Vec<RecordId>, IndexError> {
        match self {
            AnyIndex::Hash(idx) => idx.range_search(txn, start, end),
            AnyIndex::Btree(idx) => idx.range_search(txn, start, end),
        }
    }
}

#[derive(Default)]
#[must_use]
pub struct HashIndexBuilder {
    file_id: Option<FileId>,
    key_types: Option<Vec<Type>>,
    num_buckets: Option<u32>,
    store: Option<Arc<crate::buffer_pool::page_store::PageStore>>,
    existing_pages: Option<u32>,
}

/// Builder helper: unwrap a required `Option` field or return a uniform error.
///
/// Expands to `self.<field>.ok_or(IndexError::CorruptIndex("missing <field>"))?`, where
/// `"<field>"` is derived via `stringify!` so the message stays in sync with the actual field
/// name.
macro_rules! req_field {
    ($s:ident, $field:ident) => {
        $s.$field.ok_or(IndexError::CorruptIndex(concat!(
            "missing ",
            stringify!($field)
        )))?
    };
}

impl HashIndexBuilder {
    pub fn file_id(mut self, file_id: FileId) -> Self {
        self.file_id = Some(file_id);
        self
    }

    pub fn key_types(mut self, key_types: Vec<Type>) -> Self {
        self.key_types = Some(key_types);
        self
    }

    pub fn num_buckets(mut self, num_buckets: u32) -> Self {
        self.num_buckets = Some(num_buckets);
        self
    }

    pub fn store(mut self, store: Arc<crate::buffer_pool::page_store::PageStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn existing_pages(mut self, existing_pages: u32) -> Self {
        self.existing_pages = Some(existing_pages);
        self
    }

    pub fn build(self) -> Result<AnyIndex, IndexError> {
        let file_id = req_field!(self, file_id);
        let key_types = req_field!(self, key_types);
        let num_buckets = req_field!(self, num_buckets);
        let store = req_field!(self, store);
        let existing_pages = req_field!(self, existing_pages);

        Ok(AnyIndex::Hash(hash::HashIndex::new(
            file_id,
            key_types,
            num_buckets,
            store,
            existing_pages,
        )))
    }
}

#[derive(Default)]
#[must_use]
pub struct BtreeIndexBuilder {
    file_id: Option<FileId>,
    key_types: Option<Vec<Type>>,
    root: Option<PageNumber>,
    store: Option<Arc<crate::buffer_pool::page_store::PageStore>>,
    existing_pages: Option<u32>,
}

impl BtreeIndexBuilder {
    pub fn file_id(mut self, file_id: FileId) -> Self {
        self.file_id = Some(file_id);
        self
    }

    pub fn key_types(mut self, key_types: Vec<Type>) -> Self {
        self.key_types = Some(key_types);
        self
    }

    /// Overrides the starting root page. Defaults to `None`.
    pub fn root(mut self, root: Option<PageNumber>) -> Self {
        self.root = root;
        self
    }

    pub fn store(mut self, store: Arc<crate::buffer_pool::page_store::PageStore>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn existing_pages(mut self, existing_pages: u32) -> Self {
        self.existing_pages = Some(existing_pages);
        self
    }

    pub fn build(self) -> Result<AnyIndex, IndexError> {
        let file_id = req_field!(self, file_id);
        let key_types = req_field!(self, key_types);
        let root = self.root;
        let store = req_field!(self, store);
        let existing_pages = req_field!(self, existing_pages);

        Ok(AnyIndex::Btree(btree::BTreeIndex::new(
            file_id,
            key_types,
            root,
            store,
            existing_pages,
        )))
    }
}

/// Ergonomic conversion so callers can write `hash_idx.into()` instead of
/// `AnyIndex::Hash(hash_idx)`. Symmetric impls land alongside future variants.
impl From<hash::HashIndex> for AnyIndex {
    fn from(idx: hash::HashIndex) -> Self {
        AnyIndex::Hash(idx)
    }
}

impl From<btree::BTreeIndex> for AnyIndex {
    fn from(idx: btree::BTreeIndex) -> Self {
        AnyIndex::Btree(idx)
    }
}

/// Manual `Debug` so we don't have to cascade `#[derive(Debug)]` through
/// `HashIndex` and its `Arc<PageStore>`. The kind tag is the only useful
/// thing to print at this level — the inner state is buffer-pool resident.
impl std::fmt::Debug for AnyIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyIndex").field(&self.kind()).finish()
    }
}

#[cfg(test)]
mod any_index_tests {
    //! These are deliberately small. The full insert/delete/search behavior
    //! is covered exhaustively in `index::hash::tests` against `HashIndex`
    //! directly; here we only check that wrapping a `HashIndex` in `AnyIndex`
    //! preserves that behavior — i.e. the forwarding match arms don't drop
    //! anything on the floor.
    use std::sync::Arc;

    use tempfile::TempDir;

    use super::*;
    use crate::{
        FileId, TransactionId, Value,
        buffer_pool::page_store::PageStore,
        primitives::{PageNumber, RecordId, SlotId},
        storage::PAGE_SIZE,
        wal::writer::Wal,
    };

    struct Fixture {
        any: AnyIndex,
        wal: Arc<Wal>,
        _dir: TempDir,
    }

    fn make_any_hash(num_buckets: u32, key_types: Vec<Type>) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let wal = Arc::new(Wal::new(&dir.path().join("test.wal"), 0).unwrap());
        let store = Arc::new(PageStore::new(64, Arc::clone(&wal)));
        let file_id = FileId::new(1);
        let path = dir.path().join("hash.db");

        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        f.set_len(u64::from(num_buckets) * PAGE_SIZE as u64)
            .unwrap();
        drop(f);
        store.register_file(file_id, &path).unwrap();

        let any = AnyIndex::hash()
            .file_id(file_id)
            .key_types(key_types)
            .num_buckets(num_buckets)
            .store(Arc::clone(&store))
            .existing_pages(num_buckets)
            .build()
            .unwrap();

        let init_txn = TransactionId::new(0);
        wal.log_begin(init_txn).unwrap();
        any.init(init_txn).unwrap();
        store.release_all(init_txn);

        Fixture {
            any,
            wal,
            _dir: dir,
        }
    }

    fn rid(file: u64, page: u32, slot: u16) -> RecordId {
        RecordId::new(FileId::new(file), PageNumber::new(page), SlotId(slot))
    }

    fn begin(wal: &Wal, id: u64) -> TransactionId {
        let txn = TransactionId::new(id);
        wal.log_begin(txn).unwrap();
        txn
    }

    #[test]
    fn from_hash_index_is_hash_kind() {
        let fx = make_any_hash(4, vec![Type::Int32]);
        assert_eq!(fx.any.kind(), IndexKind::Hash);
        assert_eq!(fx.any.key_types(), &[Type::Int32]);
    }

    #[test]
    fn forwarded_insert_search_delete_roundtrip() {
        let fx = make_any_hash(4, vec![Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let key = CompositeKey::single(Value::Int32(42));
        let r = rid(1, 0, 0);

        fx.any.insert(txn, &key, r).unwrap();
        assert_eq!(fx.any.search(txn, &key).unwrap(), vec![r]);

        fx.any.delete(txn, &key, r).unwrap();
        assert!(fx.any.search(txn, &key).unwrap().is_empty());
    }

    #[test]
    fn forwarded_errors_propagate() {
        // Arity check still fires through the wrapper — proves we didn't
        // accidentally short-circuit validation in the forwarding layer.
        let fx = make_any_hash(4, vec![Type::Int32, Type::Int32]);
        let txn = begin(&fx.wal, 1);
        let single = CompositeKey::single(Value::Int32(1));
        let err = fx.any.search(txn, &single).unwrap_err();
        assert!(matches!(err, IndexError::KeyArityMismatch {
            expected: 2,
            got: 1
        }));
    }
}

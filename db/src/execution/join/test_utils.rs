//! Shared test infrastructure for join executor tests.
//!
//! Every executor test module does `use super::super::test_utils::*` to get these
//! helpers and the common type re-exports in one line.

use std::sync::Arc;

// ── re-exports that every test module needs ───────────────────────────────────
pub use fallible_iterator::FallibleIterator;
use tempfile::tempdir;

pub use super::{JoinPredicate, JoinType};
// ── private imports used only inside this file ────────────────────────────────
use crate::{
    FileId, TransactionId, buffer_pool::page_store::PageStore, execution::scan::SeqScan,
    heap::file::HeapFile, wal::writer::Wal,
};
pub use crate::{
    execution::{ExecutionError, Executor, PlanNode, ResolvedExpr},
    parser::statements::BinOp,
    primitives::{ColumnId, Predicate},
    tuple::{Field, Tuple, TupleSchema},
    types::{FixedValue, Type, Value},
};

// ── schema helpers ────────────────────────────────────────────────────────────

pub fn field(name: &str, field_type: Type) -> Field {
    Field::new(name, field_type).unwrap()
}

/// `(a Int32, b Int32)` — the default left-side schema.
pub fn schema_ab() -> TupleSchema {
    TupleSchema::new(vec![field("a", Type::Int32), field("b", Type::Int32)])
}

/// `(x Int32, y Int32)` — right-side schema for NLJ tests; distinct names avoid
/// ambiguity in the merged schema.
pub fn schema_xy() -> TupleSchema {
    TupleSchema::new(vec![field("x", Type::Int32), field("y", Type::Int32)])
}

// ── tuple constructors ────────────────────────────────────────────────────────

pub fn tup(a: i32, b: i32) -> Tuple {
    Tuple::new(vec![Value::int32(a), Value::int32(b)])
}

pub fn tup_null_a(b: i32) -> Tuple {
    Tuple::new(vec![Value::Null, Value::int32(b)])
}

// ── heap harness ──────────────────────────────────────────────────────────────

pub struct HeapHarness {
    pub heap: HeapFile,
    #[allow(dead_code)]
    pub wal: Arc<Wal>,
    pub _dir: tempfile::TempDir,
    pub txn: TransactionId,
}

/// Builds an in-memory [`HeapFile`] with [`schema_ab`] and inserts `tuples`.
pub fn build_heap(id: u64, tuples: &[Tuple]) -> HeapHarness {
    let dir = tempdir().unwrap();
    let wal = Arc::new(Wal::new(&dir.path().join(format!("w{id}.wal")), 0).unwrap());
    let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));
    let file_id = FileId::new(id);
    let path = dir.path().join(format!("h{id}.db"));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.set_len((4 * crate::storage::PAGE_SIZE) as u64)
        .unwrap();
    drop(file);
    store.register_file(file_id, &path).unwrap();
    let overflow_file = HeapFile::make_overflow_file(file_id, Arc::clone(&store), 0);
    let heap = HeapFile::new(
        file_id,
        Arc::new(schema_ab()),
        Arc::clone(&store),
        0,
        overflow_file,
    );
    let txn = TransactionId::new(id);
    wal.log_begin(txn).unwrap();
    for t in tuples {
        heap.insert_tuple(txn, t).unwrap();
    }
    HeapHarness {
        heap,
        wal,
        _dir: dir,
        txn,
    }
}

/// Builds an in-memory [`HeapFile`] with [`schema_xy`] and inserts `tuples`.
pub fn build_heap_xy(id: u64, tuples: &[Tuple]) -> HeapHarness {
    let dir = tempdir().unwrap();
    let wal = Arc::new(Wal::new(&dir.path().join(format!("w{id}.wal")), 0).unwrap());
    let store = Arc::new(PageStore::new(16, Arc::clone(&wal)));
    let file_id = FileId::new(id);
    let path = dir.path().join(format!("h{id}.db"));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.set_len((4 * crate::storage::PAGE_SIZE) as u64)
        .unwrap();
    drop(file);
    store.register_file(file_id, &path).unwrap();
    let overflow_file = HeapFile::make_overflow_file(file_id, Arc::clone(&store), 0);
    let heap = HeapFile::new(
        file_id,
        Arc::new(schema_xy()),
        Arc::clone(&store),
        0,
        overflow_file,
    );
    let txn = TransactionId::new(id);
    wal.log_begin(txn).unwrap();
    for t in tuples {
        heap.insert_tuple(txn, t).unwrap();
    }
    HeapHarness {
        heap,
        wal,
        _dir: dir,
        txn,
    }
}

pub fn scan(h: &HeapHarness) -> PlanNode<'_> {
    PlanNode::SeqScan(SeqScan::new(&h.heap, h.txn))
}

// ── predicate helpers ─────────────────────────────────────────────────────────

pub fn col(n: u32) -> ColumnId {
    ColumnId::new(n).unwrap()
}

pub fn eq_pred(l: u32, r: u32) -> JoinPredicate {
    JoinPredicate::new(col(l), col(r), Predicate::Equals)
}

// ── NLJ expression helpers ────────────────────────────────────────────────────
//
// Left columns: a=0, b=1. Right columns in the concat output: x=2, y=3.

pub fn nlj_col_expr(left_id: u32, op: BinOp, right_id: u32) -> ResolvedExpr {
    ResolvedExpr::BinaryOp {
        lhs: Box::new(ResolvedExpr::Column(ColumnId::new(left_id).unwrap())),
        op,
        rhs: Box::new(ResolvedExpr::Column(ColumnId::new(right_id).unwrap())),
    }
}

/// `left.a = right.x` — the most common NLJ equality predicate in tests.
pub fn nlj_eq_0_0_w2() -> ResolvedExpr {
    nlj_col_expr(0, BinOp::Eq, 2)
}

// ── drain / inspection helpers ────────────────────────────────────────────────

pub fn drain<I: FallibleIterator<Item = Tuple, Error = ExecutionError>>(
    iter: &mut I,
) -> Vec<Tuple> {
    let mut out = Vec::new();
    while let Some(t) = iter.next().unwrap() {
        out.push(t);
    }
    out
}

pub fn int(t: &Tuple, i: usize) -> i32 {
    match t.get(i) {
        Some(Value::Fixed(FixedValue::Int32(v))) => *v,
        other => panic!("expected Int32 at {i}, got {other:?}"),
    }
}

//! JSON-facing mirrors of engine types.
//!
//! Engine types ([`Tuple`](crate::tuple::Tuple),
//! [`TupleSchema`](crate::tuple::TupleSchema), [`Value`](crate::types::Value),
//! [`StatementResult`](crate::engine::StatementResult)) deliberately do not
//! implement `serde::Serialize`. We define explicit DTO structs here so wire
//! format changes don't ripple into the core crate, and so the frontend
//! contract is documented in one place.

pub mod heap;
mod query;

pub use heap::{HeapDumpDto, HeapPageDto, HeapSlotDto};
pub use query::{
    ColumnDto, QueryResultDto, QueryRowsDto, ShownIndexDto, TableInfoDto, TableSummaryDto,
    columns_from_schema, value_to_json,
};

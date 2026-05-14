//! # `StoreMy`
//!
//! A relational database management system built from scratch in Rust.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Database Coordinator                    │
//! └─────────────────────────────────────────────────────────────┘
//!                                │
//!         ┌──────────────────────┼──────────────────────┐
//!         ▼                      ▼                      ▼
//! ┌───────────────┐     ┌───────────────┐     ┌───────────────┐
//! │    Parser     │     │    Planner    │     │   Optimizer   │
//! └───────────────┘     └───────────────┘     └───────────────┘
//!                                │
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Execution Engine                       │
//! └─────────────────────────────────────────────────────────────┘
//!                                │
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Transaction Layer                       │
//! │              (Lock Manager, WAL, Recovery)                  │
//! └─────────────────────────────────────────────────────────────┘
//!                                │
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      Storage Engine                         │
//! │           (Buffer Pool, Heap Files, B+Tree, Hash)           │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Modules
//!
//! - [`primitives`] - Core types (`PageId`, `TransactionId`Id, LSN, etc.)
//! - [`types`] - Value types and type system
//! - Module-specific error enums and `Result` aliases
//! - [`mod@tuple`] - Tuple schema and record representation
//! - [`execution`] - Database operator and iterator traits

// Make `::storemy::...` resolve inside our own crate. The `Encode` / `Decode`
// derive macros (in the sibling `storemy-codec-derive` crate) emit absolute
// paths like `::storemy::codec::Encode`. Without this alias those paths only
// resolve in *other* crates that depend on us — not in our own code.
extern crate self as storemy;

pub mod buffer_pool;
pub mod catalog;
pub mod codec;
pub mod database;
pub mod execution;
pub mod heap;
pub mod index;
pub mod observability;
pub mod parser;
pub mod primitives;
pub mod registry;
pub mod repl;
pub mod storage;
pub mod transaction;
pub mod tuple;
pub mod types;
pub mod wal;
pub mod web;

pub mod engine;
pub use primitives::{FileId, IndexId, Lsn, PageNumber, TransactionId};
pub use types::{Type, Value};

pub const PAGE_SIZE: usize = 4096;
pub const STRING_MAX_SIZE: usize = 255;

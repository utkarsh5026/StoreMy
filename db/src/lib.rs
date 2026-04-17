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
//! - [`error`] - Error types and Result alias
//! - [`tuple`] - Tuple schema and record representation
//! - [`iterator`] - Database iterator traits

pub mod btree;
pub mod buffer_pool;
pub mod catalog;
pub mod codec;
pub mod database;
pub mod execution;
pub mod hash;
pub mod heap;
pub mod parser;
pub mod primitives;
pub mod storage;
pub mod transaction;
pub mod tuple;
pub mod types;
pub mod wal;

pub mod engine;
pub use primitives::{FileId, Lsn, PageNumber, TransactionId};
pub use types::{Type, Value};

/// Database page size in bytes (4KB)
pub const PAGE_SIZE: usize = 4096;

/// Maximum string length for STRING type
pub const STRING_MAX_SIZE: usize = 255;

//! Integration tests for the `StoreMy` database.
//!
//! Each submodule exercises one slice of the public surface end-to-end through
//! the [`storemy::database::Database`] facade. Helpers shared by every test
//! live in `common`. Submodules are grouped under [`integration/`] so they
//! compile into a single test binary.

#[path = "integration/common.rs"]
mod common;

#[path = "integration/alter_table.rs"]
mod alter_table;
#[path = "integration/catalog_introspection.rs"]
mod catalog_introspection;
#[path = "integration/concurrency.rs"]
mod concurrency;
#[path = "integration/constraints.rs"]
mod constraints;
#[path = "integration/ddl.rs"]
mod ddl;
#[path = "integration/dml.rs"]
mod dml;
#[path = "integration/errors.rs"]
mod errors;

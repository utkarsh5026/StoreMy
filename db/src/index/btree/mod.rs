//! B+Tree index access method.
//!
//! - [`node`] — `BTreeNode` (Leaf | Internal): page layout, encode/decode. Pure data; testable
//!   without a buffer pool.
//! - [`tree`] — `BTreeIndex`: tree traversal, splits, range scans. Talks to `PageStore` for every
//!   page.

pub mod node;
pub mod tree;

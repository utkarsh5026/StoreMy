// Package storage is the root of StoreMy's disk-based storage engine.
//
// Data is organised into fixed-size 4 KB pages that are read and written as
// atomic units. Higher-level sub-packages build on this foundation to provide
// heap file storage, index structures, and page-level management.
//
// # Sub-packages
//
//   - [storemy/pkg/storage/page]       – Page header layout, slot directory,
//     and the low-level byte-array representation of a single 4 KB page.
//   - [storemy/pkg/storage/heap]       – Heap file: an unordered collection of
//     pages that stores variable-length rows. Supports sequential scans and
//     free-space management.
//   - [storemy/pkg/storage/index/btree] – B-tree index for range queries and
//     ordered scans on a single column.
//   - [storemy/pkg/storage/index/hash]  – Extendible hash index for O(1)
//     equality lookups.
//
// # Page layout
//
// Each page starts with a fixed header (page ID, LSN, flags) followed by a
// slot directory that grows downward from the end of the page toward the
// centre. Tuple data is packed from the start of the page toward the centre.
// Pages are never partially written; the WAL ensures durability.
package storage

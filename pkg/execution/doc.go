// Package execution is the root of StoreMy's query execution engine.
//
// The engine uses the iterator (volcano) model: every operator implements a
// common interface with Open / Next / Close methods. Operators are composed
// into a tree; calling Next on the root pulls one row at a time through the
// entire pipeline without materialising intermediate results.
//
// # Sub-packages
//
//   - [storemy/pkg/execution/scanner]     – Table and index scan operators that
//     feed raw rows into the pipeline.
//   - [storemy/pkg/execution/query]       – Projection, filter, sort, limit,
//     and other single-table relational operators.
//   - [storemy/pkg/execution/join]        – Nested-loop, hash, and sort-merge
//     join operators with a strategy selector.
//   - [storemy/pkg/execution/aggregation] – GROUP BY / aggregate functions
//     (COUNT, SUM, AVG, MIN, MAX) implemented over the iterator model.
//   - [storemy/pkg/execution/setops]      – Set operations: UNION, INTERSECT,
//     and EXCEPT.
//
// # Execution flow
//
// The database package receives a parsed statements.Statement, passes it to
// the query planner which builds an operator tree, and then calls Next on the
// root until it returns io.EOF. Each row is formatted and returned to the
// caller without buffering the full result set in memory.
package execution

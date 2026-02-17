// Package parser converts SQL text into an abstract syntax tree (AST).
//
// [ParseStatement] is the single entry point. It accepts a SQL string, drives the
// lexer to produce tokens, and returns a typed [statements.Statement] value that
// the query planner can inspect and execute.
//
// # Supported statements
//
//   - SELECT  – projection (including DISTINCT), aggregate functions (COUNT, SUM, etc.),
//     WHERE with AND-chained conditions, JOIN (INNER / LEFT [OUTER] / RIGHT [OUTER]),
//     GROUP BY, ORDER BY [ASC|DESC], LIMIT / OFFSET, set operations
//     (UNION [ALL], INTERSECT [ALL], EXCEPT [ALL])
//   - INSERT  – single and multi-row inserts with optional explicit column list
//   - UPDATE  – SET one or more columns, optional WHERE clause, optional table alias
//   - DELETE  – optional WHERE clause, optional table alias
//   - CREATE TABLE [IF NOT EXISTS]  – columns typed INT / VARCHAR / TEXT / BOOLEAN / FLOAT,
//     per-column constraints NOT NULL / DEFAULT / PRIMARY KEY / AUTO_INCREMENT,
//     table-level PRIMARY KEY constraint
//   - CREATE INDEX [IF NOT EXISTS]  – ON table(column), optional USING {HASH|BTREE}
//   - DROP TABLE [IF EXISTS]
//   - DROP INDEX [IF EXISTS] [ON table_name]
//   - EXPLAIN [ANALYZE] [FORMAT {TEXT|JSON}]  – wraps any DML or DDL statement
//   - SHOW INDEXES [FROM table_name]
//
// # Usage
//
//	stmt, err := parser.ParseStatement("SELECT id, name FROM users WHERE age > 18")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	// Type-switch on statements.SelectStatement, statements.InsertStatement, etc.
//
// # Error handling
//
// Syntax errors are returned as plain Go errors with a message that describes
// the unexpected token and its position in the input. The parser does not
// panic on malformed input.
package parser

// Package parser converts SQL text into an abstract syntax tree (AST).
//
// ParseStatement is the single entry point. It accepts a SQL string, drives the
// lexer to produce tokens, and returns a typed statements.Statement value that
// the query planner can inspect and execute.
//
// # Supported statements
//
//   - SELECT  – full projection, filtering (WHERE), JOIN, GROUP BY, HAVING,
//     ORDER BY, LIMIT/OFFSET, DISTINCT, sub-selects
//   - INSERT  – single and multi-row inserts
//   - UPDATE  – predicated row updates
//   - DELETE  – predicated row deletes
//   - CREATE TABLE / CREATE INDEX
//   - DROP TABLE / DROP INDEX
//   - EXPLAIN – wraps any DML statement to expose the execution plan
//   - SHOW INDEXES
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

package parser

import (
	"errors"
	"fmt"
	"storemy/pkg/parser/statements"
)

// ParseStatement parses a SQL statement string and returns the corresponding Statement object.
// This is the main entry point for the parser component, which converts SQL text into
// an abstract syntax tree (AST) representation that can be executed by the query planner.
//
// Supported SQL statements:
//   - SELECT: Query data from tables
//   - INSERT: Add new rows to tables
//   - UPDATE: Modify existing rows
//   - DELETE: Remove rows from tables
//   - CREATE TABLE: Define new table schemas
//   - CREATE INDEX: Create indexes on tables
//   - DROP TABLE: Remove tables
//   - DROP INDEX: Remove indexes
//   - EXPLAIN: Show query execution plan
//   - SHOW INDEXES: Display index information
//
// Parameters:
//   - sql: The SQL statement string to parse
//
// Returns:
//   - statements.Statement: The parsed statement object implementing the Statement interface
//   - error: An error if parsing fails or the statement type is unsupported
func ParseStatement(sql string) (statements.Statement, error) {
	l := NewLexer(sql)
	token := l.NextToken()
	if token.Type == EOF {
		return nil, errors.New("empty statement")
	}

	switch token.Type {
	case INSERT:
		l.SetPos(0)
		return (&InsertParser{}).Parse(l)
	case CREATE:
		secondToken := l.NextToken()
		l.SetPos(0)
		switch secondToken.Type {
		case TABLE:
			return (&CreateStatementParser{}).Parse(l)
		case INDEX:
			return (&CreateIndexParser{}).Parse(l)
		default:
			return nil, fmt.Errorf("expected TABLE or INDEX after CREATE, got %s", secondToken.Value)
		}
	case DROP:
		secondToken := l.NextToken()
		l.SetPos(0)
		switch secondToken.Type {
		case TABLE:
			return (&DropStatementParser{}).Parse(l)
		case INDEX:
			return (&DropIndexParser{}).Parse(l)
		default:
			return nil, fmt.Errorf("expected TABLE or INDEX after DROP, got %s", secondToken.Value)
		}
	case DELETE:
		l.SetPos(0)
		return (&DeleteParser{}).Parse(l)
	case UPDATE:
		l.SetPos(0)
		return (&UpdateParser{}).Parse(l)
	case SELECT:
		l.SetPos(0)
		return (&SelectParser{}).Parse(l)
	case EXPLAIN:
		l.SetPos(0)
		return (&ExplainStatementParser{}).Parse(l)
	case SHOW:
		l.SetPos(0)
		return (&ShowIndexesParser{}).Parse(l)
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", token.Value)
	}
}

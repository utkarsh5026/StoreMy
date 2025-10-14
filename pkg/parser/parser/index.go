package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
)

// parseCreateIndexStatement parses a CREATE INDEX SQL statement from the lexer tokens.
// Syntax: CREATE INDEX [IF NOT EXISTS] index_name ON table_name(column_name) [USING {HASH|BTREE}]
func parseCreateIndexStatement(l *lexer.Lexer) (*statements.CreateIndexStatement, error) {
	if err := expectTokenSequence(l, lexer.CREATE, lexer.INDEX); err != nil {
		return nil, err
	}

	ifNotExists, err := parseIfNotExists(l)
	if err != nil {
		return nil, err
	}

	indexName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected index name: %w", err)
	}

	if err := expectTokenSequence(l, lexer.ON); err != nil {
		return nil, err
	}

	tableName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected table name: %w", err)
	}

	if err := expectTokenSequence(l, lexer.LPAREN); err != nil {
		return nil, err
	}

	columnName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected column name: %w", err)
	}

	if err := expectTokenSequence(l, lexer.RPAREN); err != nil {
		return nil, err
	}

	indexType := index.HashIndex
	token := l.NextToken()
	if token.Type == lexer.USING {
		typeToken := l.NextToken()
		switch typeToken.Type {
		case lexer.HASH:
			indexType = index.HashIndex
		case lexer.BTREE:
			indexType = index.BTreeIndex
		default:
			return nil, fmt.Errorf("expected HASH or BTREE after USING, got %s", typeToken.Value)
		}
	} else {
		l.SetPos(token.Position)
	}

	stmt := statements.NewCreateIndexStatement(indexName, tableName, columnName, indexType, ifNotExists)
	return stmt, nil
}

// parseDropIndexStatement parses a DROP INDEX SQL statement from the lexer tokens.
// Syntax: DROP INDEX [IF EXISTS] index_name [ON table_name]
func parseDropIndexStatement(l *lexer.Lexer) (*statements.DropIndexStatement, error) {
	if err := expectTokenSequence(l, lexer.DROP, lexer.INDEX); err != nil {
		return nil, err
	}

	ifExists, err := parseIfExists(l)
	if err != nil {
		return nil, err
	}

	indexName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected index name: %w", err)
	}

	tableName := ""
	token := l.NextToken()
	if token.Type == lexer.ON {
		tableName, err = parseValueWithType(l, lexer.IDENTIFIER)
		if err != nil {
			return nil, fmt.Errorf("expected table name after ON: %w", err)
		}
	} else {
		l.SetPos(token.Position)
	}

	stmt := statements.NewDropIndexStatement(indexName, tableName, ifExists)
	return stmt, nil
}

// parseIfExists checks for the optional "IF EXISTS" clause in DROP statements.
// Returns true if the clause is present, false otherwise.
func parseIfExists(l *lexer.Lexer) (bool, error) {
	token := l.NextToken()
	if token.Type != lexer.IF {
		l.SetPos(token.Position)
		return false, nil
	}

	if err := expectTokenSequence(l, lexer.EXISTS); err != nil {
		return false, err
	}

	return true, nil
}

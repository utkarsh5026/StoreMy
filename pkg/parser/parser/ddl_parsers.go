package parser

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
)

type DropStatementParser struct{}

// parseDropStatement parses a DROP TABLE SQL statement from the lexer tokens.
// It expects the format: DROP TABLE [IF EXISTS] table_name
func (d *DropStatementParser) Parse(l *Lexer) (*statements.DropStatement, error) {
	if err := expectTokenSequence(l, DROP, TABLE); err != nil {
		return nil, err
	}

	ifExists := false
	token := l.NextToken()
	if token.Type == IF {
		if err := expectTokenSequence(l, EXISTS); err != nil {
			return nil, fmt.Errorf("expected EXISTS after IF, got: %v", err)
		}
		ifExists = true
		token = l.NextToken()
	}

	if token.Type != IDENTIFIER {
		return nil, fmt.Errorf("expected table name, got: %s", token.Value)
	}

	tableName := token.Value
	stmt := statements.NewDropStatement(tableName, ifExists)

	if err := stmt.Validate(); err != nil {
		return nil, err
	}

	return stmt, nil
}

type CreateStatementParser struct{}

// fieldConstraints represents the constraints that can be applied to a table field
type fieldConstraints struct {
	NotNull       bool
	DefaultValue  types.Field
	PrimaryKey    bool
	AutoIncrement bool
}

// parseCreateStatement parses a CREATE TABLE SQL statement from the lexer tokens.
// It expects the format: CREATE TABLE [IF NOT EXISTS] table_name (field_definitions...)
func (c *CreateStatementParser) Parse(l *Lexer) (*statements.CreateStatement, error) {
	if err := expectTokenSequence(l, CREATE, TABLE); err != nil {
		return nil, err
	}

	ifNotExists, err := parseIfNotExists(l)
	if err != nil {
		return nil, err
	}

	tableName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected table name: %w", err)
	}

	stmt := statements.NewCreateStatement(tableName, ifNotExists)
	if err := expectTokenSequence(l, LPAREN); err != nil {
		return nil, err
	}

	if err := c.parseTableDefinition(l, stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

// readPrimaryKey parses a PRIMARY KEY constraint definition.
// Expects format: PRIMARY KEY (field_name)
// Sets the primary key field in the CREATE statement.
func (c *CreateStatementParser) readPrimaryKey(l *Lexer, stmt *statements.CreateStatement) error {
	if err := expectTokenSequence(l, KEY, LPAREN); err != nil {
		return err
	}

	fieldName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return fmt.Errorf("expected field name in PRIMARY KEY: %w", err)
	}

	stmt.PrimaryKey = fieldName
	return expectTokenSequence(l, RPAREN)
}

// parseTableDefinition parses the content inside parentheses of CREATE TABLE.
// Handles field definitions and table constraints like PRIMARY KEY.
// Continues until closing parenthesis is found, expecting comma-separated definitions.
func (c *CreateStatementParser) parseTableDefinition(l *Lexer, stmt *statements.CreateStatement) error {
loop:
	for {
		token := l.NextToken()
		var err error

		switch token.Type {
		case PRIMARY:
			err = c.readPrimaryKey(l, stmt)

		case IDENTIFIER:
			err = c.parseFieldDefinition(l, stmt, token.Value)

		default:
			return fmt.Errorf("expected field definition or PRIMARY KEY, got %s", token.Value)
		}

		if err != nil {
			return err
		}

		token = l.NextToken()
		switch token.Type {
		case COMMA:
			continue
		case RPAREN:
			break loop
		default:
			return fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}
	return nil
}

// parseFieldDefinition parses a single field definition within CREATE TABLE.
// Format: field_name data_type [constraints...]
// Adds the parsed field to the CREATE statement with its type and constraints.
func (c *CreateStatementParser) parseFieldDefinition(l *Lexer, stmt *statements.CreateStatement, fieldName string) error {
	token := l.NextToken()
	fieldType, err := c.parseDataType(token)
	if err != nil {
		return err
	}

	constraints, err := c.parseFieldConstraints(l)
	if err != nil {
		return err
	}

	stmt.AddFieldWithAutoInc(fieldName, fieldType, constraints.NotNull, constraints.DefaultValue, constraints.AutoIncrement)

	if constraints.PrimaryKey {
		stmt.PrimaryKey = fieldName
	}

	return nil
}

// parseFieldConstraints parses optional constraints for a field definition.
// Handles NOT NULL, DEFAULT value, PRIMARY KEY, and AUTO_INCREMENT constraints.
// Returns when no more recognized constraint tokens are found.
func (c *CreateStatementParser) parseFieldConstraints(l *Lexer) (*fieldConstraints, error) {
	constraints := &fieldConstraints{}

	for {
		token := l.NextToken()

		switch token.Type {
		case AUTO_INCREMENT:
			constraints.AutoIncrement = true
		case NOT:
			if err := expectTokenSequence(l, NULL); err != nil {
				return nil, err
			}
			constraints.NotNull = true
		case DEFAULT:
			defaultValue, err := parseValue(l)
			if err != nil {
				return nil, err
			}
			constraints.DefaultValue = defaultValue
		case PRIMARY:
			nextToken := l.NextToken()
			if nextToken.Type != KEY {
				return nil, fmt.Errorf("expected KEY after PRIMARY, got %s", nextToken.Value)
			}
			constraints.PrimaryKey = true
		default:
			l.SetPos(token.Position)
			return constraints, nil
		}
	}
}

// parseDataType converts a lexer token to the corresponding data type.
// Supports INT, VARCHAR, TEXT, BOOLEAN, and FLOAT types.
func (c *CreateStatementParser) parseDataType(token Token) (types.Type, error) {
	switch token.Type {
	case INT:
		return types.IntType, nil
	case VARCHAR, TEXT:
		return types.StringType, nil
	case BOOLEAN:
		return types.BoolType, nil
	case FLOAT:
		return types.FloatType, nil
	default:
		return 0, fmt.Errorf("unknown data type: %s", token.Value)
	}
}

type CreateIndexParser struct{}

// parseCreateIndexStatement parses a CREATE INDEX SQL statement from the lexer tokens.
// Syntax: CREATE INDEX [IF NOT EXISTS] index_name ON table_name(column_name) [USING {HASH|BTREE}]
func (c *CreateIndexParser) Parse(l *Lexer) (*statements.CreateIndexStatement, error) {
	if err := expectTokenSequence(l, CREATE, INDEX); err != nil {
		return nil, err
	}

	ifNotExists, err := parseIfNotExists(l)
	if err != nil {
		return nil, err
	}

	indexName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected index name: %w", err)
	}

	if err := expectTokenSequence(l, ON); err != nil {
		return nil, err
	}

	tableName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected table name: %w", err)
	}

	if err := expectTokenSequence(l, LPAREN); err != nil {
		return nil, err
	}

	columnName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected column name: %w", err)
	}

	if err := expectTokenSequence(l, RPAREN); err != nil {
		return nil, err
	}

	indexType := index.HashIndex
	token := l.NextToken()
	if token.Type == USING {
		typeToken := l.NextToken()
		switch typeToken.Type {
		case HASH:
			indexType = index.HashIndex
		case BTREE:
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

type DropIndexParser struct{}

// parseDropIndexStatement parses a DROP INDEX SQL statement from the lexer tokens.
// Syntax: DROP INDEX [IF EXISTS] index_name [ON table_name]
func (d *DropIndexParser) Parse(l *Lexer) (*statements.DropIndexStatement, error) {
	if err := expectTokenSequence(l, DROP, INDEX); err != nil {
		return nil, err
	}

	ifExists, err := parseIfExists(l)
	if err != nil {
		return nil, err
	}

	indexName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected index name: %w", err)
	}

	tableName := ""
	token := l.NextToken()
	if token.Type == ON {
		tableName, err = parseValueWithType(l, IDENTIFIER)
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
func parseIfExists(l *Lexer) (bool, error) {
	token := l.NextToken()
	if token.Type != IF {
		l.SetPos(token.Position)
		return false, nil
	}

	if err := expectTokenSequence(l, EXISTS); err != nil {
		return false, err
	}

	return true, nil
}

// parseIfNotExists checks for the optional "IF NOT EXISTS" clause in CREATE TABLE.
// Returns true if the clause is present, false otherwise.
// If IF is present but not followed by NOT EXISTS, returns an error.
func parseIfNotExists(l *Lexer) (bool, error) {
	token := l.NextToken()
	if token.Type != IF {
		l.SetPos(token.Position)
		return false, nil
	}

	if err := expectTokenSequence(l, NOT, EXISTS); err != nil {
		return false, err
	}

	return true, nil
}

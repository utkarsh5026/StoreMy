package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
)

// fieldConstraints represents the constraints that can be applied to a table field
type fieldConstraints struct {
	NotNull       bool
	DefaultValue  types.Field
	PrimaryKey    bool
	AutoIncrement bool
}

// parseCreateStatement parses a CREATE TABLE SQL statement from the lexer tokens.
// It expects the format: CREATE TABLE [IF NOT EXISTS] table_name (field_definitions...)
func parseCreateStatement(l *lexer.Lexer) (*statements.CreateStatement, error) {
	if err := expectTokenSequence(l, lexer.CREATE, lexer.TABLE); err != nil {
		return nil, err
	}

	ifNotExists, err := parseIfNotExists(l)
	if err != nil {
		return nil, err
	}

	tableName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected table name: %w", err)
	}

	stmt := statements.NewCreateStatement(tableName, ifNotExists)
	if err := expectTokenSequence(l, lexer.LPAREN); err != nil {
		return nil, err
	}

	if err := parseTableDefinition(l, stmt); err != nil {
		return nil, err
	}

	return stmt, nil
}

// parseIfNotExists checks for the optional "IF NOT EXISTS" clause in CREATE TABLE.
// Returns true if the clause is present, false otherwise.
// If IF is present but not followed by NOT EXISTS, returns an error.
func parseIfNotExists(l *lexer.Lexer) (bool, error) {
	token := l.NextToken()
	if token.Type != lexer.IF {
		l.SetPos(token.Position)
		return false, nil
	}

	if err := expectTokenSequence(l, lexer.NOT, lexer.EXISTS); err != nil {
		return false, err
	}

	return true, nil
}

// readPrimaryKey parses a PRIMARY KEY constraint definition.
// Expects format: PRIMARY KEY (field_name)
// Sets the primary key field in the CREATE statement.
func readPrimaryKey(l *lexer.Lexer, stmt *statements.CreateStatement) error {
	if err := expectTokenSequence(l, lexer.KEY, lexer.LPAREN); err != nil {
		return err
	}

	fieldName, err := parseValueWithType(l, lexer.IDENTIFIER)
	if err != nil {
		return fmt.Errorf("expected field name in PRIMARY KEY: %w", err)
	}

	stmt.PrimaryKey = fieldName
	return expectTokenSequence(l, lexer.RPAREN)
}

// parseTableDefinition parses the content inside parentheses of CREATE TABLE.
// Handles field definitions and table constraints like PRIMARY KEY.
// Continues until closing parenthesis is found, expecting comma-separated definitions.
func parseTableDefinition(l *lexer.Lexer, stmt *statements.CreateStatement) error {
	for {
		token := l.NextToken()
		var err error

		switch token.Type {
		case lexer.PRIMARY:
			err = readPrimaryKey(l, stmt)

		case lexer.IDENTIFIER:
			err = parseFieldDefinition(l, stmt, token.Value)

		default:
			return fmt.Errorf("expected field definition or PRIMARY KEY, got %s", token.Value)
		}

		if err != nil {
			return err
		}

		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		} else if token.Type == lexer.RPAREN {
			break
		} else {
			return fmt.Errorf("expected ',' or ')', got %s", token.Value)
		}
	}
	return nil
}

// parseFieldDefinition parses a single field definition within CREATE TABLE.
// Format: field_name data_type [constraints...]
// Adds the parsed field to the CREATE statement with its type and constraints.
func parseFieldDefinition(l *lexer.Lexer, stmt *statements.CreateStatement, fieldName string) error {
	token := l.NextToken()
	fieldType, err := parseDataType(token)
	if err != nil {
		return err
	}

	constraints, err := parseFieldConstraints(l)
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
func parseFieldConstraints(l *lexer.Lexer) (*fieldConstraints, error) {
	constraints := &fieldConstraints{}

	for {
		token := l.NextToken()

		switch token.Type {
		case lexer.AUTO_INCREMENT:
			constraints.AutoIncrement = true
		case lexer.NOT:
			if err := expectTokenSequence(l, lexer.NULL); err != nil {
				return nil, err
			}
			constraints.NotNull = true
		case lexer.DEFAULT:
			defaultValue, err := parseValue(l)
			if err != nil {
				return nil, err
			}
			constraints.DefaultValue = defaultValue
		case lexer.PRIMARY:
			// Handle inline PRIMARY KEY constraint
			nextToken := l.NextToken()
			if nextToken.Type != lexer.KEY {
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
func parseDataType(token lexer.Token) (types.Type, error) {
	switch token.Type {
	case lexer.INT:
		return types.IntType, nil
	case lexer.VARCHAR, lexer.TEXT:
		return types.StringType, nil
	case lexer.BOOLEAN:
		return types.BoolType, nil
	case lexer.FLOAT:
		return types.FloatType, nil
	default:
		return 0, fmt.Errorf("unknown data type: %s", token.Value)
	}
}

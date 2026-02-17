package parser

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
)

type DeleteParser struct{}

// parseDeleteStatement parses a DELETE SQL statement from the lexer tokens.
// It expects the format: DELETE FROM table_name [alias] [WHERE condition]
func (d *DeleteParser) Parse(l *Lexer) (*statements.DeleteStatement, error) {
	if err := expectTokenSequence(l, DELETE, FROM); err != nil {
		return nil, err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	statement := statements.NewDeleteStatement(tableName, alias)
	token := l.NextToken()
	if token.Type == WHERE {
		filter, err := parseWhereCondition(l)
		if err != nil {
			return nil, err
		}
		statement.SetWhereClause(filter)
	} else {
		l.SetPos(token.Position)
	}

	return statement, nil
}

type InsertParser struct{}

func (i *InsertParser) Parse(l *Lexer) (*statements.InsertStatement, error) {
	if err := expectTokenSequence(l, INSERT, INTO); err != nil {
		return nil, err
	}

	tableName, _, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	statement := statements.NewInsertStatement(tableName)

	token := l.NextToken()
	if token.Type == LPAREN {
		fields, err := i.parseFieldList(l)
		if err != nil {
			return nil, err
		}
		statement.AddFieldNames(fields)
		token = l.NextToken()
	}

	if token.Type == VALUES {
		return i.parseInsertValues(l, statement)
	} else {
		return nil, fmt.Errorf("expected VALUES or SELECT, got %s", token.Value)
	}
}

func (i *InsertParser) parseInsertValues(l *Lexer, stmt *statements.InsertStatement) (*statements.InsertStatement, error) {
	for {
		token := l.NextToken()
		if err := expectToken(token, LPAREN); err != nil {
			return nil, err
		}

		values, err := parseDelimitedList(l, parseValue, COMMA, RPAREN)
		if err != nil {
			return nil, err
		}

		stmt.AddValues(values)

		token = l.NextToken()
		if token.Type == COMMA {
			continue
		} else {
			l.SetPos(token.Position) // Put it back
			break
		}
	}

	return stmt, nil
}

func (i *InsertParser) parseFieldList(l *Lexer) ([]string, error) {
	return parseDelimitedList(l,
		func(l *Lexer) (string, error) {
			token := l.NextToken()
			if err := expectToken(token, IDENTIFIER); err != nil {
				return "", err
			}
			return token.Value, nil
		},
		COMMA,
		RPAREN,
	)
}

type UpdateParser struct{}

// parseUpdateStatement parses a SQL UPDATE statement from the lexer.
// It expects the format: UPDATE table_name [alias] SET column1=value1, column2=value2, ... [WHERE condition]
func (u *UpdateParser) Parse(l *Lexer) (*statements.UpdateStatement, error) {
	if err := expectTokenSequence(l, UPDATE); err != nil {
		return nil, err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	if err := expectTokenSequence(l, SET); err != nil {
		return nil, err
	}

	stmt := statements.NewUpdateStatement(tableName, alias)
	if err := u.parseSetClauses(l, stmt); err != nil {
		return nil, err
	}

	if err := u.parseOptionalWhereClause(l, stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseOptionalWhereClause parses an optional WHERE clause for an UPDATE statement.
// If a WHERE token is found, it parses the condition and adds it to the statement.
// If no WHERE clause is present, the token is put back and no error is returned.
func (u *UpdateParser) parseOptionalWhereClause(l *Lexer, stmt *statements.UpdateStatement) error {
	token := l.NextToken()
	if token.Type == WHERE {
		filter, err := parseWhereCondition(l)
		if err != nil {
			return err
		}
		stmt.SetWhereClause(filter)
	} else {
		l.SetPos(token.Position)
	}
	return nil
}

// parseSetClauses parses one or more SET clauses in an UPDATE statement.
// Expects the format: column1=value1, column2=value2, ...
// Each clause must be a column name followed by '=' and a value.
func (u *UpdateParser) parseSetClauses(l *Lexer, stmt *statements.UpdateStatement) error {
	for {
		fieldName, err := parseValueWithType(l, IDENTIFIER)
		if err != nil {
			return fmt.Errorf("expected field name in SET: %w", err)
		}

		opValue, err := parseValueWithType(l, OPERATOR)
		if err != nil {
			return fmt.Errorf("expected operator in SET: %w", err)
		}

		if opValue != "=" {
			return fmt.Errorf("expected '=', got %s", opValue)
		}

		value, err := parseValue(l)
		if err != nil {
			return err
		}

		stmt.AddSetClause(fieldName, value)
		token := l.NextToken()
		if token.Type == COMMA {
			continue
		}

		l.SetPos(token.Position)
		break
	}
	return nil
}

// parseWhereCondition parses a WHERE clause condition for filtering records.
// It expects the format: field_name operator value
// Currently supports simple conditions with a single field, operator, and constant value.
func parseWhereCondition(l *Lexer) (*plan.FilterNode, error) {
	fieldName, err := parseValueWithType(l, IDENTIFIER)
	if err != nil {
		return nil, fmt.Errorf("expected field name in WHERE: %w", err)
	}

	opValue, err := parseValueWithType(l, OPERATOR)
	if err != nil {
		return nil, fmt.Errorf("expected operator in WHERE: %w", err)
	}

	pred, err := parseOperator(opValue)
	if err != nil {
		return nil, err
	}

	constant, err := parseValueWithType(l, STRING, INT)
	if err != nil {
		return nil, fmt.Errorf("expected value in WHERE: %w", err)
	}

	return plan.NewFilterNode("", fieldName, pred, constant), nil
}

package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
)

// parseUpdateStatement parses a SQL UPDATE statement from the lexer.
// It expects the format: UPDATE table_name [alias] SET column1=value1, column2=value2, ... [WHERE condition]
func parseUpdateStatement(l *lexer.Lexer) (*statements.UpdateStatement, error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.UPDATE); err != nil {
		return nil, err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return nil, err
	}

	token = l.NextToken()
	if err := expectToken(token, lexer.SET); err != nil {
		return nil, err
	}

	stmt := statements.NewUpdateStatement(tableName, alias)
	if err := parseSetClauses(l, stmt); err != nil {
		return nil, err
	}

	if err := parseOptionalWhereClause(l, stmt); err != nil {
		return nil, err
	}
	return stmt, nil
}

// parseTableWithAlias parses a table name and optional alias from the lexer.
// Supports formats: "table_name" or "table_name alias_name"
func parseTableWithAlias(l *lexer.Lexer) (tableName, alias string, err error) {
	token := l.NextToken()
	if err := expectToken(token, lexer.IDENTIFIER); err != nil {
		return "", "", err
	}

	tableName = token.Value
	alias = tableName

	token = l.NextToken()
	if token.Type == lexer.IDENTIFIER {
		alias = token.Value
	} else {
		l.SetPos(token.Position)
	}

	return tableName, alias, nil
}

// parseOptionalWhereClause parses an optional WHERE clause for an UPDATE statement.
// If a WHERE token is found, it parses the condition and adds it to the statement.
// If no WHERE clause is present, the token is put back and no error is returned.
func parseOptionalWhereClause(l *lexer.Lexer, stmt *statements.UpdateStatement) error {
	token := l.NextToken()
	if token.Type == lexer.WHERE {
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
func parseSetClauses(l *lexer.Lexer, stmt *statements.UpdateStatement) error {
	for {
		token := l.NextToken()
		if err := expectToken(token, lexer.IDENTIFIER); err != nil {
			return err
		}
		fieldName := token.Value

		token = l.NextToken()
		if token.Type != lexer.OPERATOR || token.Value != "=" {
			return fmt.Errorf("expected '=', got %s", token.Value)
		}

		value, err := parseValue(l)
		if err != nil {
			return err
		}

		stmt.AddSetClause(fieldName, value)
		token = l.NextToken()
		if token.Type == lexer.COMMA {
			continue
		}

		l.SetPos(token.Position)
		break
	}
	return nil
}

package parser

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/types"
	"strings"
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

type SelectParser struct{}

type joinCondition struct {
	leftField  string
	rightField string
	predicate  primitives.Predicate
}

// parseSelectStatement is the main entry point for parsing SELECT statements.
// It orchestrates the parsing of all SELECT clauses in order: SELECT, FROM, WHERE, GROUP BY, ORDER BY.
// Also handles set operations (UNION, INTERSECT, EXCEPT) which can combine multiple SELECT statements.
//
// Grammar:
//
//	SELECT [* | field_list] FROM table_list [WHERE conditions] [GROUP BY field] [ORDER BY field [ASC|DESC]]
//	[UNION [ALL] | INTERSECT [ALL] | EXCEPT [ALL] SELECT ...]
//
// Returns a SelectStatement ready for execution planning, or an error if parsing fails.
func (s *SelectParser) Parse(l *Lexer) (*statements.SelectStatement, error) {
	p := plan.NewSelectPlan()

	parseFuncs := []func(*Lexer, *plan.SelectPlan) error{
		s.parseSelect,
		s.parseFrom,
		s.parseWhere,
		s.parseGroupBy,
		s.parseOrderBy,
		s.parseLimit,
	}

	for _, parseFunc := range parseFuncs {
		if err := parseFunc(l, p); err != nil {
			return nil, err
		}
	}

	setOpPlan, err := s.parseSetOperation(l, p)
	if err != nil {
		return nil, err
	}

	if setOpPlan != nil {
		return statements.NewSelectStatement(setOpPlan), nil
	}

	return statements.NewSelectStatement(p), nil
}

// parseSelect parses the SELECT clause, handling both SELECT * and explicit field lists.
// Also handles aggregate functions like COUNT(field), SUM(field), etc.
// Supports DISTINCT keyword: SELECT DISTINCT ...
//
// Grammar:
//
//	SELECT [DISTINCT] * | field [, field]*
//	field = IDENTIFIER | AGGREGATE_FUNC(IDENTIFIER)
func (s *SelectParser) parseSelect(l *Lexer, p *plan.SelectPlan) error {
	if err := expectTokenSequence(l, SELECT); err != nil {
		return err
	}

	token := l.NextToken()

	if token.Type == DISTINCT {
		p.SetDistinct(true)
		token = l.NextToken() // Get next token after DISTINCT
	}

	if token.Type == ASTERISK {
		p.SetSelectAll(true)
		return nil
	}

	for {
		if token.Type == FROM {
			l.SetPos(token.Position)
			break
		}

		if err := expectToken(token, IDENTIFIER); err != nil {
			return fmt.Errorf("expected field name, got %s", token.Value)
		}

		if err := s.parseSelectField(l, p, token); err != nil {
			return err
		}

		token = l.NextToken()
		if token.Type != COMMA {
			l.SetPos(token.Position)
			break
		}

		token = l.NextToken()
	}

	return nil
}

// parseSelectField handles a single field in the SELECT clause.
// Distinguishes between regular fields (NAME) and aggregate functions (COUNT(ID)).
func (s *SelectParser) parseSelectField(l *Lexer, p *plan.SelectPlan, fieldToken Token) error {
	nextToken := l.NextToken()

	// Check if this is an aggregate function call (e.g., COUNT(ID))
	if nextToken.Type == LPAREN {
		aggOp := strings.ToUpper(fieldToken.Value)
		fieldToken := l.NextToken()

		var fieldName string
		switch fieldToken.Type {
		case ASTERISK:
			fieldName = "*"
		case IDENTIFIER:
			fieldName = strings.ToUpper(fieldToken.Value)
		default:
			return fmt.Errorf("expected field name or * in aggregate function, got %s", fieldToken.Value)
		}

		parenToken := l.NextToken()
		if err := expectToken(parenToken, RPAREN); err != nil {
			return fmt.Errorf("expected closing parenthesis in aggregate function")
		}
		return p.AddProjectField(fieldName, aggOp)
	}

	l.SetPos(nextToken.Position)
	return p.AddProjectField(strings.ToUpper(fieldToken.Value), "")
}

// parseGroupBy parses the optional GROUP BY clause.
//
// Grammar:
//
//	[GROUP BY field]
//
// Example: GROUP BY DEPARTMENT
// Currently supports grouping by a single field only.
func (s *SelectParser) parseGroupBy(l *Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != GROUP {
		l.SetPos(token.Position)
		return nil
	}

	if err := expectTokenSequence(l, BY); err != nil {
		return fmt.Errorf("expected BY after GROUP, got %s", err)
	}

	fieldToken := l.NextToken()
	if err := expectToken(fieldToken, IDENTIFIER); err != nil {
		return fmt.Errorf("expected field name in GROUP BY, got %s", fieldToken.Value)
	}

	p.SetGroupBy(strings.ToUpper(fieldToken.Value))
	return nil
}

// parseOrderBy parses the optional ORDER BY clause.
//
// Grammar:
//
//	[ORDER BY field [ASC|DESC]]
//
// Example: ORDER BY AGE DESC
// Defaults to ascending if no direction specified.
func (s *SelectParser) parseOrderBy(l *Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != ORDER {
		l.SetPos(token.Position)
		return nil
	}

	if err := expectTokenSequence(l, BY); err != nil {
		return fmt.Errorf("expected BY after ORDER, got %s", err)
	}

	fieldToken := l.NextToken()
	if err := expectToken(fieldToken, IDENTIFIER); err != nil {
		return fmt.Errorf("expected field name in ORDER BY, got %s", fieldToken.Value)
	}

	ascending := true
	dirToken := l.NextToken()
	if dirToken.Type == DESC {
		ascending = false
	} else {
		l.SetPos(dirToken.Position)
	}

	p.AddOrderBy(strings.ToUpper(fieldToken.Value), ascending)
	return nil
}

// parseLimit parses the optional LIMIT clause with optional OFFSET.
//
// Grammar:
//
//	[LIMIT number [OFFSET number]]
//
// Example: LIMIT 10 OFFSET 5
// Defaults to offset 0 if not specified.
func (s *SelectParser) parseLimit(l *Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != LIMIT {
		l.SetPos(token.Position)
		return nil
	}

	limitToken := l.NextToken()
	if limitToken.Type != INT {
		return fmt.Errorf("expected integer after LIMIT, got %s", limitToken.Value)
	}

	var limitValue = 0
	if _, err := fmt.Sscanf(limitToken.Value, "%d", &limitValue); err != nil {
		return fmt.Errorf("invalid LIMIT value: %s", limitToken.Value)
	}

	offset := 0
	offsetToken := l.NextToken()
	if offsetToken.Type == OFFSET {
		offsetValueToken := l.NextToken()
		if offsetValueToken.Type != INT {
			return fmt.Errorf("expected integer after OFFSET, got %s", offsetValueToken.Value)
		}
		if _, err := fmt.Sscanf(offsetValueToken.Value, "%d", &offset); err != nil {
			return fmt.Errorf("invalid OFFSET value: %s", offsetValueToken.Value)
		}
	} else {
		l.SetPos(offsetToken.Position)
	}

	if offset < 0 || limitValue < 0 {
		return fmt.Errorf("limit and offset cannot be zero")
	}

	p.SetLimit(primitives.RowID(limitValue), primitives.RowID(offset))
	return nil
}

// parseFrom parses the FROM clause including table references and JOIN operations.
//
// Grammar:
//
//	FROM table [, table]* | table [JOIN table ON condition]*
//
// Supports:
//   - Comma-separated tables (cross join): FROM users, orders
//   - Explicit JOINs: FROM users INNER JOIN orders ON users.id = orders.user_id
//   - LEFT/RIGHT OUTER JOINs
//
// Tables can have aliases: FROM users u, orders o
func (s *SelectParser) parseFrom(l *Lexer, p *plan.SelectPlan) error {
	if err := expectTokenSequence(l, FROM); err != nil {
		return err
	}

	if err := s.parseTable(l, p); err != nil {
		return err
	}

	for {
		token := l.NextToken()
		switch token.Type {
		case COMMA:
			// Legacy comma-separated tables (cross join)
			if err := s.parseTable(l, p); err != nil {
				return err
			}
		case INNER, LEFT, RIGHT, JOIN:
			if err := s.parseJoin(l, p, token); err != nil {
				return err
			}
		default:
			l.SetPos(token.Position)
			return nil
		}
	}
}

// parseJoin parses a JOIN clause including the join type, table, and condition.
//
// Grammar:
//
//	[INNER|LEFT|RIGHT] [OUTER] JOIN table [alias] ON field = field
//
// Examples:
//
//	INNER JOIN orders ON users.id = orders.user_id
//	LEFT OUTER JOIN departments d ON e.dept_id = d.id
//	RIGHT JOIN products p ON o.product_id = p.id
func (s *SelectParser) parseJoin(l *Lexer, p *plan.SelectPlan, firstToken Token) error {
	joinType, err := s.parseJoinType(l, firstToken)
	if err != nil {
		return err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return fmt.Errorf("error parsing JOIN table: %w", err)
	}

	rightTable := plan.NewScanNode(tableName, alias)
	cond, err := s.parseJoinCondition(l)
	if err != nil {
		return err
	}

	p.AddJoin(rightTable, joinType, cond.leftField, cond.rightField, cond.predicate)
	return nil
}

// parseJoinType determines the type of JOIN operation.
// Handles INNER JOIN, LEFT [OUTER] JOIN, RIGHT [OUTER] JOIN.
// Returns the corresponding JoinType enum value.
func (s *SelectParser) parseJoinType(l *Lexer, first Token) (plan.JoinType, error) {
	joinType := plan.InnerJoin
	var err error

	switch first.Type {
	case INNER:
		err = expectTokenSequence(l, JOIN)

	case LEFT:
		joinType = plan.LeftJoin
		err = s.parseOptionalOuterJoin(l)

	case RIGHT:
		joinType = plan.RightJoin
		err = s.parseOptionalOuterJoin(l)

	}

	return joinType, err
}

// parseOptionalOuterJoin consumes the optional OUTER keyword followed by required JOIN keyword.
// Handles syntax: [OUTER] JOIN
func (s *SelectParser) parseOptionalOuterJoin(l *Lexer) error {
	t := l.NextToken()
	if t.Type == OUTER {
		t = l.NextToken()
	}

	if err := expectToken(t, JOIN); err != nil {
		return fmt.Errorf("expected JOIN: %w", err)
	}
	return nil
}

// parseTable parses a single table reference with optional alias.
func (s *SelectParser) parseTable(l *Lexer, p *plan.SelectPlan) error {
	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return err
	}
	p.AddScan(tableName, alias)
	return nil
}

// parseWhere parses the optional WHERE clause containing filter conditions.
//
// Grammar:
//
//	[WHERE condition [AND condition]*]
//	condition = field OPERATOR value
//
// Examples:
//
//	WHERE AGE > 18
//	WHERE NAME = 'John' AND STATUS = 'ACTIVE'
//
// Supports operators: =, !=, <, >, <=, >=
func (s *SelectParser) parseWhere(l *Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != WHERE {
		l.SetPos(token.Position)
		return nil
	}

	for {
		fieldToken := l.NextToken()
		if fieldToken.Type != IDENTIFIER {
			l.SetPos(fieldToken.Position)
			break
		}

		opToken := l.NextToken()
		if err := expectToken(opToken, OPERATOR); err != nil {
			return fmt.Errorf("expected operator, got %s", opToken.Value)
		}

		valueToken := l.NextToken()
		var value string

		switch valueToken.Type {
		case STRING, BOOLEAN, INT, IDENTIFIER:
			value = valueToken.Value
		default:
			return fmt.Errorf("expected value, got %s", valueToken.Value)
		}

		pred, err := parseOperator(opToken.Value)
		if err != nil {
			return err
		}

		if err := p.AddFilter(strings.ToUpper(fieldToken.Value), pred, strings.ToUpper(value)); err != nil {
			return err
		}

		nextToken := l.NextToken()
		if nextToken.Type == AND {
			continue
		} else {
			l.SetPos(nextToken.Position)
			break
		}
	}

	return nil
}

// parseJoinCondition parses the ON clause of a JOIN statement.
//
// Grammar:
//
//	ON field OPERATOR field
//
// Example: ON users.id = orders.user_id
// Returns a joinCondition with left field, right field, and comparison predicate.
func (s *SelectParser) parseJoinCondition(l *Lexer) (*joinCondition, error) {
	if err := expectTokenSequence(l, ON); err != nil {
		return nil, fmt.Errorf("expected ON in JOIN clause: %w", err)
	}

	leftFieldToken := l.NextToken()
	if err := expectToken(leftFieldToken, IDENTIFIER); err != nil {
		return nil, fmt.Errorf("expected left field in JOIN condition, got %s", leftFieldToken.Value)
	}

	opToken := l.NextToken()
	if err := expectToken(opToken, OPERATOR); err != nil {
		return nil, fmt.Errorf("expected operator in JOIN condition, got %s", opToken.Value)
	}

	rightFieldToken := l.NextToken()
	if err := expectToken(rightFieldToken, IDENTIFIER); err != nil {
		return nil, fmt.Errorf("expected right field in JOIN condition, got %s", rightFieldToken.Value)
	}

	predicate, err := parseOperator(opToken.Value)
	if err != nil {
		return nil, err
	}

	cond := &joinCondition{
		leftField:  strings.ToUpper(leftFieldToken.Value),
		rightField: strings.ToUpper(rightFieldToken.Value),
		predicate:  predicate,
	}

	return cond, nil
}

// parseSetOperation checks for and parses set operations (UNION, INTERSECT, EXCEPT).
// If a set operation is found, it recursively parses the right SELECT statement
// and returns a SetOperationPlan wrapping both sides.
//
// Grammar:
//
//	[UNION [ALL] | INTERSECT [ALL] | EXCEPT [ALL] SELECT ...]
//
// Returns:
//   - *plan.SelectPlan: A SetOperationPlan if a set operation is found, nil otherwise
//   - error: An error if parsing fails
func (s *SelectParser) parseSetOperation(l *Lexer, leftPlan *plan.SelectPlan) (*plan.SelectPlan, error) {
	token := l.NextToken()

	var opType plan.SetOperationType
	var isAll bool = false

	switch token.Type {
	case UNION:
		opType = plan.UnionOp
	case INTERSECT:
		opType = plan.IntersectOp
	case EXCEPT:
		opType = plan.ExceptOp
	default:
		l.SetPos(token.Position)
		return nil, nil
	}

	nextToken := l.NextToken()
	if nextToken.Type == ALL {
		isAll = true
	} else {
		l.SetPos(nextToken.Position)
	}

	selectToken := l.NextToken()
	if selectToken.Type != SELECT {
		return nil, fmt.Errorf("expected SELECT after %s, got %s", token.Value, selectToken.Value)
	}
	l.SetPos(selectToken.Position)

	rightStmt, err := s.Parse(l)
	if err != nil {
		return nil, fmt.Errorf("failed to parse right side of %s: %v", token.Value, err)
	}

	setOpPlan := plan.NewSetOperationPlan(leftPlan, rightStmt.Plan, opType, isAll)
	return setOpPlan, nil
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

// parseValueList parses a comma-separated list of values terminated by ')'.
// Used when parsing INSERT value tuples without the surrounding parentheses.
func parseValueList(l *Lexer) ([]types.Field, error) {
	return parseDelimitedList(l, parseValue, COMMA, RPAREN)
}

// parseConditions parses one or more filter conditions (field op value) separated
// by AND, without expecting a leading WHERE keyword. Used to test condition parsing
// in isolation.
func parseConditions(l *Lexer, p *plan.SelectPlan) error {
	for {
		fieldToken := l.NextToken()
		if fieldToken.Type != IDENTIFIER {
			l.SetPos(fieldToken.Position)
			break
		}

		opToken := l.NextToken()
		if err := expectToken(opToken, OPERATOR); err != nil {
			return fmt.Errorf("expected operator, got %s", opToken.Value)
		}

		valueToken := l.NextToken()
		var value string
		switch valueToken.Type {
		case STRING, BOOLEAN, INT, IDENTIFIER:
			value = valueToken.Value
		default:
			return fmt.Errorf("expected value, got %s", valueToken.Value)
		}

		pred, err := parseOperator(opToken.Value)
		if err != nil {
			return err
		}

		if err := p.AddFilter(fieldToken.Value, pred, value); err != nil {
			return err
		}

		nextToken := l.NextToken()
		if nextToken.Type == AND {
			continue
		}
		l.SetPos(nextToken.Position)
		break
	}
	return nil
}

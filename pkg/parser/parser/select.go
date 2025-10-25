package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"strings"
)

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
func parseSelectStatement(l *lexer.Lexer) (*statements.SelectStatement, error) {
	p := plan.NewSelectPlan()

	parseFuncs := []func(*lexer.Lexer, *plan.SelectPlan) error{
		parseSelect,
		parseFrom,
		parseWhere,
		parseGroupBy,
		parseOrderBy,
		parseLimit,
	}

	for _, parseFunc := range parseFuncs {
		if err := parseFunc(l, p); err != nil {
			return nil, err
		}
	}

	// Check for set operations (UNION, INTERSECT, EXCEPT)
	setOpPlan, err := parseSetOperation(l, p)
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
func parseSelect(l *lexer.Lexer, p *plan.SelectPlan) error {
	if err := expectTokenSequence(l, lexer.SELECT); err != nil {
		return err
	}

	token := l.NextToken()

	if token.Type == lexer.DISTINCT {
		p.SetDistinct(true)
		token = l.NextToken() // Get next token after DISTINCT
	}

	if token.Type == lexer.ASTERISK {
		p.SetSelectAll(true)
		return nil
	}

	return parseSelectFieldList(l, p, token)
}

// parseSelectFieldList processes a comma-separated list of fields in the SELECT clause.
// Stops when it encounters the FROM keyword.
func parseSelectFieldList(l *lexer.Lexer, p *plan.SelectPlan, firstToken lexer.Token) error {
	token := firstToken

	for {
		if token.Type == lexer.FROM {
			l.SetPos(token.Position)
			break
		}

		if err := expectToken(token, lexer.IDENTIFIER); err != nil {
			return fmt.Errorf("expected field name, got %s", token.Value)
		}

		if err := parseSelectField(l, p, token); err != nil {
			return err
		}

		if !consumeCommaIfPresent(l) {
			break
		}

		token = l.NextToken()
	}

	return nil
}

// parseSelectField handles a single field in the SELECT clause.
// Distinguishes between regular fields (NAME) and aggregate functions (COUNT(ID)).
func parseSelectField(l *lexer.Lexer, p *plan.SelectPlan, fieldToken lexer.Token) error {
	nextToken := l.NextToken()
	if nextToken.Type == lexer.LPAREN {
		return parseAggregateFunction(l, p, fieldToken)
	}

	l.SetPos(nextToken.Position)
	p.AddProjectField(strings.ToUpper(fieldToken.Value), "")
	return nil
}

// parseAggregateFunction parses aggregate function calls in SELECT clause.
//
// Grammar:
//
//	AGGREGATE_FUNC(IDENTIFIER | *)
//
// Supported functions: COUNT, SUM, AVG, MIN, MAX
// Example: COUNT(ID) → adds projection with field "ID" and aggregation operator "COUNT"
// Example: COUNT(*) → adds projection with field "*" and aggregation operator "COUNT"
func parseAggregateFunction(l *lexer.Lexer, p *plan.SelectPlan, funcToken lexer.Token) error {
	aggOp := strings.ToUpper(funcToken.Value)

	fieldToken := l.NextToken()

	// Handle COUNT(*) special case
	var fieldName string
	switch fieldToken.Type {
	case lexer.ASTERISK:
		fieldName = "*"
	case lexer.IDENTIFIER:
		fieldName = strings.ToUpper(fieldToken.Value)
	default:
		return fmt.Errorf("expected field name or * in aggregate function, got %s", fieldToken.Value)
	}

	parenToken := l.NextToken()
	if err := expectToken(parenToken, lexer.RPAREN); err != nil {
		return fmt.Errorf("expected closing parenthesis in aggregate function")
	}

	p.AddProjectField(fieldName, aggOp)
	return nil
}

// parseGroupBy parses the optional GROUP BY clause.
//
// Grammar:
//
//	[GROUP BY field]
//
// Example: GROUP BY DEPARTMENT
// Currently supports grouping by a single field only.
func parseGroupBy(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.GROUP {
		l.SetPos(token.Position)
		return nil
	}

	if err := expectTokenSequence(l, lexer.BY); err != nil {
		return fmt.Errorf("expected BY after GROUP, got %s", err)
	}

	fieldToken := l.NextToken()
	if err := expectToken(fieldToken, lexer.IDENTIFIER); err != nil {
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
func parseOrderBy(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.ORDER {
		l.SetPos(token.Position)
		return nil
	}

	if err := expectTokenSequence(l, lexer.BY); err != nil {
		return fmt.Errorf("expected BY after ORDER, got %s", err)
	}

	fieldToken := l.NextToken()
	if err := expectToken(fieldToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected field name in ORDER BY, got %s", fieldToken.Value)
	}

	ascending := true
	dirToken := l.NextToken()
	if dirToken.Type == lexer.DESC {
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
func parseLimit(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.LIMIT {
		l.SetPos(token.Position)
		return nil
	}

	limitToken := l.NextToken()
	if limitToken.Type != lexer.INT {
		return fmt.Errorf("expected integer after LIMIT, got %s", limitToken.Value)
	}

	limitValue := 0
	if _, err := fmt.Sscanf(limitToken.Value, "%d", &limitValue); err != nil {
		return fmt.Errorf("invalid LIMIT value: %s", limitToken.Value)
	}

	offset := 0
	offsetToken := l.NextToken()
	if offsetToken.Type == lexer.OFFSET {
		offsetValueToken := l.NextToken()
		if offsetValueToken.Type != lexer.INT {
			return fmt.Errorf("expected integer after OFFSET, got %s", offsetValueToken.Value)
		}
		if _, err := fmt.Sscanf(offsetValueToken.Value, "%d", &offset); err != nil {
			return fmt.Errorf("invalid OFFSET value: %s", offsetValueToken.Value)
		}
	} else {
		l.SetPos(offsetToken.Position)
	}

	p.SetLimit(limitValue, offset)
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
func parseFrom(l *lexer.Lexer, p *plan.SelectPlan) error {
	if err := expectTokenSequence(l, lexer.FROM); err != nil {
		return err
	}

	if err := parseTable(l, p); err != nil {
		return err
	}

	for {
		token := l.NextToken()
		switch token.Type {
		case lexer.COMMA:
			// Legacy comma-separated tables (cross join)
			if err := parseTable(l, p); err != nil {
				return err
			}
		case lexer.INNER, lexer.LEFT, lexer.RIGHT, lexer.JOIN:
			if err := parseJoin(l, p, token); err != nil {
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
func parseJoin(l *lexer.Lexer, p *plan.SelectPlan, firstToken lexer.Token) error {
	joinType, err := parseJoinType(l, firstToken)
	if err != nil {
		return err
	}

	tableName, alias, err := parseTableWithAlias(l)
	if err != nil {
		return fmt.Errorf("error parsing JOIN table: %w", err)
	}

	rightTable := plan.NewScanNode(tableName, alias)
	cond, err := parseJoinCondition(l)
	if err != nil {
		return err
	}

	p.AddJoin(rightTable, joinType, cond.leftField, cond.rightField, cond.predicate)
	return nil
}

// parseJoinType determines the type of JOIN operation.
// Handles INNER JOIN, LEFT [OUTER] JOIN, RIGHT [OUTER] JOIN.
// Returns the corresponding JoinType enum value.
func parseJoinType(l *lexer.Lexer, first lexer.Token) (plan.JoinType, error) {
	joinType := plan.InnerJoin
	var err error

	switch first.Type {
	case lexer.INNER:
		err = expectTokenSequence(l, lexer.JOIN)

	case lexer.LEFT:
		joinType = plan.LeftJoin
		err = parseOptionalOuterJoin(l)

	case lexer.RIGHT:
		joinType = plan.RightJoin
		err = parseOptionalOuterJoin(l)

	}

	return joinType, err
}

// parseOptionalOuterJoin consumes the optional OUTER keyword followed by required JOIN keyword.
// Handles syntax: [OUTER] JOIN
func parseOptionalOuterJoin(l *lexer.Lexer) error {
	t := l.NextToken()
	if t.Type == lexer.OUTER {
		t = l.NextToken()
	}

	if err := expectToken(t, lexer.JOIN); err != nil {
		return fmt.Errorf("expected JOIN: %w", err)
	}
	return nil
}

// parseTable parses a single table reference with optional alias.
func parseTable(l *lexer.Lexer, p *plan.SelectPlan) error {
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
func parseWhere(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.WHERE {
		l.SetPos(token.Position)
		return nil
	}

	return parseConditions(l, p)
}

// parseJoinCondition parses the ON clause of a JOIN statement.
//
// Grammar:
//
//	ON field OPERATOR field
//
// Example: ON users.id = orders.user_id
// Returns a joinCondition with left field, right field, and comparison predicate.
func parseJoinCondition(l *lexer.Lexer) (*joinCondition, error) {
	if err := expectTokenSequence(l, lexer.ON); err != nil {
		return nil, fmt.Errorf("expected ON in JOIN clause: %w", err)
	}

	leftFieldToken := l.NextToken()
	if err := expectToken(leftFieldToken, lexer.IDENTIFIER); err != nil {
		return nil, fmt.Errorf("expected left field in JOIN condition, got %s", leftFieldToken.Value)
	}

	opToken := l.NextToken()
	if err := expectToken(opToken, lexer.OPERATOR); err != nil {
		return nil, fmt.Errorf("expected operator in JOIN condition, got %s", opToken.Value)
	}

	rightFieldToken := l.NextToken()
	if err := expectToken(rightFieldToken, lexer.IDENTIFIER); err != nil {
		return nil, fmt.Errorf("expected right field in JOIN condition, got %s", rightFieldToken.Value)
	}

	predicate, err := parseOperator(opToken.Value)
	if err != nil {
		return nil, err
	}

	cond := &joinCondition{
		leftField:  strings.ToUpper(leftFieldToken.Value),
		rightField: strings.ToUpper(rightFieldToken.Value),
		predicate:  predicate}

	return cond, nil
}

// parseConditions parses a sequence of filter conditions connected by AND.
// Each condition follows the pattern: field OPERATOR value
//
// Stops when encountering a token that's not part of a condition (e.g., GROUP, ORDER, EOF).
// All field names and values are normalized to uppercase.
func parseConditions(l *lexer.Lexer, p *plan.SelectPlan) error {
	for {
		fieldToken := l.NextToken()
		if fieldToken.Type != lexer.IDENTIFIER {
			l.SetPos(fieldToken.Position)
			break
		}

		opToken := l.NextToken()
		if err := expectToken(opToken, lexer.OPERATOR); err != nil {
			return fmt.Errorf("expected operator, got %s", opToken.Value)
		}

		valueToken := l.NextToken()
		var value string

		switch valueToken.Type {
		case lexer.STRING, lexer.BOOLEAN, lexer.INT, lexer.IDENTIFIER:
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
		if nextToken.Type == lexer.AND {
			continue
		} else {
			l.SetPos(nextToken.Position)
			break
		}
	}

	return nil
}

// consumeCommaIfPresent checks if the next token is a comma and consumes it.
func consumeCommaIfPresent(l *lexer.Lexer) bool {
	token := l.NextToken()
	if token.Type == lexer.COMMA {
		return true
	}

	l.SetPos(token.Position)
	return false
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
func parseSetOperation(l *lexer.Lexer, leftPlan *plan.SelectPlan) (*plan.SelectPlan, error) {
	token := l.NextToken()

	var opType plan.SetOperationType
	var isAll bool = false

	switch token.Type {
	case lexer.UNION:
		opType = plan.UnionOp
	case lexer.INTERSECT:
		opType = plan.IntersectOp
	case lexer.EXCEPT:
		opType = plan.ExceptOp
	default:
		l.SetPos(token.Position)
		return nil, nil
	}

	nextToken := l.NextToken()
	if nextToken.Type == lexer.ALL {
		isAll = true
	} else {
		l.SetPos(nextToken.Position)
	}

	selectToken := l.NextToken()
	if selectToken.Type != lexer.SELECT {
		return nil, fmt.Errorf("expected SELECT after %s, got %s", token.Value, selectToken.Value)
	}
	l.SetPos(selectToken.Position)

	rightStmt, err := parseSelectStatement(l)
	if err != nil {
		return nil, fmt.Errorf("failed to parse right side of %s: %v", token.Value, err)
	}

	setOpPlan := plan.NewSetOperationPlan(leftPlan, rightStmt.Plan, opType, isAll)
	return setOpPlan, nil
}

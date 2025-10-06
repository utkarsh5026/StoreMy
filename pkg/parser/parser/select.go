package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
	"strings"
)

func parseSelectStatement(l *lexer.Lexer) (*statements.SelectStatement, error) {
	p := plan.NewSelectPlan()

	parseFuncs := []func(*lexer.Lexer, *plan.SelectPlan) error{
		parseSelect,
		parseFrom,
		parseWhere,
		parseGroupBy,
		parseOrderBy,
	}

	for _, parseFunc := range parseFuncs {
		if err := parseFunc(l, p); err != nil {
			return nil, err
		}
	}

	return statements.NewSelectStatement(p), nil
}

func parseSelect(l *lexer.Lexer, p *plan.SelectPlan) error {
	if err := expectTokenSequence(l, lexer.SELECT); err != nil {
		return err
	}

	token := l.NextToken()
	if token.Type == lexer.ASTERISK {
		p.SetSelectAll(true)
		return nil
	}

	return parseSelectFieldList(l, p, token)
}

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

func parseSelectField(l *lexer.Lexer, p *plan.SelectPlan, fieldToken lexer.Token) error {
	nextToken := l.NextToken()
	if nextToken.Type == lexer.LPAREN {
		return parseAggregateFunction(l, p, fieldToken)
	}

	l.SetPos(nextToken.Position)
	p.AddProjectField(strings.ToUpper(fieldToken.Value), "")
	return nil
}

func parseAggregateFunction(l *lexer.Lexer, p *plan.SelectPlan, funcToken lexer.Token) error {
	aggOp := strings.ToUpper(funcToken.Value)

	fieldToken := l.NextToken()
	if err := expectToken(fieldToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected field name in aggregate function")
	}

	parenToken := l.NextToken()
	if err := expectToken(parenToken, lexer.RPAREN); err != nil {
		return fmt.Errorf("expected closing parenthesis in aggregate function")
	}

	p.AddProjectField(strings.ToUpper(fieldToken.Value), aggOp)
	return nil
}

func parseGroupBy(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.GROUP {
		l.SetPos(token.Position)
		return nil
	}

	by := l.NextToken()
	if err := expectToken(by, lexer.BY); err != nil {
		return fmt.Errorf("expected BY after GROUP, got %s", by.Value)
	}

	fieldToken := l.NextToken()
	if err := expectToken(fieldToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected field name in GROUP BY, got %s", fieldToken.Value)
	}

	p.SetGroupBy(strings.ToUpper(fieldToken.Value))
	return nil
}

func parseOrderBy(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.ORDER {
		l.SetPos(token.Position)
		return nil
	}

	by := l.NextToken()
	if err := expectToken(by, lexer.BY); err != nil {
		return fmt.Errorf("expected BY after ORDER, got %s", by.Value)
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

func parseFrom(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if err := expectToken(token, lexer.FROM); err != nil {
		return err
	}

	// Parse first table
	if err := parseTable(l, p); err != nil {
		return err
	}

	// Parse JOIN clauses or comma-separated tables
	for {
		token := l.NextToken()

		if token.Type == lexer.COMMA {
			// Legacy comma-separated tables (cross join)
			if err := parseTable(l, p); err != nil {
				return err
			}
			continue
		}

		if token.Type == lexer.INNER || token.Type == lexer.LEFT || token.Type == lexer.RIGHT || token.Type == lexer.JOIN {
			// Handle JOIN
			if err := parseJoin(l, p, token); err != nil {
				return err
			}
			continue
		}

		// No more tables or joins
		l.SetPos(token.Position)
		return nil
	}
}

func parseJoin(l *lexer.Lexer, p *plan.SelectPlan, firstToken lexer.Token) error {
	// Determine join type
	joinType := plan.InnerJoin // default

	if firstToken.Type == lexer.INNER {
		// Expect JOIN token
		joinToken := l.NextToken()
		if err := expectToken(joinToken, lexer.JOIN); err != nil {
			return fmt.Errorf("expected JOIN after INNER, got %s", joinToken.Value)
		}
		joinType = plan.InnerJoin
	} else if firstToken.Type == lexer.LEFT {
		// Expect optional OUTER, then JOIN
		nextToken := l.NextToken()
		if nextToken.Type == lexer.OUTER {
			nextToken = l.NextToken()
		}
		if err := expectToken(nextToken, lexer.JOIN); err != nil {
			return fmt.Errorf("expected JOIN after LEFT, got %s", nextToken.Value)
		}
		joinType = plan.LeftJoin
	} else if firstToken.Type == lexer.RIGHT {
		// Expect optional OUTER, then JOIN
		nextToken := l.NextToken()
		if nextToken.Type == lexer.OUTER {
			nextToken = l.NextToken()
		}
		if err := expectToken(nextToken, lexer.JOIN); err != nil {
			return fmt.Errorf("expected JOIN after RIGHT, got %s", nextToken.Value)
		}
		joinType = plan.RightJoin
	}
	// else firstToken.Type == lexer.JOIN, already consumed

	// Parse table name and optional alias
	tableToken := l.NextToken()
	if err := expectToken(tableToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected table name in JOIN, got %s", tableToken.Value)
	}

	tableName := strings.ToUpper(tableToken.Value)
	alias := tableName

	// Check for table alias
	aliasToken := l.NextToken()
	if aliasToken.Type == lexer.IDENTIFIER {
		alias = strings.ToUpper(aliasToken.Value)
	} else {
		l.SetPos(aliasToken.Position)
	}

	rightTable := plan.NewScanNode(tableName, alias)

	// Expect ON keyword
	onToken := l.NextToken()
	if err := expectToken(onToken, lexer.ON); err != nil {
		return fmt.Errorf("expected ON in JOIN clause, got %s", onToken.Value)
	}

	// Parse join condition: table1.field1 = table2.field2
	leftFieldToken := l.NextToken()
	if err := expectToken(leftFieldToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected left field in JOIN condition, got %s", leftFieldToken.Value)
	}

	opToken := l.NextToken()
	if err := expectToken(opToken, lexer.OPERATOR); err != nil {
		return fmt.Errorf("expected operator in JOIN condition, got %s", opToken.Value)
	}

	rightFieldToken := l.NextToken()
	if err := expectToken(rightFieldToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected right field in JOIN condition, got %s", rightFieldToken.Value)
	}

	// Parse the operator
	predicate, err := parseOperator(opToken.Value)
	if err != nil {
		return err
	}

	leftField := strings.ToUpper(leftFieldToken.Value)
	rightField := strings.ToUpper(rightFieldToken.Value)

	p.AddJoin(rightTable, joinType, leftField, rightField, predicate)
	return nil
}

func parseTable(l *lexer.Lexer, p *plan.SelectPlan) error {
	tableToken := l.NextToken()
	if err := expectToken(tableToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected table name, got %s", tableToken.Value)
	}

	tableName := strings.ToUpper(tableToken.Value)
	alias := tableName

	aliasToken := l.NextToken()
	if aliasToken.Type == lexer.IDENTIFIER {
		alias = strings.ToUpper(aliasToken.Value)
	} else {
		l.SetPos(aliasToken.Position)
	}

	p.AddScan(tableName, alias)
	return nil
}

func parseWhere(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if token.Type != lexer.WHERE {
		l.SetPos(token.Position)
		return nil
	}

	return parseConditions(l, p)
}

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

func consumeCommaIfPresent(l *lexer.Lexer) bool {
	token := l.NextToken()
	if token.Type == lexer.COMMA {
		return true
	}

	l.SetPos(token.Position)
	return false
}

package parser

import (
	"fmt"
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
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
	token := l.NextToken()
	if err := expectToken(token, lexer.SELECT); err != nil {
		return err
	}

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
			return nil
		}
	}

	return nil
}

func parseSelectField(l *lexer.Lexer, p *plan.SelectPlan, fieldToken lexer.Token) error {
	nextToken := l.NextToken()
	if nextToken.Type == lexer.LPAREN {
		aggOp := fieldToken.Value
		fieldToken := l.NextToken()
		if err := expectToken(fieldToken, lexer.IDENTIFIER); err != nil {
			return fmt.Errorf("expected field name in aggregate function")
		}

		parenToken := l.NextToken()
		if err := expectToken(parenToken, lexer.RPAREN); err != nil {
			return fmt.Errorf("expected closing parenthesis in aggregate function")
		}
		p.AddProjectField(fieldToken.Value, aggOp)
	} else {
		l.SetPos(nextToken.Position)
		p.AddProjectField(fieldToken.Value, "")
	}
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

	p.SetGroupBy(fieldToken.Value)
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

	p.AddOrderBy(fieldToken.Value, ascending)
	return nil
}

func parseFrom(l *lexer.Lexer, p *plan.SelectPlan) error {
	token := l.NextToken()
	if err := expectToken(token, lexer.FROM); err != nil {
		return err
	}

	for {
		if err := parseTable(l, p); err != nil {
			return err
		}

		if !consumeCommaIfPresent(l) {
			return nil
		}
	}
}

func parseTable(l *lexer.Lexer, p *plan.SelectPlan) error {
	tableToken := l.NextToken()
	if err := expectToken(tableToken, lexer.IDENTIFIER); err != nil {
		return fmt.Errorf("expected table name, got %s", tableToken.Value)
	}

	tableName := tableToken.Value
	alias := tableName

	aliasToken := l.NextToken()
	if aliasToken.Type == lexer.IDENTIFIER {
		alias = aliasToken.Value
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
		case lexer.STRING, lexer.BOOLEAN, lexer.INT:
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

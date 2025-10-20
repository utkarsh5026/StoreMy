package parser

import (
	"storemy/pkg/parser/statements"
	"testing"
)

func TestParseSelectWithInnerJoin(t *testing.T) {
	query := "SELECT users.name, orders.amount FROM users INNER JOIN orders ON users.id = orders.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	// Check we have one table scan
	tables := selectStmt.Plan.Tables()
	if len(tables) != 1 {
		t.Errorf("Expected 1 table scan, got %d", len(tables))
	}

	// Check we have one join
	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(joins))
	}

	join := joins[0]
	if join.JoinType != "inner" {
		t.Errorf("Expected inner join, got %v", join.JoinType)
	}

	if join.RightTable.TableName != "ORDERS" {
		t.Errorf("Expected right table 'ORDERS', got '%s'", join.RightTable.TableName)
	}

	if join.LeftField != "USERS.ID" {
		t.Errorf("Expected left field 'USERS.ID', got '%s'", join.LeftField)
	}

	if join.RightField != "ORDERS.USER_ID" {
		t.Errorf("Expected right field 'ORDERS.USER_ID', got '%s'", join.RightField)
	}
}

func TestParseSelectWithSimpleJoin(t *testing.T) {
	query := "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(joins))
	}

	join := joins[0]
	if join.JoinType != "inner" {
		t.Errorf("Expected inner join for plain JOIN, got %v", join.JoinType)
	}
}

func TestParseSelectWithLeftJoin(t *testing.T) {
	query := "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(joins))
	}

	join := joins[0]
	if join.JoinType != "left" {
		t.Errorf("Expected left join, got %v", join.JoinType)
	}
}

func TestParseSelectWithLeftOuterJoin(t *testing.T) {
	query := "SELECT * FROM users LEFT OUTER JOIN orders ON users.id = orders.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(joins))
	}

	join := joins[0]
	if join.JoinType != "left" {
		t.Errorf("Expected left join for LEFT OUTER JOIN, got %v", join.JoinType)
	}
}

func TestParseSelectWithRightJoin(t *testing.T) {
	query := "SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(joins))
	}

	join := joins[0]
	if join.JoinType != "right" {
		t.Errorf("Expected right join, got %v", join.JoinType)
	}
}

func TestParseSelectWithMultipleJoins(t *testing.T) {
	query := "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id LEFT JOIN products ON orders.product_id = products.id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 2 {
		t.Fatalf("Expected 2 joins, got %d", len(joins))
	}

	if joins[0].JoinType != "inner" {
		t.Errorf("Expected first join to be inner join, got %v", joins[0].JoinType)
	}

	if joins[1].JoinType != "left" {
		t.Errorf("Expected second join to be left join, got %v", joins[1].JoinType)
	}
}

func TestParseSelectWithJoinAndTableAliases(t *testing.T) {
	query := "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	tables := selectStmt.Plan.Tables()
	if len(tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(tables))
	}

	if tables[0].Alias != "U" {
		t.Errorf("Expected alias 'U' for users table, got '%s'", tables[0].Alias)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Fatalf("Expected 1 join, got %d", len(joins))
	}

	if joins[0].RightTable.Alias != "O" {
		t.Errorf("Expected alias 'O' for orders table, got '%s'", joins[0].RightTable.Alias)
	}
}

func TestParseSelectWithJoinAndWhere(t *testing.T) {
	query := "SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.age > 25"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	joins := selectStmt.Plan.Joins()
	if len(joins) != 1 {
		t.Errorf("Expected 1 join, got %d", len(joins))
	}

	filters := selectStmt.Plan.Filters()
	if len(filters) != 1 {
		t.Errorf("Expected 1 filter, got %d", len(filters))
	}
}

func TestParseSelectBackwardCompatibleCommaJoin(t *testing.T) {
	// Ensure comma-separated tables still work
	query := "SELECT * FROM users, orders WHERE users.id = orders.user_id"
	stmt, err := ParseStatement(query)
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	selectStmt, ok := stmt.(*statements.SelectStatement)
	if !ok {
		t.Fatalf("Expected SelectStatement, got %T", stmt)
	}

	tables := selectStmt.Plan.Tables()
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables with comma syntax, got %d", len(tables))
	}

	// Comma syntax should not create joins
	joins := selectStmt.Plan.Joins()
	if len(joins) != 0 {
		t.Errorf("Expected 0 joins with comma syntax, got %d", len(joins))
	}
}

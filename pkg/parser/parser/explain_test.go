package parser

import (
	"storemy/pkg/parser/statements"
	"testing"
)

func TestParseExplainSelect(t *testing.T) {
	sql := "EXPLAIN SELECT * FROM users"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN SELECT: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if explainStmt.GetOptions().Analyze {
		t.Error("ANALYZE should be false")
	}

	if explainStmt.GetOptions().Format != "TEXT" {
		t.Errorf("Expected format TEXT, got %s", explainStmt.GetOptions().Format)
	}

	underlyingStmt := explainStmt.GetStatement()
	if underlyingStmt.GetType() != statements.Select {
		t.Errorf("Expected SELECT statement, got %s", underlyingStmt.GetType())
	}
}

func TestParseExplainAnalyze(t *testing.T) {
	sql := "EXPLAIN ANALYZE SELECT id, name FROM users WHERE age > 18"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN ANALYZE: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if !explainStmt.GetOptions().Analyze {
		t.Error("ANALYZE should be true")
	}

	underlyingStmt := explainStmt.GetStatement()
	if underlyingStmt.GetType() != statements.Select {
		t.Errorf("Expected SELECT statement, got %s", underlyingStmt.GetType())
	}
}

func TestParseExplainFormatJSON(t *testing.T) {
	sql := "EXPLAIN FORMAT JSON SELECT * FROM users"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN FORMAT JSON: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if explainStmt.GetOptions().Format != "JSON" {
		t.Errorf("Expected format JSON, got %s", explainStmt.GetOptions().Format)
	}
}

func TestParseExplainAnalyzeFormatJSON(t *testing.T) {
	sql := "EXPLAIN ANALYZE FORMAT JSON SELECT * FROM users WHERE id = 1"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN ANALYZE FORMAT JSON: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if !explainStmt.GetOptions().Analyze {
		t.Error("ANALYZE should be true")
	}

	if explainStmt.GetOptions().Format != "JSON" {
		t.Errorf("Expected format JSON, got %s", explainStmt.GetOptions().Format)
	}
}

func TestExplainStatementString(t *testing.T) {
	sql := "EXPLAIN ANALYZE SELECT * FROM users"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	explainStmt := stmt.(*statements.ExplainStatement)
	str := explainStmt.String()

	// Should contain EXPLAIN and ANALYZE
	if len(str) == 0 {
		t.Error("String() returned empty string")
	}

	t.Logf("EXPLAIN statement string: %s", str)
}

func TestParseExplainInsert(t *testing.T) {
	sql := "EXPLAIN INSERT INTO users (id, name) VALUES (1, 'John')"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN INSERT: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	underlyingStmt := explainStmt.GetStatement()
	if underlyingStmt.GetType() != statements.Insert {
		t.Errorf("Expected INSERT statement, got %s", underlyingStmt.GetType())
	}
}

func TestParseExplainUpdate(t *testing.T) {
	sql := "EXPLAIN UPDATE users SET name = 'Jane' WHERE id = 1"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN UPDATE: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	underlyingStmt := explainStmt.GetStatement()
	if underlyingStmt.GetType() != statements.Update {
		t.Errorf("Expected UPDATE statement, got %s", underlyingStmt.GetType())
	}
}

func TestParseExplainDelete(t *testing.T) {
	sql := "EXPLAIN DELETE FROM users WHERE age < 18"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN DELETE: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	underlyingStmt := explainStmt.GetStatement()
	if underlyingStmt.GetType() != statements.Delete {
		t.Errorf("Expected DELETE statement, got %s", underlyingStmt.GetType())
	}
}

func TestParseExplainFormatText(t *testing.T) {
	sql := "EXPLAIN FORMAT TEXT SELECT * FROM users"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN FORMAT TEXT: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if explainStmt.GetOptions().Format != "TEXT" {
		t.Errorf("Expected format TEXT, got %s", explainStmt.GetOptions().Format)
	}
}

func TestParseExplainAnalyzeFormatText(t *testing.T) {
	sql := "EXPLAIN ANALYZE FORMAT TEXT SELECT * FROM users"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN ANALYZE FORMAT TEXT: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if !explainStmt.GetOptions().Analyze {
		t.Error("ANALYZE should be true")
	}

	if explainStmt.GetOptions().Format != "TEXT" {
		t.Errorf("Expected format TEXT, got %s", explainStmt.GetOptions().Format)
	}
}

func TestParseExplainComplexSelect(t *testing.T) {
	sql := "EXPLAIN ANALYZE FORMAT JSON SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 21 ORDER BY o.total DESC LIMIT 10"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse complex EXPLAIN: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	if !explainStmt.GetOptions().Analyze {
		t.Error("ANALYZE should be true")
	}

	if explainStmt.GetOptions().Format != "JSON" {
		t.Errorf("Expected format JSON, got %s", explainStmt.GetOptions().Format)
	}

	underlyingStmt := explainStmt.GetStatement()
	if underlyingStmt.GetType() != statements.Select {
		t.Errorf("Expected SELECT statement, got %s", underlyingStmt.GetType())
	}
}

func TestParseExplainInvalidFormat(t *testing.T) {
	sql := "EXPLAIN FORMAT XML SELECT * FROM users"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Fatal("Expected error for invalid format type, got nil")
	}
}

func TestParseExplainInvalidStatement(t *testing.T) {
	sql := "EXPLAIN BEGIN TRANSACTION"
	_, err := ParseStatement(sql)
	if err == nil {
		t.Fatal("Expected error for non-explainable statement, got nil")
	}
}

func TestParseExplainDefaultOptions(t *testing.T) {
	sql := "EXPLAIN SELECT * FROM users"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("Failed to parse EXPLAIN: %v", err)
	}

	explainStmt, ok := stmt.(*statements.ExplainStatement)
	if !ok {
		t.Fatalf("Expected ExplainStatement, got %T", stmt)
	}

	options := explainStmt.GetOptions()
	if options.Analyze {
		t.Error("Default ANALYZE should be false")
	}

	if options.Format != "TEXT" {
		t.Errorf("Default format should be TEXT, got %s", options.Format)
	}
}

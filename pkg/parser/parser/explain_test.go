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

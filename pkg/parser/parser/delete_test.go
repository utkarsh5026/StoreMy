package parser

import (
	"storemy/pkg/parser/statements"
	"testing"
)

// DELETE statement tests

func TestParseStatement_BasicDelete(t *testing.T) {
	stmt, err := ParseStatement("DELETE FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	deleteStmt, ok := stmt.(*statements.DeleteStatement)
	if !ok {
		t.Fatal("expected DeleteStatement")
	}

	if deleteStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", deleteStmt.TableName)
	}

	if deleteStmt.Alias != "USERS" {
		t.Errorf("expected alias 'USERS', got %s", deleteStmt.Alias)
	}

	if deleteStmt.WhereClause != nil {
		t.Error("expected no WHERE clause")
	}
}

func TestParseStatement_DeleteWithAlias(t *testing.T) {
	stmt, err := ParseStatement("DELETE FROM users u")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	deleteStmt, ok := stmt.(*statements.DeleteStatement)
	if !ok {
		t.Fatal("expected DeleteStatement")
	}

	if deleteStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", deleteStmt.TableName)
	}

	if deleteStmt.Alias != "U" {
		t.Errorf("expected alias 'U', got %s", deleteStmt.Alias)
	}
}

func TestParseStatement_DeleteWithWhereClause(t *testing.T) {
	stmt, err := ParseStatement("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	deleteStmt, ok := stmt.(*statements.DeleteStatement)
	if !ok {
		t.Fatal("expected DeleteStatement")
	}

	if deleteStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", deleteStmt.TableName)
	}

	if deleteStmt.WhereClause == nil {
		t.Error("expected WHERE clause")
	}
}

func TestParseStatement_DeleteWithAliasAndWhere(t *testing.T) {
	stmt, err := ParseStatement("DELETE FROM users u WHERE u.id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	deleteStmt, ok := stmt.(*statements.DeleteStatement)
	if !ok {
		t.Fatal("expected DeleteStatement")
	}

	if deleteStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", deleteStmt.TableName)
	}

	if deleteStmt.Alias != "U" {
		t.Errorf("expected alias 'U', got %s", deleteStmt.Alias)
	}

	if deleteStmt.WhereClause == nil {
		t.Error("expected WHERE clause")
	}
}

// DELETE statement error handling tests
func TestParseDeleteStatement_MissingFrom(t *testing.T) {
	lexer := NewLexer("users")

	_, err := (&DeleteParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing FROM")
	}

	if err.Error() != "expected DELETE, got USERS" {
		t.Errorf("expected 'expected DELETE, got USERS', got %s", err.Error())
	}
}

func TestParseDeleteStatement_MissingTableName(t *testing.T) {
	lexer := NewLexer("FROM")

	_, err := (&DeleteParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected DELETE, got FROM" {
		t.Errorf("expected 'expected DELETE, got FROM', got %s", err.Error())
	}
}

func TestParseDeleteStatement_InvalidTableName(t *testing.T) {
	lexer := NewLexer("FROM 123")

	_, err := (&DeleteParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for invalid table name")
	}

	if err.Error() != "expected DELETE, got FROM" {
		t.Errorf("expected 'expected DELETE, got FROM', got %s", err.Error())
	}
}

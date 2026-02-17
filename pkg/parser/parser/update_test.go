package parser

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"testing"
)

// UPDATE statement tests

func TestParseStatement_BasicUpdate(t *testing.T) {
	stmt, err := ParseStatement("UPDATE users SET name = 'John'")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	updateStmt, ok := stmt.(*statements.UpdateStatement)
	if !ok {
		t.Fatal("expected UpdateStatement")
	}

	if updateStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", updateStmt.TableName)
	}

	if updateStmt.Alias != "USERS" {
		t.Errorf("expected alias 'USERS', got %s", updateStmt.Alias)
	}

	if len(updateStmt.SetClauses) != 1 {
		t.Errorf("expected 1 set clause, got %d", len(updateStmt.SetClauses))
	}

	if updateStmt.SetClauses[0].FieldName != "NAME" {
		t.Errorf("expected field name 'NAME', got %s", updateStmt.SetClauses[0].FieldName)
	}

	if updateStmt.WhereClause != nil {
		t.Error("expected no WHERE clause")
	}
}

func TestParseStatement_UpdateWithAlias(t *testing.T) {
	stmt, err := ParseStatement("UPDATE users u SET name = 'John'")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	updateStmt, ok := stmt.(*statements.UpdateStatement)
	if !ok {
		t.Fatal("expected UpdateStatement")
	}

	if updateStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", updateStmt.TableName)
	}

	if updateStmt.Alias != "U" {
		t.Errorf("expected alias 'U', got %s", updateStmt.Alias)
	}
}

func TestParseStatement_UpdateWithMultipleSetClauses(t *testing.T) {
	stmt, err := ParseStatement("UPDATE users SET name = 'John', age = 25")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	updateStmt, ok := stmt.(*statements.UpdateStatement)
	if !ok {
		t.Fatal("expected UpdateStatement")
	}

	if len(updateStmt.SetClauses) != 2 {
		t.Errorf("expected 2 set clauses, got %d", len(updateStmt.SetClauses))
	}

	expectedFields := []string{"NAME", "AGE"}
	for i, expected := range expectedFields {
		if updateStmt.SetClauses[i].FieldName != expected {
			t.Errorf("expected field name '%s', got %s", expected, updateStmt.SetClauses[i].FieldName)
		}
	}
}

func TestParseStatement_UpdateWithWhereClause(t *testing.T) {
	stmt, err := ParseStatement("UPDATE users SET name = 'John' WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	updateStmt, ok := stmt.(*statements.UpdateStatement)
	if !ok {
		t.Fatal("expected UpdateStatement")
	}

	if updateStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", updateStmt.TableName)
	}

	if updateStmt.WhereClause == nil {
		t.Error("expected WHERE clause")
	}
}

func TestParseStatement_UpdateWithAliasAndWhere(t *testing.T) {
	stmt, err := ParseStatement("UPDATE users u SET u.name = 'John' WHERE u.id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	updateStmt, ok := stmt.(*statements.UpdateStatement)
	if !ok {
		t.Fatal("expected UpdateStatement")
	}

	if updateStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", updateStmt.TableName)
	}

	if updateStmt.Alias != "U" {
		t.Errorf("expected alias 'U', got %s", updateStmt.Alias)
	}

	if updateStmt.WhereClause == nil {
		t.Error("expected WHERE clause")
	}
}

func TestParseStatement_UpdateWithDifferentDataTypes(t *testing.T) {
	stmt, err := ParseStatement("UPDATE users SET name = 'John', age = 25")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	updateStmt, ok := stmt.(*statements.UpdateStatement)
	if !ok {
		t.Fatal("expected UpdateStatement")
	}

	if len(updateStmt.SetClauses) != 2 {
		t.Errorf("expected 2 set clauses, got %d", len(updateStmt.SetClauses))
	}

	// Check string value
	if updateStmt.SetClauses[0].Value.Type() != types.StringType {
		t.Errorf("expected string type for name, got %d", updateStmt.SetClauses[0].Value.Type())
	}

	// Check integer value
	if updateStmt.SetClauses[1].Value.Type() != types.IntType {
		t.Errorf("expected integer type for age, got %d", updateStmt.SetClauses[1].Value.Type())
	}
}

// UPDATE statement error handling tests

func TestParseUpdateStatement_MissingTableName(t *testing.T) {
	lexer := NewLexer("SET name = 'John'")

	_, err := (&UpdateParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected UPDATE, got SET" {
		t.Errorf("expected 'expected UPDATE, got SET', got %s", err.Error())
	}
}

func TestParseUpdateStatement_MissingSetKeyword(t *testing.T) {
	lexer := NewLexer("users name = 'John'")

	_, err := (&UpdateParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing SET keyword")
	}

	if err.Error() != "expected UPDATE, got USERS" {
		t.Errorf("expected 'expected UPDATE, got USERS', got %s", err.Error())
	}
}

func TestParseUpdateStatement_MissingEqualSign(t *testing.T) {
	lexer := NewLexer("users SET name 'John'")

	_, err := (&UpdateParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing equals sign")
	}

	if err.Error() != "expected UPDATE, got USERS" {
		t.Errorf("expected 'expected UPDATE, got USERS', got %s", err.Error())
	}
}

func TestParseUpdateStatement_MissingValue(t *testing.T) {
	lexer := NewLexer("users SET name =")

	_, err := (&UpdateParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing value")
	}

	if err.Error() != "expected UPDATE, got USERS" {
		t.Errorf("expected 'expected UPDATE, got USERS', got %s", err.Error())
	}
}

func TestParseUpdateStatement_InvalidTableName(t *testing.T) {
	lexer := NewLexer("123 SET name = 'John'")

	_, err := (&UpdateParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for invalid table name")
	}

	if err.Error() != "expected UPDATE, got 123" {
		t.Errorf("expected 'expected UPDATE, got 123', got %s", err.Error())
	}
}

// Test parseSetClauses function specifically
func TestParseSetClauses_SingleClause(t *testing.T) {
	lexer := NewLexer("name = 'John'")
	stmt := statements.NewUpdateStatement("USERS", "USERS")

	err := (&UpdateParser{}).parseSetClauses(lexer, stmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(stmt.SetClauses) != 1 {
		t.Errorf("expected 1 set clause, got %d", len(stmt.SetClauses))
	}

	if stmt.SetClauses[0].FieldName != "NAME" {
		t.Errorf("expected field name 'NAME', got %s", stmt.SetClauses[0].FieldName)
	}
}

func TestParseSetClauses_MultipleClauses(t *testing.T) {
	lexer := NewLexer("name = 'John', age = 25")
	stmt := statements.NewUpdateStatement("USERS", "USERS")

	err := (&UpdateParser{}).parseSetClauses(lexer, stmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(stmt.SetClauses) != 2 {
		t.Errorf("expected 2 set clauses, got %d", len(stmt.SetClauses))
	}

	if stmt.SetClauses[0].FieldName != "NAME" {
		t.Errorf("expected first field name 'NAME', got %s", stmt.SetClauses[0].FieldName)
	}

	if stmt.SetClauses[1].FieldName != "AGE" {
		t.Errorf("expected second field name 'AGE', got %s", stmt.SetClauses[1].FieldName)
	}
}

// Test parseTableWithAlias function specifically
func TestParseTableWithAlias_NoAlias(t *testing.T) {
	lexer := NewLexer("users SET")

	tableName, alias, err := parseTableWithAlias(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if tableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", tableName)
	}

	if alias != "USERS" {
		t.Errorf("expected alias 'USERS', got %s", alias)
	}
}

func TestParseTableWithAlias_WithAlias(t *testing.T) {
	lexer := NewLexer("users u SET")

	tableName, alias, err := parseTableWithAlias(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if tableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", tableName)
	}

	if alias != "U" {
		t.Errorf("expected alias 'U', got %s", alias)
	}
}

func TestParseOptionalWhereClause_NoWhere(t *testing.T) {
	lexer := NewLexer("EOF")
	stmt := statements.NewUpdateStatement("USERS", "USERS")

	err := (&UpdateParser{}).parseOptionalWhereClause(lexer, stmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if stmt.WhereClause != nil {
		t.Error("expected no WHERE clause")
	}
}

func TestParseOptionalWhereClause_WithWhere(t *testing.T) {
	lexer := NewLexer("WHERE id = 1")
	stmt := statements.NewUpdateStatement("USERS", "USERS")

	err := (&UpdateParser{}).parseOptionalWhereClause(lexer, stmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if stmt.WhereClause == nil {
		t.Error("expected WHERE clause")
	}
}

package parser

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"testing"
)


// INSERT statement tests

func TestParseStatement_BasicInsert(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("INSERT INTO users VALUES ('john', 25)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	insertStmt, ok := stmt.(*statements.InsertStatement)
	if !ok {
		t.Fatal("expected InsertStatement")
	}

	if insertStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", insertStmt.TableName)
	}

	if len(insertStmt.Values) != 1 {
		t.Errorf("expected 1 value row, got %d", len(insertStmt.Values))
	}

	if len(insertStmt.Values[0]) != 2 {
		t.Errorf("expected 2 values, got %d", len(insertStmt.Values[0]))
	}
}

func TestParseStatement_InsertWithFields(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("INSERT INTO users (name, age) VALUES ('john', 25)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	insertStmt, ok := stmt.(*statements.InsertStatement)
	if !ok {
		t.Fatal("expected InsertStatement")
	}

	if insertStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", insertStmt.TableName)
	}

	expectedFields := []string{"NAME", "AGE"}
	if len(insertStmt.Fields) != len(expectedFields) {
		t.Fatalf("expected %d fields, got %d", len(expectedFields), len(insertStmt.Fields))
	}

	for i, field := range expectedFields {
		if insertStmt.Fields[i] != field {
			t.Errorf("expected field %s, got %s", field, insertStmt.Fields[i])
		}
	}
}

func TestParseStatement_InsertMultipleValues(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("INSERT INTO users VALUES ('john', 25), ('jane', 30)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	insertStmt, ok := stmt.(*statements.InsertStatement)
	if !ok {
		t.Fatal("expected InsertStatement")
	}

	if len(insertStmt.Values) != 2 {
		t.Errorf("expected 2 value rows, got %d", len(insertStmt.Values))
	}

	if len(insertStmt.Values[0]) != 2 {
		t.Errorf("expected 2 values in first row, got %d", len(insertStmt.Values[0]))
	}

	if len(insertStmt.Values[1]) != 2 {
		t.Errorf("expected 2 values in second row, got %d", len(insertStmt.Values[1]))
	}
}

func TestParseValue_String(t *testing.T) {
	lexer := NewLexer("'hello world'")

	field, err := parseValue(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	stringField, ok := field.(*types.StringField)
	if !ok {
		t.Fatal("expected StringField")
	}

	if stringField.Value != "HELLO WORLD" {
		t.Errorf("expected 'HELLO WORLD', got %s", stringField.Value)
	}
}

func TestParseValue_Integer(t *testing.T) {
	lexer := NewLexer("42")

	field, err := parseValue(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	intField, ok := field.(*types.IntField)
	if !ok {
		t.Fatal("expected IntField")
	}

	if intField.Value != 42 {
		t.Errorf("expected 42, got %d", intField.Value)
	}
}

func TestParseValue_Null(t *testing.T) {
	lexer := NewLexer("NULL")

	field, err := parseValue(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if field != nil {
		t.Error("expected nil for NULL value")
	}
}

func TestParseValue_InvalidInteger(t *testing.T) {
	lexer := NewLexer("abc")
	lexer.NextToken() // Skip to INT token that would be invalid

	_, err := parseValue(lexer)
	if err == nil {
		t.Error("expected error for invalid value")
	}
}

func TestParseFieldList_SingleField(t *testing.T) {
	lexer := NewLexer("name)")

	fields, err := parseFieldList(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	if len(fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(fields))
	}

	if fields[0] != "NAME" {
		t.Errorf("expected 'NAME', got %s", fields[0])
	}
}

func TestParseFieldList_MultipleFields(t *testing.T) {
	lexer := NewLexer("name, age, email)")

	fields, err := parseFieldList(lexer)
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	expectedFields := []string{"NAME", "AGE", "EMAIL"}
	if len(fields) != len(expectedFields) {
		t.Fatalf("expected %d fields, got %d", len(expectedFields), len(fields))
	}

	for i, expected := range expectedFields {
		if fields[i] != expected {
			t.Errorf("expected field %s, got %s", expected, fields[i])
		}
	}
}

func TestParseInsertStatement_MissingInto(t *testing.T) {
	lexer := NewLexer("users VALUES ('john')")

	_, err := parseInsertStatement(lexer)
	if err == nil {
		t.Error("expected error for missing INTO")
	}

	if err.Error() != "expected INSERT, got USERS" {
		t.Errorf("expected 'expected INSERT, got USERS', got %s", err.Error())
	}
}

func TestParseInsertStatement_MissingTableName(t *testing.T) {
	lexer := NewLexer("INTO VALUES ('john')")

	_, err := parseInsertStatement(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected INSERT, got INTO" {
		t.Errorf("expected 'expected INSERT, got INTO', got %s", err.Error())
	}
}

func TestParseInsertStatement_MissingValues(t *testing.T) {
	lexer := NewLexer("INTO users SELECT")

	_, err := parseInsertStatement(lexer)
	if err == nil {
		t.Error("expected error for missing VALUES")
	}

	if err.Error() != "expected INSERT, got INTO" {
		t.Errorf("expected 'expected INSERT, got INTO', got %s", err.Error())
	}
}

func TestParseValueList_MissingCommaOrParen(t *testing.T) {
	lexer := NewLexer("'john' 'doe'")

	_, err := parseValueList(lexer)
	if err == nil {
		t.Error("expected error for missing comma or parenthesis")
	}
}

func TestParseInsertValues_MissingLeftParen(t *testing.T) {
	lexer := NewLexer("'john', 25)")
	stmt := statements.NewInsertStatement("users")

	_, err := parseInsertValues(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected LPAREN, got JOHN" {
		t.Errorf("expected 'expected LPAREN, got JOHN', got %s", err.Error())
	}
}
package parser

import (
	"storemy/pkg/parser/lexer"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"testing"
)


// CREATE TABLE statement tests

func TestParseStatement_BasicCreateTable(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("CREATE TABLE users (id INT, name VARCHAR)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if createStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", createStmt.TableName)
	}

	if len(createStmt.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(createStmt.Fields))
	}

	if createStmt.Fields[0].Name != "ID" {
		t.Errorf("expected first field name 'ID', got %s", createStmt.Fields[0].Name)
	}

	if createStmt.Fields[0].Type != types.IntType {
		t.Errorf("expected first field type IntType, got %v", createStmt.Fields[0].Type)
	}

	if createStmt.Fields[1].Name != "NAME" {
		t.Errorf("expected second field name 'NAME', got %s", createStmt.Fields[1].Name)
	}

	if createStmt.Fields[1].Type != types.StringType {
		t.Errorf("expected second field type StringType, got %v", createStmt.Fields[1].Type)
	}
}

func TestParseStatement_CreateTableIfNotExists(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("CREATE TABLE IF NOT EXISTS users (id INT)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if !createStmt.IfNotExists {
		t.Error("expected IfNotExists to be true")
	}

	if createStmt.TableName != "USERS" {
		t.Errorf("expected table name 'USERS', got %s", createStmt.TableName)
	}
}

func TestParseStatement_CreateTableWithNotNull(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("CREATE TABLE users (id INT NOT NULL, name VARCHAR)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if !createStmt.Fields[0].NotNull {
		t.Error("expected first field to be NOT NULL")
	}

	if createStmt.Fields[1].NotNull {
		t.Error("expected second field to not be NOT NULL")
	}
}

func TestParseStatement_CreateTableWithDefault(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("CREATE TABLE users (id INT DEFAULT 0, name VARCHAR DEFAULT 'unknown')")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if createStmt.Fields[0].DefaultValue == nil {
		t.Error("expected first field to have default value")
	}

	intField, ok := createStmt.Fields[0].DefaultValue.(*types.IntField)
	if !ok {
		t.Fatal("expected default value to be IntField")
	}

	if intField.Value != 0 {
		t.Errorf("expected default value 0, got %d", intField.Value)
	}

	stringField, ok := createStmt.Fields[1].DefaultValue.(*types.StringField)
	if !ok {
		t.Fatal("expected default value to be StringField")
	}

	if stringField.Value != "UNKNOWN" {
		t.Errorf("expected default value 'UNKNOWN', got %s", stringField.Value)
	}
}

func TestParseStatement_CreateTableWithPrimaryKey(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("CREATE TABLE users (id INT, name VARCHAR, PRIMARY KEY (id))")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if createStmt.PrimaryKey != "ID" {
		t.Errorf("expected primary key 'ID', got %s", createStmt.PrimaryKey)
	}
}

func TestParseStatement_CreateTableAllDataTypes(t *testing.T) {
	parser := &Parser{}

	stmt, err := parser.ParseStatement("CREATE TABLE test (id INT, name TEXT, active BOOLEAN, price FLOAT)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	expectedTypes := []types.Type{types.IntType, types.StringType, types.BoolType, types.FloatType}

	for i, expected := range expectedTypes {
		if createStmt.Fields[i].Type != expected {
			t.Errorf("expected field %d type %v, got %v", i, expected, createStmt.Fields[i].Type)
		}
	}
}

func TestParseDataType_ValidTypes(t *testing.T) {
	tests := []struct {
		tokenType lexer.TokenType
		expected  types.Type
	}{
		{lexer.INT, types.IntType},
		{lexer.VARCHAR, types.StringType},
		{lexer.TEXT, types.StringType},
		{lexer.BOOLEAN, types.BoolType},
		{lexer.FLOAT, types.FloatType},
	}

	for _, test := range tests {
		token := lexer.Token{Type: test.tokenType, Value: "test"}
		result, err := parseDataType(token)
		if err != nil {
			t.Errorf("unexpected error for %v: %s", test.tokenType, err.Error())
		}

		if result != test.expected {
			t.Errorf("expected %v, got %v", test.expected, result)
		}
	}
}

func TestParseDataType_InvalidType(t *testing.T) {
	token := lexer.Token{Type: lexer.IDENTIFIER, Value: "INVALID"}
	_, err := parseDataType(token)
	if err == nil {
		t.Error("expected error for invalid data type")
	}

	if err.Error() != "unknown data type: INVALID" {
		t.Errorf("expected 'unknown data type: INVALID', got %s", err.Error())
	}
}

// CREATE TABLE error handling tests

func TestParseCreateStatement_MissingTable(t *testing.T) {
	lexer := NewLexer("users (id INT)")

	_, err := parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for missing TABLE")
	}

	if err.Error() != "expected CREATE, got USERS" {
		t.Errorf("expected 'expected CREATE, got USERS', got %s", err.Error())
	}
}

func TestParseCreateStatement_MissingTableName(t *testing.T) {
	lexer := NewLexer("TABLE (id INT)")

	_, err := parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestParseCreateStatement_MissingLeftParen(t *testing.T) {
	lexer := NewLexer("TABLE users id INT)")

	_, err := parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestParseCreateStatement_InvalidIfNotExists(t *testing.T) {
	lexer := NewLexer("TABLE IF EXISTS users (id INT)")

	_, err := parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for invalid IF NOT EXISTS")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingKey(t *testing.T) {
	lexer := NewLexer("(id)")
	stmt := statements.NewCreateStatement("users", false)

	err := readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing KEY")
	}

	if err.Error() != "expected KEY, got (" {
		t.Errorf("expected 'expected KEY, got (', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingLeftParen(t *testing.T) {
	lexer := NewLexer("KEY id)")
	stmt := statements.NewCreateStatement("users", false)

	err := readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected LPAREN, got ID" {
		t.Errorf("expected 'expected LPAREN, got ID', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingFieldName(t *testing.T) {
	lexer := NewLexer("KEY ()")
	stmt := statements.NewCreateStatement("users", false)

	err := readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing field name")
	}

	if err.Error() != "expected IDENTIFIER, got )" {
		t.Errorf("expected 'expected IDENTIFIER, got )', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingRightParen(t *testing.T) {
	lexer := NewLexer("KEY (id")
	stmt := statements.NewCreateStatement("users", false)

	err := readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing right parenthesis")
	}
}
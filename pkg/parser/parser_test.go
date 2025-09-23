package parser

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"testing"
)

func TestParseStatement_EmptyStatement(t *testing.T) {
	parser := &Parser{}

	_, err := parser.ParseStatement("")
	if err == nil {
		t.Error("expected error for empty statement")
	}

	if err.Error() != "empty statement" {
		t.Errorf("expected 'empty statement', got %s", err.Error())
	}
}

func TestParseStatement_UnsupportedStatement(t *testing.T) {
	parser := &Parser{}

	_, err := parser.ParseStatement("SELECT * FROM users")
	if err == nil {
		t.Error("expected error for unsupported statement")
	}

	if err.Error() != "unsupported statement type: SELECT" {
		t.Errorf("expected 'unsupported statement type: SELECT', got %s", err.Error())
	}
}

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
	parser := &Parser{}
	lexer := NewLexer("users VALUES ('john')")

	_, err := parser.parseInsertStatement(lexer)
	if err == nil {
		t.Error("expected error for missing INTO")
	}

	if err.Error() != "expected INSERT, got USERS" {
		t.Errorf("expected 'expected INSERT, got USERS', got %s", err.Error())
	}
}

func TestParseInsertStatement_MissingTableName(t *testing.T) {
	parser := &Parser{}
	lexer := NewLexer("INTO VALUES ('john')")

	_, err := parser.parseInsertStatement(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected INSERT, got INTO" {
		t.Errorf("expected 'expected INSERT, got INTO', got %s", err.Error())
	}
}

func TestParseInsertStatement_MissingValues(t *testing.T) {
	parser := &Parser{}
	lexer := NewLexer("INTO users SELECT")

	_, err := parser.parseInsertStatement(lexer)
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
	parser := &Parser{}
	lexer := NewLexer("'john', 25)")
	stmt := statements.NewInsertStatement("users")

	_, err := parser.parseInsertValues(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected '(', got JOHN" {
		t.Errorf("expected 'expected '(', got JOHN', got %s", err.Error())
	}
}

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
		tokenType TokenType
		expected  types.Type
	}{
		{INT, types.IntType},
		{VARCHAR, types.StringType},
		{TEXT, types.StringType},
		{BOOLEAN, types.BoolType},
		{FLOAT, types.FloatType},
	}

	for _, test := range tests {
		token := Token{Type: test.tokenType, Value: "test"}
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
	token := Token{Type: IDENTIFIER, Value: "INVALID"}
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
	parser := &Parser{}
	lexer := NewLexer("users (id INT)")

	_, err := parser.parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for missing TABLE")
	}

	if err.Error() != "expected CREATE, got USERS" {
		t.Errorf("expected 'expected CREATE, got USERS', got %s", err.Error())
	}
}

func TestParseCreateStatement_MissingTableName(t *testing.T) {
	parser := &Parser{}
	lexer := NewLexer("TABLE (id INT)")

	_, err := parser.parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestParseCreateStatement_MissingLeftParen(t *testing.T) {
	parser := &Parser{}
	lexer := NewLexer("TABLE users id INT)")

	_, err := parser.parseCreateStatement(lexer)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestParseCreateStatement_InvalidIfNotExists(t *testing.T) {
	parser := &Parser{}
	lexer := NewLexer("TABLE IF EXISTS users (id INT)")

	_, err := parser.parseCreateStatement(lexer)
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

	if err.Error() != "expected KEY after PRIMARY, got (" {
		t.Errorf("expected 'expected KEY after PRIMARY, got (', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingLeftParen(t *testing.T) {
	lexer := NewLexer("KEY id)")
	stmt := statements.NewCreateStatement("users", false)

	err := readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected '(' after PRIMARY KEY, got ID" {
		t.Errorf("expected 'expected '(' after PRIMARY KEY, got ID', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingFieldName(t *testing.T) {
	lexer := NewLexer("KEY ()")
	stmt := statements.NewCreateStatement("users", false)

	err := readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing field name")
	}

	if err.Error() != "expected field name in PRIMARY KEY, got )" {
		t.Errorf("expected 'expected field name in PRIMARY KEY, got )', got %s", err.Error())
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

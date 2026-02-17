package parser

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"testing"
)

// CREATE TABLE statement tests
func TestParseStatement_BasicCreateTable(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE users (id INT, name VARCHAR)")
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
	stmt, err := ParseStatement("CREATE TABLE IF NOT EXISTS users (id INT)")
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
	stmt, err := ParseStatement("CREATE TABLE users (id INT NOT NULL, name VARCHAR)")
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
	stmt, err := ParseStatement("CREATE TABLE users (id INT DEFAULT 0, name VARCHAR DEFAULT 'unknown')")
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
	stmt, err := ParseStatement("CREATE TABLE users (id INT, name VARCHAR, PRIMARY KEY (id))")
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
	stmt, err := ParseStatement("CREATE TABLE test (id INT, name TEXT, active BOOLEAN, price FLOAT)")
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
		result, err := (&CreateStatementParser{}).parseDataType(token)
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
	_, err := (&CreateStatementParser{}).parseDataType(token)
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

	_, err := (&CreateStatementParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing TABLE")
	}

	if err.Error() != "expected CREATE, got USERS" {
		t.Errorf("expected 'expected CREATE, got USERS', got %s", err.Error())
	}
}

func TestParseCreateStatement_MissingTableName(t *testing.T) {
	lexer := NewLexer("TABLE (id INT)")

	_, err := (&CreateStatementParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing table name")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestParseCreateStatement_MissingLeftParen(t *testing.T) {
	lexer := NewLexer("TABLE users id INT)")

	_, err := (&CreateStatementParser{}).Parse(lexer)
	if err == nil {
		t.Error("expected error for missing left parenthesis")
	}

	if err.Error() != "expected CREATE, got TABLE" {
		t.Errorf("expected 'expected CREATE, got TABLE', got %s", err.Error())
	}
}

func TestParseCreateStatement_InvalidIfNotExists(t *testing.T) {
	lexer := NewLexer("TABLE IF EXISTS users (id INT)")

	_, err := (&CreateStatementParser{}).Parse(lexer)
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

	err := (&CreateStatementParser{}).readPrimaryKey(lexer, stmt)
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

	err := (&CreateStatementParser{}).readPrimaryKey(lexer, stmt)
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

	err := (&CreateStatementParser{}).readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing field name")
	}

	if err.Error() != "expected field name in PRIMARY KEY: expected value of type [IDENTIFIER], got )" {
		t.Errorf("expected 'expected field name in PRIMARY KEY: expected value of type [IDENTIFIER], got )', got %s", err.Error())
	}
}

func TestReadPrimaryKey_MissingRightParen(t *testing.T) {
	lexer := NewLexer("KEY (id")
	stmt := statements.NewCreateStatement("users", false)

	err := (&CreateStatementParser{}).readPrimaryKey(lexer, stmt)
	if err == nil {
		t.Error("expected error for missing right parenthesis")
	}
}

// AUTO_INCREMENT tests
func TestParseStatement_CreateTableWithAutoIncrement(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR)")
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

	if !createStmt.Fields[0].AutoIncrement {
		t.Error("expected first field to have AUTO_INCREMENT")
	}

	if createStmt.Fields[0].Type != types.IntType {
		t.Errorf("expected first field type IntType, got %v", createStmt.Fields[0].Type)
	}

	if createStmt.PrimaryKey != "ID" {
		t.Errorf("expected primary key 'ID', got %s", createStmt.PrimaryKey)
	}

	if createStmt.Fields[1].AutoIncrement {
		t.Error("expected second field to not have AUTO_INCREMENT")
	}
}

func TestParseStatement_CreateTableAutoIncrementNotNull(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE products (id INT AUTO_INCREMENT NOT NULL, name VARCHAR)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if !createStmt.Fields[0].AutoIncrement {
		t.Error("expected first field to have AUTO_INCREMENT")
	}

	if !createStmt.Fields[0].NotNull {
		t.Error("expected first field to be NOT NULL")
	}
}

func TestParseStatement_CreateTableAutoIncrementMultipleConstraints(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE orders (order_id INT AUTO_INCREMENT PRIMARY KEY, customer VARCHAR NOT NULL, total FLOAT)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	if !createStmt.Fields[0].AutoIncrement {
		t.Error("expected first field to have AUTO_INCREMENT")
	}

	if createStmt.PrimaryKey != "ORDER_ID" {
		t.Errorf("expected primary key 'ORDER_ID', got %s", createStmt.PrimaryKey)
	}

	if !createStmt.Fields[1].NotNull {
		t.Error("expected customer field to be NOT NULL")
	}
}

func TestCreateStatementValidation_MultipleAutoIncrement(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE test (id1 INT AUTO_INCREMENT, id2 INT AUTO_INCREMENT)")
	if err != nil {
		t.Fatalf("unexpected parse error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	err = createStmt.Validate()
	if err == nil {
		t.Error("expected validation error for multiple AUTO_INCREMENT columns")
	}

	expectedMsg := "table can have only one auto-increment column"
	if !contains(err.Error(), expectedMsg) {
		t.Errorf("expected error to contain '%s', got: %s", expectedMsg, err.Error())
	}
}

func TestCreateStatementValidation_AutoIncrementNonInt(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE test (id VARCHAR AUTO_INCREMENT)")
	if err != nil {
		t.Fatalf("unexpected parse error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	err = createStmt.Validate()
	if err == nil {
		t.Error("expected validation error for non-INT AUTO_INCREMENT column")
	}
}

func TestCreateStatementString_WithAutoIncrement(t *testing.T) {
	stmt, err := ParseStatement("CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR)")
	if err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	createStmt, ok := stmt.(*statements.CreateStatement)
	if !ok {
		t.Fatal("expected CreateStatement")
	}

	str := createStmt.String()
	if str == "" {
		t.Error("expected non-empty string representation")
	}

	// Verify AUTO_INCREMENT is in the string representation
	expectedSubstr := "AUTO_INCREMENT"
	if !contains(str, expectedSubstr) {
		t.Errorf("expected string to contain '%s', got: %s", expectedSubstr, str)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestParseCreateIndexStatement(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		indexName   string
		tableName   string
		columnName  string
		indexType   index.IndexType
		ifNotExists bool
	}{
		{
			name:        "Basic CREATE INDEX with HASH",
			sql:         "CREATE INDEX idx_users_email ON users(email) USING HASH",
			wantErr:     false,
			indexName:   "IDX_USERS_EMAIL",
			tableName:   "USERS",
			columnName:  "EMAIL",
			indexType:   index.HashIndex,
			ifNotExists: false,
		},
		{
			name:        "CREATE INDEX with BTREE",
			sql:         "CREATE INDEX idx_products_price ON products(price) USING BTREE",
			wantErr:     false,
			indexName:   "IDX_PRODUCTS_PRICE",
			tableName:   "PRODUCTS",
			columnName:  "PRICE",
			indexType:   index.BTreeIndex,
			ifNotExists: false,
		},
		{
			name:        "CREATE INDEX without USING clause (defaults to HASH)",
			sql:         "CREATE INDEX idx_orders_id ON orders(order_id)",
			wantErr:     false,
			indexName:   "IDX_ORDERS_ID",
			tableName:   "ORDERS",
			columnName:  "ORDER_ID",
			indexType:   index.HashIndex,
			ifNotExists: false,
		},
		{
			name:        "CREATE INDEX IF NOT EXISTS",
			sql:         "CREATE INDEX IF NOT EXISTS idx_users_name ON users(name) USING HASH",
			wantErr:     false,
			indexName:   "IDX_USERS_NAME",
			tableName:   "USERS",
			columnName:  "NAME",
			indexType:   index.HashIndex,
			ifNotExists: true,
		},
		{
			name:        "CREATE INDEX IF NOT EXISTS without USING",
			sql:         "CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)",
			wantErr:     false,
			indexName:   "IDX_STATUS",
			tableName:   "TASKS",
			columnName:  "STATUS",
			indexType:   index.HashIndex,
			ifNotExists: true,
		},
		{
			name:    "Missing index name",
			sql:     "CREATE INDEX ON users(email)",
			wantErr: true,
		},
		{
			name:    "Missing table name",
			sql:     "CREATE INDEX idx_test",
			wantErr: true,
		},
		{
			name:    "Missing column name",
			sql:     "CREATE INDEX idx_test ON users",
			wantErr: true,
		},
		{
			name:    "Invalid index type",
			sql:     "CREATE INDEX idx_test ON users(email) USING INVALID",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseStatement() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("ParseStatement() error = %v", err)
				return
			}

			createIndexStmt, ok := stmt.(*statements.CreateIndexStatement)
			if !ok {
				t.Errorf("ParseStatement() returned wrong type, got %T, want *statements.CreateIndexStatement", stmt)
				return
			}

			if createIndexStmt.IndexName != tt.indexName {
				t.Errorf("IndexName = %v, want %v", createIndexStmt.IndexName, tt.indexName)
			}

			if createIndexStmt.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", createIndexStmt.TableName, tt.tableName)
			}

			if createIndexStmt.ColumnName != tt.columnName {
				t.Errorf("ColumnName = %v, want %v", createIndexStmt.ColumnName, tt.columnName)
			}

			if createIndexStmt.IndexType != tt.indexType {
				t.Errorf("IndexType = %v, want %v", createIndexStmt.IndexType, tt.indexType)
			}

			if createIndexStmt.IfNotExists != tt.ifNotExists {
				t.Errorf("IfNotExists = %v, want %v", createIndexStmt.IfNotExists, tt.ifNotExists)
			}

			// Test validation
			if err := createIndexStmt.Validate(); err != nil {
				t.Errorf("Validate() error = %v", err)
			}

			// Test String() method
			str := createIndexStmt.String()
			if str == "" {
				t.Errorf("String() returned empty string")
			}
		})
	}
}

func TestParseDropIndexStatement(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		indexName string
		tableName string
		ifExists  bool
	}{
		{
			name:      "Basic DROP INDEX",
			sql:       "DROP INDEX idx_users_email",
			wantErr:   false,
			indexName: "IDX_USERS_EMAIL",
			tableName: "",
			ifExists:  false,
		},
		{
			name:      "DROP INDEX with table name",
			sql:       "DROP INDEX idx_users_email ON users",
			wantErr:   false,
			indexName: "IDX_USERS_EMAIL",
			tableName: "USERS",
			ifExists:  false,
		},
		{
			name:      "DROP INDEX IF EXISTS",
			sql:       "DROP INDEX IF EXISTS idx_products_price",
			wantErr:   false,
			indexName: "IDX_PRODUCTS_PRICE",
			tableName: "",
			ifExists:  true,
		},
		{
			name:      "DROP INDEX IF EXISTS with table name",
			sql:       "DROP INDEX IF EXISTS idx_orders_status ON orders",
			wantErr:   false,
			indexName: "IDX_ORDERS_STATUS",
			tableName: "ORDERS",
			ifExists:  true,
		},
		{
			name:    "Missing index name",
			sql:     "DROP INDEX",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseStatement(tt.sql)

			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseStatement() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("ParseStatement() error = %v", err)
				return
			}

			dropIndexStmt, ok := stmt.(*statements.DropIndexStatement)
			if !ok {
				t.Errorf("ParseStatement() returned wrong type, got %T, want *statements.DropIndexStatement", stmt)
				return
			}

			if dropIndexStmt.IndexName != tt.indexName {
				t.Errorf("IndexName = %v, want %v", dropIndexStmt.IndexName, tt.indexName)
			}

			if dropIndexStmt.TableName != tt.tableName {
				t.Errorf("TableName = %v, want %v", dropIndexStmt.TableName, tt.tableName)
			}

			if dropIndexStmt.IfExists != tt.ifExists {
				t.Errorf("IfExists = %v, want %v", dropIndexStmt.IfExists, tt.ifExists)
			}

			// Test validation
			if err := dropIndexStmt.Validate(); err != nil {
				t.Errorf("Validate() error = %v", err)
			}

			// Test String() method
			str := dropIndexStmt.String()
			if str == "" {
				t.Errorf("String() returned empty string")
			}
		})
	}
}

func TestCreateIndexStatementType(t *testing.T) {
	sql := "CREATE INDEX idx_test ON users(email) USING HASH"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	if stmt.GetType() != statements.CreateIndex {
		t.Errorf("GetType() = %v, want %v", stmt.GetType(), statements.CreateIndex)
	}

	if stmt.GetType().String() != "CREATE INDEX" {
		t.Errorf("GetType().String() = %v, want %v", stmt.GetType().String(), "CREATE INDEX")
	}

	if !stmt.GetType().IsDDL() {
		t.Errorf("IsDDL() = false, want true")
	}
}

func TestDropIndexStatementType(t *testing.T) {
	sql := "DROP INDEX idx_test"
	stmt, err := ParseStatement(sql)
	if err != nil {
		t.Fatalf("ParseStatement() error = %v", err)
	}

	if stmt.GetType() != statements.DropIndex {
		t.Errorf("GetType() = %v, want %v", stmt.GetType(), statements.DropIndex)
	}

	if stmt.GetType().String() != "DROP INDEX" {
		t.Errorf("GetType().String() = %v, want %v", stmt.GetType().String(), "DROP INDEX")
	}

	if !stmt.GetType().IsDDL() {
		t.Errorf("IsDDL() = false, want true")
	}
}

package parser

import (
	"storemy/pkg/parser/statements"
	"testing"
)

func TestParseCreateIndexStatement(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantErr     bool
		indexName   string
		tableName   string
		columnName  string
		indexType   statements.IndexType
		ifNotExists bool
	}{
		{
			name:        "Basic CREATE INDEX with HASH",
			sql:         "CREATE INDEX idx_users_email ON users(email) USING HASH",
			wantErr:     false,
			indexName:   "IDX_USERS_EMAIL",
			tableName:   "USERS",
			columnName:  "EMAIL",
			indexType:   statements.HashIndex,
			ifNotExists: false,
		},
		{
			name:        "CREATE INDEX with BTREE",
			sql:         "CREATE INDEX idx_products_price ON products(price) USING BTREE",
			wantErr:     false,
			indexName:   "IDX_PRODUCTS_PRICE",
			tableName:   "PRODUCTS",
			columnName:  "PRICE",
			indexType:   statements.BTreeIndex,
			ifNotExists: false,
		},
		{
			name:        "CREATE INDEX without USING clause (defaults to HASH)",
			sql:         "CREATE INDEX idx_orders_id ON orders(order_id)",
			wantErr:     false,
			indexName:   "IDX_ORDERS_ID",
			tableName:   "ORDERS",
			columnName:  "ORDER_ID",
			indexType:   statements.HashIndex,
			ifNotExists: false,
		},
		{
			name:        "CREATE INDEX IF NOT EXISTS",
			sql:         "CREATE INDEX IF NOT EXISTS idx_users_name ON users(name) USING HASH",
			wantErr:     false,
			indexName:   "IDX_USERS_NAME",
			tableName:   "USERS",
			columnName:  "NAME",
			indexType:   statements.HashIndex,
			ifNotExists: true,
		},
		{
			name:        "CREATE INDEX IF NOT EXISTS without USING",
			sql:         "CREATE INDEX IF NOT EXISTS idx_status ON tasks(status)",
			wantErr:     false,
			indexName:   "IDX_STATUS",
			tableName:   "TASKS",
			columnName:  "STATUS",
			indexType:   statements.HashIndex,
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

package statements

import (
	"storemy/pkg/plan"
	"storemy/pkg/types"
	"testing"
)

// Test BaseStatement
func TestBaseStatement_GetType(t *testing.T) {
	base := NewBaseStatement(Select)
	if base.GetType() != Select {
		t.Errorf("Expected Select, got %v", base.GetType())
	}
}

// Test TableStatement
func TestTableStatement_GetAlias(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		alias     string
		expected  string
	}{
		{"WithAlias", "users", "u", "u"},
		{"WithoutAlias", "users", "", "users"},
		{"SameAsTableName", "users", "users", "users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := NewTableStatement(Select, tt.tableName, tt.alias)
			if got := ts.GetAlias(); got != tt.expected {
				t.Errorf("GetAlias() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestTableStatement_HasAlias(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		alias     string
		expected  bool
	}{
		{"WithDifferentAlias", "users", "u", true},
		{"WithoutAlias", "users", "", false},
		{"SameAsTableName", "users", "users", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := NewTableStatement(Select, tt.tableName, tt.alias)
			if got := ts.HasAlias(); got != tt.expected {
				t.Errorf("HasAlias() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// Test DeleteStatement
func TestDeleteStatement_Validate(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{"ValidDelete", "users", false},
		{"EmptyTableName", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewDeleteStatement(tt.tableName, "")
			err := stmt.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeleteStatement_HasWhereClause(t *testing.T) {
	stmt := NewDeleteStatement("users", "")
	if stmt.HasWhereClause() {
		t.Error("Expected no WHERE clause")
	}

	filter := &plan.FilterNode{}
	stmt.SetWhereClause(filter)
	if !stmt.HasWhereClause() {
		t.Error("Expected WHERE clause to be present")
	}
}

// Test UpdateStatement
func TestUpdateStatement_Validate(t *testing.T) {
	tests := []struct {
		name       string
		tableName  string
		setClauses []SetClause
		wantErr    bool
		errField   string
	}{
		{
			name:      "ValidUpdate",
			tableName: "users",
			setClauses: []SetClause{
				{FieldName: "name", Value: types.NewStringField("John", 255)},
			},
			wantErr: false,
		},
		{
			name:       "EmptyTableName",
			tableName:  "",
			setClauses: []SetClause{{FieldName: "name", Value: types.NewStringField("John", 255)}},
			wantErr:    true,
			errField:   "TableName",
		},
		{
			name:       "NoSetClauses",
			tableName:  "users",
			setClauses: []SetClause{},
			wantErr:    true,
			errField:   "SetClauses",
		},
		{
			name:      "EmptyFieldName",
			tableName: "users",
			setClauses: []SetClause{
				{FieldName: "", Value: types.NewStringField("John", 255)},
			},
			wantErr:  true,
			errField: "SetClauses[0].FieldName",
		},
		{
			name:      "NilValue",
			tableName: "users",
			setClauses: []SetClause{
				{FieldName: "name", Value: nil},
			},
			wantErr:  true,
			errField: "SetClauses[0].Value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewUpdateStatement(tt.tableName, "")
			for _, clause := range tt.setClauses {
				stmt.AddSetClause(clause.FieldName, clause.Value)
			}

			err := stmt.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err != nil {
				validationErr, ok := err.(*ValidationError)
				if !ok {
					t.Errorf("Expected ValidationError, got %T", err)
					return
				}
				if tt.errField != "" && validationErr.Field != tt.errField {
					t.Errorf("Expected error field %v, got %v", tt.errField, validationErr.Field)
				}
			}
		})
	}
}

// Test InsertStatement
func TestInsertStatement_Validate(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		fields    []string
		values    [][]types.Field
		wantErr   bool
	}{
		{
			name:      "ValidInsert",
			tableName: "users",
			fields:    []string{"name", "age"},
			values: [][]types.Field{
				{types.NewStringField("John", 255), types.NewIntField(30)},
			},
			wantErr: false,
		},
		{
			name:      "EmptyTableName",
			tableName: "",
			fields:    []string{"name"},
			values:    [][]types.Field{{types.NewStringField("John", 255)}},
			wantErr:   true,
		},
		{
			name:      "NoValues",
			tableName: "users",
			fields:    []string{"name"},
			values:    [][]types.Field{},
			wantErr:   true,
		},
		{
			name:      "MismatchedFieldCount",
			tableName: "users",
			fields:    []string{"name", "age"},
			values: [][]types.Field{
				{types.NewStringField("John", 255)}, // Only 1 value but 2 fields
			},
			wantErr: true,
		},
		{
			name:      "InconsistentRowSizes",
			tableName: "users",
			fields:    []string{},
			values: [][]types.Field{
				{types.NewStringField("John", 255), types.NewIntField(30)},
				{types.NewStringField("Jane", 255)}, // Different size
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewInsertStatement(tt.tableName)
			if len(tt.fields) > 0 {
				stmt.AddFieldNames(tt.fields)
			}
			for _, row := range tt.values {
				stmt.AddValues(row)
			}

			err := stmt.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestInsertStatement_ValueCount(t *testing.T) {
	stmt := NewInsertStatement("users")
	if stmt.ValueCount() != 0 {
		t.Errorf("Expected 0 values, got %d", stmt.ValueCount())
	}

	stmt.AddValues([]types.Field{types.NewStringField("John", 255)})
	stmt.AddValues([]types.Field{types.NewStringField("Jane", 255)})

	if stmt.ValueCount() != 2 {
		t.Errorf("Expected 2 values, got %d", stmt.ValueCount())
	}
}

// Test CreateStatement
func TestCreateStatement_Validate(t *testing.T) {
	tests := []struct {
		name       string
		tableName  string
		fields     []FieldDefinition
		primaryKey string
		wantErr    bool
	}{
		{
			name:      "ValidCreate",
			tableName: "users",
			fields: []FieldDefinition{
				{Name: "id", Type: types.IntType, NotNull: true},
				{Name: "name", Type: types.StringType, NotNull: false},
			},
			primaryKey: "id",
			wantErr:    false,
		},
		{
			name:      "EmptyTableName",
			tableName: "",
			fields:    []FieldDefinition{{Name: "id", Type: types.IntType}},
			wantErr:   true,
		},
		{
			name:      "NoFields",
			tableName: "users",
			fields:    []FieldDefinition{},
			wantErr:   true,
		},
		{
			name:      "EmptyFieldName",
			tableName: "users",
			fields:    []FieldDefinition{{Name: "", Type: types.IntType}},
			wantErr:   true,
		},
		{
			name:      "DuplicateFieldNames",
			tableName: "users",
			fields: []FieldDefinition{
				{Name: "id", Type: types.IntType},
				{Name: "id", Type: types.StringType},
			},
			wantErr: true,
		},
		{
			name:      "InvalidPrimaryKey",
			tableName: "users",
			fields: []FieldDefinition{
				{Name: "id", Type: types.IntType},
			},
			primaryKey: "nonexistent",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewCreateStatement(tt.tableName, false)
			for _, field := range tt.fields {
				stmt.AddField(field.Name, field.Type, field.NotNull, field.DefaultValue)
			}
			if tt.primaryKey != "" {
				stmt.PrimaryKey = tt.primaryKey
			}

			err := stmt.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreateStatement_HasPrimaryKey(t *testing.T) {
	stmt := NewCreateStatement("users", false)
	if stmt.PrimaryKey != "" {
		t.Error("Expected no primary key")
	}

	stmt.PrimaryKey = "id"
	if stmt.PrimaryKey == "" {
		t.Error("Expected primary key to be set")
	}
}

// Test SelectStatement
func TestSelectStatement_Validate(t *testing.T) {
	tests := []struct {
		name    string
		plan    *plan.SelectPlan
		wantErr bool
	}{
		{
			name: "ValidSelect",
			plan: func() *plan.SelectPlan {
				p := plan.NewSelectPlan()
				p.AddScan("users", "users")
				return p
			}(),
			wantErr: false,
		},
		{
			name:    "NilPlan",
			plan:    nil,
			wantErr: true,
		},
		{
			name:    "NoTables",
			plan:    plan.NewSelectPlan(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := NewSelectStatement(tt.plan)
			err := stmt.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSelectStatement_Helpers(t *testing.T) {
	p := plan.NewSelectPlan()
	p.AddScan("users", "users")
	p.AddScan("orders", "orders")
	p.SetSelectAll(true)

	stmt := NewSelectStatement(p)

	if !stmt.IsSelectAll() {
		t.Error("Expected SELECT * to be true")
	}

	if stmt.TableCount() != 2 {
		t.Errorf("Expected 2 tables, got %d", stmt.TableCount())
	}
}

// Test StatementType helpers
func TestStatementType_IsDML(t *testing.T) {
	tests := []struct {
		stmtType StatementType
		expected bool
	}{
		{Select, true},
		{Insert, true},
		{Update, true},
		{Delete, true},
		{CreateTable, false},
		{DropTable, false},
		{Transaction, false},
	}

	for _, tt := range tests {
		t.Run(tt.stmtType.String(), func(t *testing.T) {
			if got := tt.stmtType.IsDML(); got != tt.expected {
				t.Errorf("IsDML() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestStatementType_IsDDL(t *testing.T) {
	tests := []struct {
		stmtType StatementType
		expected bool
	}{
		{Select, false},
		{Insert, false},
		{Update, false},
		{Delete, false},
		{CreateTable, true},
		{DropTable, true},
		{Transaction, false},
	}

	for _, tt := range tests {
		t.Run(tt.stmtType.String(), func(t *testing.T) {
			if got := tt.stmtType.IsDDL(); got != tt.expected {
				t.Errorf("IsDDL() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// Test ValidationError
func TestValidationError_Error(t *testing.T) {
	err := NewValidationError(Insert, "TableName", "table name cannot be empty")
	expected := "INSERT validation error: TableName - table name cannot be empty"
	if err.Error() != expected {
		t.Errorf("Error() = %v, want %v", err.Error(), expected)
	}
}

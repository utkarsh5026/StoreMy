package ddl

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/planner/internal/testutil"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"strings"
	"testing"
)

// Helper function to execute plan and cast result to DDLResult
func executePlan(t *testing.T, plan *CreateTablePlan) (*result.DDLResult, error) {
	res, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if res == nil {
		return nil, nil
	}

	if ddlResult, ok := res.(*result.DDLResult); ok {
		return ddlResult, nil
	}

	return nil, fmt.Errorf("unexpected result type: %T", res)
}

func TestNewCreateTablePlan(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	if plan == nil {
		t.Fatal("NewCreateTablePlan returned nil")
	}

	if plan.Statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.TxContext != transCtx {
		t.Error("TransactionID not properly assigned")
	}
}

func TestCreateTablePlan_Execute_BasicSuccess(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)
	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users created successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	if !ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table was not added to catalog")
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateTablePlan_Execute_WithPrimaryKey(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("products", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.PrimaryKey = "id"

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.CatalogManager().TableExists(transCtx, "products") {
		t.Error("Table was not added to catalog")
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "products", transCtx)
}

func TestCreateTablePlan_Execute_AllFieldTypes(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("test_types", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("active", types.BoolType, false, nil)
	stmt.AddField("price", types.FloatType, false, nil)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.CatalogManager().TableExists(transCtx, "test_types") {
		t.Error("Table was not added to catalog")
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "test_types", transCtx)
}

func TestCreateTablePlan_Execute_IfNotExists_TableDoesNotExist(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("users", true)
	stmt.AddField("id", types.IntType, true, nil)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users created successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	if !ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table was not added to catalog")
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateTablePlan_Execute_IfNotExists_TableExists(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	existingStmt := statements.NewCreateStatement("users", false)
	existingStmt.AddField("id", types.IntType, true, nil)
	existingPlan := NewCreateTablePlan(existingStmt, ctx, transCtx)
	_, err := existingPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create existing table: %v", err)
	}

	stmt := statements.NewCreateStatement("users", true)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users already exists (IF NOT EXISTS)"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateTablePlan_Execute_Error_TableAlreadyExists(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	existingStmt := statements.NewCreateStatement("users", false)
	existingStmt.AddField("id", types.IntType, true, nil)
	existingPlan := NewCreateTablePlan(existingStmt, ctx, transCtx)
	_, err := existingPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create existing table: %v", err)
	}

	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when table already exists")
	}

	expectedError := "table users already exists"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateTablePlan_Execute_Error_EmptyFields(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("empty_table", false)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := plan.Execute()

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when creating table with no fields")
	}
}

func TestCreateTablePlan_Execute_Error_InvalidFieldType(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("invalid_table", false)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := plan.Execute()

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when creating table with no fields")
	}
}

func TestCreateTablePlan_Execute_ComplexTable(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	defaultInt := &types.IntField{Value: 1}
	defaultString := types.NewStringField("default", types.StringMaxSize)
	defaultBool := &types.BoolField{Value: true}
	defaultFloat := &types.Float64Field{Value: 0.0}

	stmt := statements.NewCreateStatement("complex_table", false)
	stmt.AddField("id", types.IntType, true, defaultInt)
	stmt.AddField("name", types.StringType, true, defaultString)
	stmt.AddField("active", types.BoolType, false, defaultBool)
	stmt.AddField("price", types.FloatType, false, defaultFloat)
	stmt.PrimaryKey = "id"

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.CatalogManager().TableExists(transCtx, "complex_table") {
		t.Error("Table was not added to catalog")
	}

	tableID, err := ctx.CatalogManager().GetTableID(transCtx, "complex_table")
	if err != nil {
		t.Fatalf("Failed to get table ID: %v", err)
	}

	schema, err := ctx.CatalogManager().GetTableSchema(transCtx, tableID)
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	td := schema.TupleDesc
	if td.NumFields() != 4 {
		t.Errorf("Expected 4 fields, got %d", td.NumFields())
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "complex_table", transCtx)
}

func TestCreateTablePlan_Execute_FileCreation(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("file_test", false)
	stmt.AddField("id", types.IntType, true, nil)

	// Use proper data directory
	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedFileName := filepath.Join(dataDir, "file_test.dat")
	if _, err := os.Stat(expectedFileName); os.IsNotExist(err) {
		t.Errorf("Expected file %s to be created", expectedFileName)
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "file_test", transCtx)
}

func TestDDLResult_String(t *testing.T) {
	result := &result.DDLResult{
		Success: true,
		Message: "Test message",
	}

	expected := fmt.Sprintf("DDL Result - Success: %t, Message: %s", result.Success, result.Message)
	if result.String() != expected {
		t.Errorf("Expected string representation %q, got %q", expected, result.String())
	}
}

func TestDDLResult_Values(t *testing.T) {
	tests := []struct {
		name    string
		success bool
		message string
	}{
		{"Success case", true, "Operation completed"},
		{"Failure case", false, "Operation failed"},
		{"Empty message", true, ""},
		{"Long message", false, "This is a very long error message that describes what went wrong"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &result.DDLResult{
				Success: tt.success,
				Message: tt.message,
			}

			if result.Success != tt.success {
				t.Errorf("Expected success %t, got %t", tt.success, result.Success)
			}

			if result.Message != tt.message {
				t.Errorf("Expected message %q, got %q", tt.message, result.Message)
			}
		})
	}
}

// Tests for primary key index creation

func TestCreateTablePlan_Execute_PrimaryKeyIndex_Created(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("email", types.StringType, false, nil)
	stmt.PrimaryKey = "id"

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)
	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify the success message mentions the primary key index
	if !strings.Contains(result.Message, "BTree index on primary key id") {
		t.Errorf("Expected message to mention primary key index, got: %q", result.Message)
	}

	// Verify table exists
	if !ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table was not created")
	}

	// Get table ID
	tableID, err := ctx.CatalogManager().GetTableID(transCtx, "users")
	if err != nil {
		t.Fatalf("Failed to get table ID: %v", err)
	}

	// Verify primary key index was created
	indexes, err := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) == 0 {
		t.Fatal("No index was created for primary key")
	}

	// Verify index properties
	var pkIndex *systemtable.IndexMetadata
	for _, idx := range indexes {
		if idx.ColumnName == "id" && idx.IndexType == index.BTreeIndex {
			pkIndex = idx
			break
		}
	}

	if pkIndex == nil {
		t.Fatal("Primary key BTree index not found")
	}

	expectedIndexName := "pk_users_id"
	if pkIndex.IndexName != expectedIndexName {
		t.Errorf("Expected index name %q, got %q", expectedIndexName, pkIndex.IndexName)
	}

	// Verify index file exists
	if _, err := os.Stat(pkIndex.FilePath); os.IsNotExist(err) {
		t.Errorf("Index file %s does not exist", pkIndex.FilePath)
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateTablePlan_Execute_NoPrimaryKey_NoIndex(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("simple_table", false)
	stmt.AddField("col1", types.IntType, false, nil)
	stmt.AddField("col2", types.StringType, false, nil)
	// No primary key set

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)
	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify the message doesn't mention index
	if strings.Contains(result.Message, "BTree index") {
		t.Errorf("Expected message to not mention index for table without primary key, got: %q", result.Message)
	}

	// Get table ID
	tableID, err := ctx.CatalogManager().GetTableID(transCtx, "simple_table")
	if err != nil {
		t.Fatalf("Failed to get table ID: %v", err)
	}

	// Verify no index was created
	indexes, err := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	if len(indexes) != 0 {
		t.Errorf("Expected no indexes, but found %d", len(indexes))
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "simple_table", transCtx)
}

func TestCreateTablePlan_Execute_PrimaryKeyIndex_DifferentTypes(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)
	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	tests := []struct {
		name            string
		tableName       string
		pkColumn        string
		pkType          types.Type
		expectedIdxType index.IndexType
	}{
		{
			name:            "Int primary key",
			tableName:       "users_int",
			pkColumn:        "id",
			pkType:          types.IntType,
			expectedIdxType: index.BTreeIndex,
		},
		{
			name:            "String primary key",
			tableName:       "users_string",
			pkColumn:        "email",
			pkType:          types.StringType,
			expectedIdxType: index.BTreeIndex,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := statements.NewCreateStatement(tt.tableName, false)
			stmt.AddField(tt.pkColumn, tt.pkType, true, nil)
			stmt.AddField("name", types.StringType, false, nil)
			stmt.PrimaryKey = tt.pkColumn

			plan := NewCreateTablePlan(stmt, ctx, transCtx)
			result, err := executePlan(t, plan)

			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			if !result.Success {
				t.Error("Expected success to be true")
			}

			// Verify index was created
			tableID, _ := ctx.CatalogManager().GetTableID(transCtx, tt.tableName)
			indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)

			if len(indexes) == 0 {
				t.Error("No index was created")
			} else {
				idx := indexes[0]
				if idx.IndexType != tt.expectedIdxType {
					t.Errorf("Expected index type %v, got %v", tt.expectedIdxType, idx.IndexType)
				}

				if idx.ColumnName != tt.pkColumn {
					t.Errorf("Expected column name %q, got %q", tt.pkColumn, idx.ColumnName)
				}
			}

			testutil.CleanupTable(t, ctx.CatalogManager(), tt.tableName, transCtx)
		})
	}
}

func TestCreateTablePlan_Execute_PrimaryKeyIndex_MultipleColumns(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("products", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("category", types.StringType, false, nil)
	stmt.AddField("price", types.FloatType, false, nil)
	stmt.PrimaryKey = "id" // Only one column as primary key

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)
	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify only one index was created (on the primary key column)
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "products")
	indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)

	if len(indexes) != 1 {
		t.Errorf("Expected exactly 1 index, got %d", len(indexes))
	}

	if len(indexes) > 0 {
		idx := indexes[0]
		if idx.ColumnName != "id" {
			t.Errorf("Expected index on column 'id', got %q", idx.ColumnName)
		}

		expectedName := "pk_products_id"
		if idx.IndexName != expectedName {
			t.Errorf("Expected index name %q, got %q", expectedName, idx.IndexName)
		}
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "products", transCtx)
}

func TestCreateTablePlan_Execute_PrimaryKeyIndex_IndexNameConvention(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)
	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	tests := []struct {
		tableName         string
		pkColumn          string
		expectedIndexName string
	}{
		{"users", "id", "pk_users_id"},
		{"products", "product_id", "pk_products_product_id"},
		{"orders", "order_number", "pk_orders_order_number"},
	}

	for _, tt := range tests {
		t.Run(tt.tableName, func(t *testing.T) {
			stmt := statements.NewCreateStatement(tt.tableName, false)
			stmt.AddField(tt.pkColumn, types.IntType, true, nil)
			stmt.AddField("data", types.StringType, false, nil)
			stmt.PrimaryKey = tt.pkColumn

			plan := NewCreateTablePlan(stmt, ctx, transCtx)
			_, err := executePlan(t, plan)

			if err != nil {
				t.Fatalf("Execute failed: %v", err)
			}

			// Verify index name follows convention
			tableID, _ := ctx.CatalogManager().GetTableID(transCtx, tt.tableName)
			indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)

			if len(indexes) > 0 {
				if indexes[0].IndexName != tt.expectedIndexName {
					t.Errorf("Expected index name %q, got %q", tt.expectedIndexName, indexes[0].IndexName)
				}
			}

			testutil.CleanupTable(t, ctx.CatalogManager(), tt.tableName, transCtx)
		})
	}
}

func TestCreateTablePlan_Execute_PrimaryKeyIndex_FileLocation(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("test_table", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.PrimaryKey = "id"

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)
	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify index file is in the correct location
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "test_table")
	indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)

	if len(indexes) > 0 {
		idx := indexes[0]

		// Verify file path contains table name and index name
		if !strings.Contains(idx.FilePath, "test_table") {
			t.Errorf("Index file path should contain table name, got: %q", idx.FilePath)
		}

		if !strings.Contains(idx.FilePath, "pk_test_table_id") {
			t.Errorf("Index file path should contain index name, got: %q", idx.FilePath)
		}

		// Verify file has .idx extension
		if !strings.HasSuffix(idx.FilePath, ".idx") {
			t.Errorf("Index file should have .idx extension, got: %q", idx.FilePath)
		}

		// Verify file actually exists
		if _, err := os.Stat(idx.FilePath); os.IsNotExist(err) {
			t.Errorf("Index file does not exist at: %q", idx.FilePath)
		}
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "test_table", transCtx)
}

func TestCreateTablePlan_Execute_PrimaryKeyIndex_WithAutoIncrement(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateStatement("auto_inc_table", false)
	stmt.AddField("id", types.IntType, true, nil) // Auto-increment
	stmt.AddField("name", types.StringType, false, nil)
	stmt.PrimaryKey = "id"

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateTablePlan(stmt, ctx, transCtx)
	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify index was created even with auto-increment
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "auto_inc_table")
	indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)

	if len(indexes) == 0 {
		t.Error("No index was created for auto-increment primary key")
	}

	if len(indexes) > 0 {
		idx := indexes[0]
		if idx.ColumnName != "id" {
			t.Errorf("Expected index on 'id', got %q", idx.ColumnName)
		}
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "auto_inc_table", transCtx)
}

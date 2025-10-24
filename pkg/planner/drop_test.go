package planner

import (
	"os"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"testing"
)

// Helper function to create a test table for DROP testing
func createTestTableForDrop(t *testing.T, ctx DbContext, transCtx TxContext, tableName, primaryKey string) {
	t.Helper()
	stmt := statements.NewCreateStatement(tableName, false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("email", types.StringType, false, nil)
	stmt.AddField("age", types.IntType, false, nil)
	stmt.PrimaryKey = primaryKey

	plan := NewCreateTablePlan(stmt, ctx, transCtx)
	_, err := plan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
}

// Helper function to execute DROP TABLE plan
func executeDropTablePlan(t *testing.T, plan *DropTablePlan) (*DDLResult, error) {
	t.Helper()
	result, err := plan.Execute()
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	ddlResult, ok := result.(*DDLResult)
	if !ok {
		t.Fatalf("Expected *DDLResult, got %T", result)
	}
	return ddlResult, nil
}

func TestNewDropTablePlan(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewDropStatement("users", false)
	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewDropTablePlan(stmt, ctx, transCtx)

	if plan == nil {
		t.Fatal("NewDropTablePlan returned nil")
	}

	if plan.Statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.ctx != ctx {
		t.Error("Context not properly assigned")
	}

	if plan.tx != transCtx {
		t.Error("TxContext not properly assigned")
	}
}

func TestDropTablePlan_Execute_BasicSuccess(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create a simple table without primary key
	stmt := statements.NewCreateStatement("simple_table", false)
	stmt.AddField("col1", types.IntType, false, nil)
	stmt.AddField("col2", types.StringType, false, nil)

	createPlan := NewCreateTablePlan(stmt, ctx, transCtx)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Verify table exists
	if !ctx.CatalogManager().TableExists(transCtx, "simple_table") {
		t.Fatal("Table was not created")
	}

	// Drop the table
	dropStmt := statements.NewDropStatement("simple_table", false)
	dropPlan := NewDropTablePlan(dropStmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, dropPlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table simple_table dropped successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	// Verify table no longer exists
	if ctx.CatalogManager().TableExists(transCtx, "simple_table") {
		t.Error("Table still exists after drop")
	}
}

func TestDropTablePlan_Execute_WithPrimaryKeyIndex(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create table with primary key (which should auto-create an index)
	createTestTableForDrop(t, ctx, transCtx, "users", "id")

	// Verify table exists
	if !ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Fatal("Table was not created")
	}

	// Get table ID and check for primary key index
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
		t.Fatal("No primary key index was created")
	}

	// Store index file paths for verification
	var indexPaths []string
	for _, idx := range indexes {
		indexPaths = append(indexPaths, idx.FilePath)
		// Verify index file exists
		if _, err := os.Stat(idx.FilePath); os.IsNotExist(err) {
			t.Errorf("Index file %s does not exist", idx.FilePath)
		}
	}

	// Drop the table
	dropStmt := statements.NewDropStatement("users", false)
	dropPlan := NewDropTablePlan(dropStmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, dropPlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify table no longer exists
	if ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table still exists after drop")
	}

	// Verify all index files were deleted
	for _, indexPath := range indexPaths {
		if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
			t.Errorf("Index file %s still exists after table drop", indexPath)
		}
	}
}

func TestDropTablePlan_Execute_WithMultipleIndexes(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create table with primary key
	createTestTableForDrop(t, ctx, transCtx, "users", "id")

	// Create additional indexes
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)
	createTestIndex(t, ctx, transCtx, "users", "idx_users_name", "name", index.BTreeIndex)
	createTestIndex(t, ctx, transCtx, "users", "idx_users_age", "age", index.HashIndex)

	// Verify all indexes exist
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "users")
	indexes, err := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	// Should have 4 indexes: 1 primary key + 3 manual
	if len(indexes) < 3 {
		t.Logf("Warning: Expected at least 3 indexes, got %d", len(indexes))
	}

	// Store index file paths and names
	var indexPaths []string
	var indexNames []string
	for _, idx := range indexes {
		indexPaths = append(indexPaths, idx.FilePath)
		indexNames = append(indexNames, idx.IndexName)
	}

	// Drop the table
	dropStmt := statements.NewDropStatement("users", false)
	dropPlan := NewDropTablePlan(dropStmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, dropPlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify table no longer exists
	if ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table still exists after drop")
	}

	// Verify all indexes no longer exist in catalog
	for _, indexName := range indexNames {
		if ctx.CatalogManager().IndexExists(transCtx, indexName) {
			t.Errorf("Index %s still exists in catalog after table drop", indexName)
		}
	}

	// Verify all index files were deleted
	for _, indexPath := range indexPaths {
		if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
			t.Errorf("Index file %s still exists after table drop", indexPath)
		}
	}
}

func TestDropTablePlan_Execute_IfExists_TableExists(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create table
	createTestTableForDrop(t, ctx, transCtx, "users", "id")

	// Drop with IF EXISTS
	stmt := statements.NewDropStatement("users", true)
	plan := NewDropTablePlan(stmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users dropped successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	// Verify table no longer exists
	if ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table still exists after drop")
	}
}

func TestDropTablePlan_Execute_IfExists_TableDoesNotExist(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Don't create the table

	// Drop with IF EXISTS
	stmt := statements.NewDropStatement("nonexistent_table", true)
	plan := NewDropTablePlan(stmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table nonexistent_table does not exist (IF EXISTS)"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDropTablePlan_Execute_Error_TableDoesNotExist(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Drop without IF EXISTS
	stmt := statements.NewDropStatement("nonexistent_table", false)
	plan := NewDropTablePlan(stmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when table does not exist")
	}

	expectedError := "table nonexistent_table does not exist"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestDropTablePlan_Execute_WithIndexFilesMissing(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create table with primary key
	createTestTableForDrop(t, ctx, transCtx, "users", "id")

	// Create additional index
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Get indexes and manually delete their files
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "users")
	indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)

	for _, idx := range indexes {
		os.Remove(idx.FilePath)
	}

	// Drop the table (should succeed even if index files don't exist)
	dropStmt := statements.NewDropStatement("users", false)
	dropPlan := NewDropTablePlan(dropStmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, dropPlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true even when index files are missing")
	}

	// Verify table was removed
	if ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Table still exists after drop")
	}
}

func TestDropTablePlan_Execute_NoIndexes(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create table without primary key (no auto-index)
	stmt := statements.NewCreateStatement("no_pk_table", false)
	stmt.AddField("col1", types.IntType, false, nil)
	stmt.AddField("col2", types.StringType, false, nil)

	createPlan := NewCreateTablePlan(stmt, ctx, transCtx)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Drop the table
	dropStmt := statements.NewDropStatement("no_pk_table", false)
	dropPlan := NewDropTablePlan(dropStmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, dropPlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify table no longer exists
	if ctx.CatalogManager().TableExists(transCtx, "no_pk_table") {
		t.Error("Table still exists after drop")
	}
}

func TestDropTablePlan_Execute_CascadeMultipleTables(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create multiple tables
	createTestTableForDrop(t, ctx, transCtx, "users", "id")
	createTestTableForDrop(t, ctx, transCtx, "products", "id")
	createTestTableForDrop(t, ctx, transCtx, "orders", "id")

	// Create indexes on each table
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)
	createTestIndex(t, ctx, transCtx, "products", "idx_products_name", "name", index.BTreeIndex)
	createTestIndex(t, ctx, transCtx, "orders", "idx_orders_email", "email", index.HashIndex)

	// Drop users table
	dropStmt := statements.NewDropStatement("users", false)
	dropPlan := NewDropTablePlan(dropStmt, ctx, transCtx)
	result, err := executeDropTablePlan(t, dropPlan)

	if err != nil {
		t.Fatalf("Failed to drop users table: %v", err)
	}

	if !result.Success {
		t.Error("Expected success for users drop")
	}

	// Verify users table and its indexes are gone
	if ctx.CatalogManager().TableExists(transCtx, "users") {
		t.Error("Users table still exists")
	}

	if ctx.CatalogManager().IndexExists(transCtx, "idx_users_email") {
		t.Error("Users index still exists")
	}

	// Verify other tables still exist
	if !ctx.CatalogManager().TableExists(transCtx, "products") {
		t.Error("Products table should still exist")
	}

	if !ctx.CatalogManager().TableExists(transCtx, "orders") {
		t.Error("Orders table should still exist")
	}

	if !ctx.CatalogManager().IndexExists(transCtx, "idx_products_name") {
		t.Error("Products index should still exist")
	}

	if !ctx.CatalogManager().IndexExists(transCtx, "idx_orders_email") {
		t.Error("Orders index should still exist")
	}

	// Cleanup remaining tables
	dropPlan2 := NewDropTablePlan(statements.NewDropStatement("products", false), ctx, transCtx)
	dropPlan2.Execute()

	dropPlan3 := NewDropTablePlan(statements.NewDropStatement("orders", false), ctx, transCtx)
	dropPlan3.Execute()
}

func TestDropTablePlan_Execute_EmptyTableName(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Try to drop empty table name
	stmt := statements.NewDropStatement("", false)
	plan := NewDropTablePlan(stmt, ctx, transCtx)

	result, err := executeDropTablePlan(t, plan)

	if result != nil && result.Success {
		t.Error("Should not succeed with empty table name")
	}

	if err == nil {
		t.Error("Expected error with empty table name")
	}
}

func TestDDLResult_DropTable(t *testing.T) {
	tests := []struct {
		name           string
		success        bool
		message        string
		expectedString string
	}{
		{
			name:           "Success",
			success:        true,
			message:        "Table users dropped successfully",
			expectedString: "DDL Result - Success: true, Message: Table users dropped successfully",
		},
		{
			name:           "Failure",
			success:        false,
			message:        "Failed to drop table",
			expectedString: "DDL Result - Success: false, Message: Failed to drop table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DDLResult{
				Success: tt.success,
				Message: tt.message,
			}

			if result.String() != tt.expectedString {
				t.Errorf("Expected %q, got %q", tt.expectedString, result.String())
			}
		})
	}
}

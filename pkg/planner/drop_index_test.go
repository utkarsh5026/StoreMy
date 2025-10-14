package planner

import (
	"os"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
	"testing"
)

// Helper function to create an index for DROP testing
func createTestIndex(t *testing.T, ctx DbContext, transCtx TransactionCtx, tableName, indexName, columnName string, indexType index.IndexType) {
	stmt := statements.NewCreateIndexStatement(indexName, tableName, columnName, indexType, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)
	_, err := plan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test index: %v", err)
	}
}

func TestNewDropIndexPlan(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewDropIndexStatement("idx_users_email", "", false)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	if plan == nil {
		t.Fatal("NewDropIndexPlan returned nil")
	}

	if plan.Statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.ctx != ctx {
		t.Error("Context not properly assigned")
	}

	if plan.transactionCtx != transCtx {
		t.Error("TransactionCtx not properly assigned")
	}
}

func TestDropIndexPlan_Execute_Success(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create table and index
	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Verify index exists
	if !ctx.CatalogManager().IndexExists(transCtx.ID, "idx_users_email") {
		t.Fatal("Index was not created")
	}

	// Get index metadata to check file path
	indexMeta, _ := ctx.CatalogManager().GetIndexByName(transCtx.ID, "idx_users_email")
	filePath := indexMeta.FilePath

	// Verify file exists before dropping
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatal("Index file does not exist before dropping")
	}

	// Drop the index
	stmt := statements.NewDropIndexStatement("idx_users_email", "", false)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Index idx_users_email dropped successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	// Verify index no longer exists in catalog
	if ctx.CatalogManager().IndexExists(transCtx.ID, "idx_users_email") {
		t.Error("Index still exists in catalog after drop")
	}

	// Verify file was deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("Index file still exists after drop")
	}

	// Cleanup
	cleanupTable(t, ctx.CatalogManager(), "users", transCtx.ID)
}

func TestDropIndexPlan_Execute_IfExists_IndexExists(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Drop with IF EXISTS
	stmt := statements.NewDropIndexStatement("idx_users_email", "", true)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Index idx_users_email dropped successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	// Cleanup
	cleanupTable(t, ctx.CatalogManager(), "users", transCtx.ID)
}

func TestDropIndexPlan_Execute_IfExists_IndexDoesNotExist(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Don't create the index

	// Drop with IF EXISTS
	stmt := statements.NewDropIndexStatement("nonexistent_index", "", true)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Index nonexistent_index does not exist (IF EXISTS)"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDropIndexPlan_Execute_Error_IndexDoesNotExist(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Drop without IF EXISTS
	stmt := statements.NewDropIndexStatement("nonexistent_index", "", false)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when index does not exist")
	}

	expectedError := "index nonexistent_index does not exist"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestDropIndexPlan_Execute_WithTableName_Success(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Drop with table name specified
	stmt := statements.NewDropIndexStatement("idx_users_email", "users", false)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Cleanup
	cleanupTable(t, ctx.CatalogManager(), "users", transCtx.ID)
}

func TestDropIndexPlan_Execute_WithTableName_Error_WrongTable(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create two tables
	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestTableForIndex(t, ctx, transCtx, "products")

	// Create index on users table
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Try to drop index with wrong table name
	stmt := statements.NewDropIndexStatement("idx_users_email", "products", false)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when table name doesn't match")
	}

	expectedError := "index idx_users_email does not belong to table products"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	cleanupTable(t, ctx.CatalogManager(), "users", transCtx.ID)
	cleanupTable(t, ctx.CatalogManager(), "products", transCtx.ID)
}

func TestDropIndexPlan_Execute_FileAlreadyDeleted(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Get index metadata and manually delete the file
	indexMeta, _ := ctx.CatalogManager().GetIndexByName(transCtx.ID, "idx_users_email")
	filePath := indexMeta.FilePath
	os.Remove(filePath)

	// Drop the index (should succeed even if file doesn't exist)
	stmt := statements.NewDropIndexStatement("idx_users_email", "", false)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	result, err := executeDropIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true even when file is missing")
	}

	// Verify index was removed from catalog
	if ctx.CatalogManager().IndexExists(transCtx.ID, "idx_users_email") {
		t.Error("Index still exists in catalog")
	}

	// Cleanup
	cleanupTable(t, ctx.CatalogManager(), "users", transCtx.ID)
}

func TestDropIndexPlan_Execute_MultipleIndexes(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create multiple indexes
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)
	createTestIndex(t, ctx, transCtx, "users", "idx_users_age", "age", index.BTreeIndex)
	createTestIndex(t, ctx, transCtx, "users", "idx_users_name", "name", index.HashIndex)

	// Verify all exist
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx.ID, "users")
	indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx.ID, tableID)
	if len(indexes) != 3 {
		t.Fatalf("Expected 3 indexes, got %d", len(indexes))
	}

	// Drop one index
	stmt := statements.NewDropIndexStatement("idx_users_age", "", false)
	plan := NewDropIndexPlan(stmt, ctx, transCtx)
	_, err := executeDropIndexPlan(t, plan)
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Verify only 2 remain
	indexes, _ = ctx.CatalogManager().GetIndexesByTable(transCtx.ID, tableID)
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes after drop, got %d", len(indexes))
	}

	// Verify correct index was dropped
	if ctx.CatalogManager().IndexExists(transCtx.ID, "idx_users_age") {
		t.Error("Dropped index still exists")
	}

	// Verify other indexes still exist
	if !ctx.CatalogManager().IndexExists(transCtx.ID, "idx_users_email") {
		t.Error("idx_users_email should still exist")
	}
	if !ctx.CatalogManager().IndexExists(transCtx.ID, "idx_users_name") {
		t.Error("idx_users_name should still exist")
	}

	// Cleanup
	dropPlan1 := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan1.Execute()

	dropPlan2 := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_name", "", false),
		ctx, transCtx)
	dropPlan2.Execute()

	cleanupTable(t, ctx.CatalogManager(), "users", transCtx.ID)
}

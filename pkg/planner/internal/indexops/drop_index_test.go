package indexops

import (
	"storemy/pkg/planner/internal/testutil"
	"os"
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
	"testing"
)

func TestNewDropIndexPlan(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewDropIndexStatement("idx_users_email", "", false)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewDropIndexPlan(stmt, ctx, transCtx)

	if plan == nil {
		t.Fatal("NewDropIndexPlan returned nil")
	}

	if plan.Statement != stmt {
		t.Error("Statement not properly assigned")
	}

}

func TestDropIndexPlan_Execute_Success(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	// Create table and index
	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Verify index exists
	if !ctx.CatalogManager().IndexExists(transCtx, "idx_users_email") {
		t.Fatal("Index was not created")
	}

	// Get index metadata to check file path
	indexMeta, _ := ctx.CatalogManager().GetIndexByName(transCtx, "idx_users_email")
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
	if ctx.CatalogManager().IndexExists(transCtx, "idx_users_email") {
		t.Error("Index still exists in catalog after drop")
	}

	// Verify file was deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Error("Index file still exists after drop")
	}

	// Cleanup
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestDropIndexPlan_Execute_IfExists_IndexExists(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

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
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestDropIndexPlan_Execute_IfExists_IndexDoesNotExist(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

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
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

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
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

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
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestDropIndexPlan_Execute_WithTableName_Error_WrongTable(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

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
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
	testutil.CleanupTable(t, ctx.CatalogManager(), "products", transCtx)
}

func TestDropIndexPlan_Execute_FileAlreadyDeleted(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)

	// Get index metadata and manually delete the file
	indexMeta, _ := ctx.CatalogManager().GetIndexByName(transCtx, "idx_users_email")
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
	if ctx.CatalogManager().IndexExists(transCtx, "idx_users_email") {
		t.Error("Index still exists in catalog")
	}

	// Cleanup
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestDropIndexPlan_Execute_MultipleIndexes(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create multiple indexes
	createTestIndex(t, ctx, transCtx, "users", "idx_users_email", "email", index.HashIndex)
	createTestIndex(t, ctx, transCtx, "users", "idx_users_age", "age", index.BTreeIndex)
	createTestIndex(t, ctx, transCtx, "users", "idx_users_name", "name", index.HashIndex)

	// Verify all exist (3 manually created indexes - PK indexes are now created by DDL layer)
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "users")
	indexes, _ := ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)
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
	indexes, _ = ctx.CatalogManager().GetIndexesByTable(transCtx, tableID)
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes after drop, got %d", len(indexes))
	}

	// Verify correct index was dropped
	if ctx.CatalogManager().IndexExists(transCtx, "idx_users_age") {
		t.Error("Dropped index still exists")
	}

	// Verify other indexes still exist
	if !ctx.CatalogManager().IndexExists(transCtx, "idx_users_email") {
		t.Error("idx_users_email should still exist")
	}
	if !ctx.CatalogManager().IndexExists(transCtx, "idx_users_name") {
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

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

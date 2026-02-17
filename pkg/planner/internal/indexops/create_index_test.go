package indexops

import (
	"os"
	"path/filepath"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/planner/internal/testutil"
	"storemy/pkg/registry"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"testing"
)

// Helper function to create a table for index testing
func createTestTableForIndex(t *testing.T, ctx *registry.DatabaseContext, transCtx *transaction.TransactionContext, tableName string) {
	t.Helper()

	// Create schema columns
	columns := []schema.ColumnMetadata{
		{Name: "id", FieldType: types.IntType, Position: 0, IsPrimary: true},
		{Name: "name", FieldType: types.StringType, Position: 1},
		{Name: "age", FieldType: types.IntType, Position: 2},
		{Name: "email", FieldType: types.StringType, Position: 3},
	}

	// Create table schema
	tblSchema, err := schema.NewSchema(0, tableName, columns)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}
	tblSchema.PrimaryKey = "id"

	// Create table in catalog
	_, err = ctx.CatalogManager().CreateTable(transCtx, tblSchema)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}
}

// Helper function to execute CREATE INDEX plan
func executeCreateIndexPlan(t *testing.T, plan *CreateIndexPlan) (*result.DDLResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*result.DDLResult)
	if !ok {
		t.Fatalf("Result is not a DDLResult, got %T", resultAny)
	}

	return result, nil
}

// Helper function to execute DROP INDEX plan
func executeDropIndexPlan(t *testing.T, plan *DropIndexPlan) (*result.DDLResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*result.DDLResult)
	if !ok {
		t.Fatalf("Result is not a DDLResult, got %T", resultAny)
	}

	return result, nil
}

func TestNewCreateIndexPlan(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	stmt := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	if plan == nil {
		t.Fatal("NewCreateIndexPlan returned nil")
	}

	if plan.Statement != stmt {
		t.Error("Statement not properly assigned")
	}

}

func TestCreateIndexPlan_Execute_HashIndex(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	// Create the test table first
	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create HASH index
	stmt := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify index exists in catalog
	if !ctx.CatalogManager().NewIndexOps(transCtx).IndexExists("idx_users_email") {
		t.Error("Index was not added to catalog")
	}

	// Verify index file was created
	indexMeta, _ := ctx.CatalogManager().NewIndexOps(transCtx).GetIndexByName("idx_users_email")
	if !indexMeta.FilePath.Exists() {
		t.Errorf("Index file was not created at %s", indexMeta.FilePath)
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_BTreeIndex(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create BTREE index
	stmt := statements.NewCreateIndexStatement("idx_users_age", "users", "age", index.BTreeIndex, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.CatalogManager().NewIndexOps(transCtx).IndexExists("idx_users_age") {
		t.Error("Index was not added to catalog")
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_age", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_IfNotExists_IndexDoesNotExist(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	stmt := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, true)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Index idx_users_email created successfully on users(email) using HASH"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_IfNotExists_IndexExists(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create first index
	stmt1 := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)
	plan1 := NewCreateIndexPlan(stmt1, ctx, transCtx)
	_, err := plan1.Execute()
	if err != nil {
		t.Fatalf("Failed to create first index: %v", err)
	}

	// Try to create same index with IF NOT EXISTS
	stmt2 := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, true)
	plan2 := NewCreateIndexPlan(stmt2, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan2)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Index idx_users_email already exists (IF NOT EXISTS)"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_Error_TableDoesNotExist(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	stmt := statements.NewCreateIndexStatement("idx_users_email", "nonexistent_table", "email", index.HashIndex, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan)

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

func TestCreateIndexPlan_Execute_Error_ColumnDoesNotExist(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	stmt := statements.NewCreateIndexStatement("idx_users_invalid", "users", "nonexistent_column", index.HashIndex, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when column does not exist")
	}

	expectedError := "column nonexistent_column does not exist in table users"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_Error_IndexAlreadyExists(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create first index
	stmt1 := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)
	plan1 := NewCreateIndexPlan(stmt1, ctx, transCtx)
	_, err := plan1.Execute()
	if err != nil {
		t.Fatalf("Failed to create first index: %v", err)
	}

	// Try to create same index again without IF NOT EXISTS
	stmt2 := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)
	plan2 := NewCreateIndexPlan(stmt2, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan2)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when index already exists")
	}

	expectedError := "index idx_users_email already exists"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_MultipleIndexesOnSameTable(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	// Create first index on email
	stmt1 := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)
	plan1 := NewCreateIndexPlan(stmt1, ctx, transCtx)
	result1, err := executeCreateIndexPlan(t, plan1)
	if err != nil {
		t.Fatalf("Failed to create first index: %v", err)
	}
	if !result1.Success {
		t.Error("Expected first index creation to succeed")
	}

	// Create second index on age
	stmt2 := statements.NewCreateIndexStatement("idx_users_age", "users", "age", index.BTreeIndex, false)
	plan2 := NewCreateIndexPlan(stmt2, ctx, transCtx)
	result2, err := executeCreateIndexPlan(t, plan2)
	if err != nil {
		t.Fatalf("Failed to create second index: %v", err)
	}
	if !result2.Success {
		t.Error("Expected second index creation to succeed")
	}

	// Verify both indexes exist
	tableID, _ := ctx.CatalogManager().GetTableID(transCtx, "users")
	indexes, err := ctx.CatalogManager().NewIndexOps(transCtx).GetIndexesByTable(tableID)
	if err != nil {
		t.Fatalf("Failed to get indexes: %v", err)
	}

	// Expected: 2 manually created indexes (PK indexes are now created by DDL layer, not catalog manager)
	if len(indexes) != 2 {
		t.Errorf("Expected 2 indexes, got %d", len(indexes))
	}

	// Cleanup
	dropPlan1 := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan1.Execute()

	dropPlan2 := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_age", "", false),
		ctx, transCtx)
	dropPlan2.Execute()

	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

func TestCreateIndexPlan_Execute_IndexFileCreation(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	transCtx, _ := txRegistry.Begin()

	createTestTableForIndex(t, ctx, transCtx, "users")

	stmt := statements.NewCreateIndexStatement("idx_users_email", "users", "email", index.HashIndex, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)

	result, err := executeCreateIndexPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	// Verify index file exists
	expectedFileName := filepath.Join(dataDir, "users_idx_users_email.idx")
	if _, err := os.Stat(expectedFileName); os.IsNotExist(err) {
		t.Errorf("Expected index file %s to be created", expectedFileName)
	}

	// Cleanup
	dropPlan := NewDropIndexPlan(
		statements.NewDropIndexStatement("idx_users_email", "", false),
		ctx, transCtx)
	dropPlan.Execute()
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", transCtx)
}

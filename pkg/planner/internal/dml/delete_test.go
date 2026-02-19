package dml

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/planner/internal/testutil"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/types"
	"testing"
)

func setupDeleteTest(t *testing.T) (string, *registry.DatabaseContext, *transaction.TransactionContext) {
	dataDir := testutil.SetupTestDataDir(t)
	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	tx, _ := txRegistry.Begin()

	return dataDir, ctx, tx
}

// Helper function to execute delete plan and cast result to result.DMLResult
func executeDeletePlan(t *testing.T, plan *DeletePlan) (*shared.DMLResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*shared.DMLResult)
	if !ok {
		t.Fatalf("Result is not a result.DMLResult, got %T", resultAny)
	}

	return result, nil
}

// Helper function to create and populate a test table with sample data
func createAndPopulateTestTable(t *testing.T, ctx *registry.DatabaseContext, tx *transaction.TransactionContext) {
	t.Helper()

	// Create table using direct schema creation
	createTestTable(t, ctx, tx)

	// Note: testutil.CleanupTable is called inside createTestTable

	// Insert test data
	insertStmt := statements.NewInsertStatement("test_table")

	// Row 1
	values1 := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 99.99},
	}
	insertStmt.AddValues(values1)

	// Row 2
	values2 := []types.Field{
		&types.IntField{Value: 2},
		types.NewStringField("Jane", types.StringMaxSize),
		&types.BoolField{Value: false},
		&types.Float64Field{Value: 149.99},
	}
	insertStmt.AddValues(values2)

	// Row 3
	values3 := []types.Field{
		&types.IntField{Value: 3},
		types.NewStringField("Bob", types.StringMaxSize),
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 199.99},
	}
	insertStmt.AddValues(values3)

	insertPlan := NewInsertPlan(insertStmt, tx, ctx)
	_, err := insertPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to populate test table: %v", err)
	}
}

func TestNewDeletePlan(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	stmt := statements.NewDeleteStatement("test_table", "")

	plan := NewDeletePlan(stmt, tx, ctx)

	if plan == nil {
		t.Fatal("NewDeletePlan returned nil")
	}
}

func TestDeletePlan_Execute_DeleteAll(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)
	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	tx, _ := txRegistry.Begin()

	createAndPopulateTestTable(t, ctx, tx)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "3 row(s) deleted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDeletePlan_Execute_WithWhereClause(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	// DELETE FROM test_table WHERE id = 2
	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: primitives.Equals,
		Constant:  "2",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tx, ctx)
	result, err := executeDeletePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	expectedMessage := "1 row(s) deleted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDeletePlan_Execute_WithWhereClause_MultipleRows(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	// DELETE FROM test_table WHERE active = true
	whereClause := &plan.FilterNode{
		Field:     "active",
		Predicate: primitives.Equals,
		Constant:  "true",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "2 row(s) deleted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDeletePlan_Execute_WithWhereClause_NoMatch(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	// DELETE FROM test_table WHERE id = 999 (no match)
	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: primitives.Equals,
		Constant:  "999",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "0 row(s) deleted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDeletePlan_Execute_WithWhereClause_GreaterThan(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	// DELETE FROM test_table WHERE id > 1
	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: primitives.GreaterThan,
		Constant:  "1",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "2 row(s) deleted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDeletePlan_Execute_Error_TableNotFound(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)

	stmt := statements.NewDeleteStatement("nonexistent_table", "")
	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when table does not exist")
	}

	expectedError := "table nonexistent_table not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestDeletePlan_Execute_Error_InvalidField(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	// DELETE FROM test_table WHERE invalid_field = 'value'
	whereClause := &plan.FilterNode{
		Field:     "invalid_field",
		Predicate: primitives.Equals,
		Constant:  "value",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when field does not exist")
	}

	expectedError := "failed to build WHERE predicate: column invalid_field not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestDeletePlan_Execute_EmptyTable(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createTestTable(t, ctx, tx)
	testutil.CleanupTable(t, ctx.CatalogManager(), "test_table", tx)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tx, ctx)

	result, err := executeDeletePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "0 row(s) deleted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestDeletePlan_createTableScan(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	stmt := statements.NewDeleteStatement("test_table", "")

	tableID, err := shared.ResolveTableID(stmt.TableName, tx, ctx)
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	scanOp, err := BuildScanWithFilter(tx, tableID, nil, ctx)

	if err != nil {
		t.Fatalf("createTableScan failed: %v", err)
	}

	if scanOp == nil {
		t.Fatal("Scan operation is nil")
	}

	// Test that the scan can be opened and closed
	err = scanOp.Open()
	if err != nil {
		t.Fatalf("Failed to open scan: %v", err)
	}
	defer scanOp.Close()
}

func TestDeletePlan_addWhereFilter(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: primitives.Equals,
		Constant:  "1",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	tableID, err := shared.ResolveTableID(stmt.TableName, tx, ctx)
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	filterOp, err := BuildScanWithFilter(tx, tableID, whereClause, ctx)

	if err != nil {
		t.Fatalf("addWhereFilter failed: %v", err)
	}

	if filterOp == nil {
		t.Fatal("Filter operation is nil")
	}

	// Test that the filter can be opened and closed
	err = filterOp.Open()
	if err != nil {
		t.Fatalf("Failed to open filter: %v", err)
	}
	defer filterOp.Close()
}

func TestDeletePlan_collectTuplesToDelete(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	stmt := statements.NewDeleteStatement("test_table", "")

	tableID, err := shared.ResolveTableID(stmt.TableName, tx, ctx)
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	query, err := BuildScanWithFilter(tx, tableID, nil, ctx)
	if err != nil {
		t.Fatalf("createQuery failed: %v", err)
	}

	tuplesToDelete, err := shared.CollectAllTuples(query)

	if err != nil {
		t.Fatalf("collectAllTuples failed: %v", err)
	}

	if len(tuplesToDelete) != 3 {
		t.Errorf("Expected 3 tuples to delete, got %d", len(tuplesToDelete))
	}

	// Verify all tuples are not nil
	for i, tuple := range tuplesToDelete {
		if tuple == nil {
			t.Errorf("Tuple %d is nil", i)
		}
	}
}

func TestDeletePlan_createQuery_NoWhere(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	stmt := statements.NewDeleteStatement("test_table", "")

	tableID, err := shared.ResolveTableID(stmt.TableName, tx, ctx)
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	query, err := BuildScanWithFilter(tx, tableID, nil, ctx)

	if err != nil {
		t.Fatalf("createQuery failed: %v", err)
	}

	if query == nil {
		t.Fatal("Query is nil")
	}

	// Test that the query can be opened and closed
	err = query.Open()
	if err != nil {
		t.Fatalf("Failed to open query: %v", err)
	}
	defer query.Close()
}

func TestDeletePlan_createQuery_WithWhere(t *testing.T) {
	_, ctx, tx := setupDeleteTest(t)
	createAndPopulateTestTable(t, ctx, tx)

	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: primitives.Equals,
		Constant:  "2",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	tableID, err := shared.ResolveTableID(stmt.TableName, tx, ctx)
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	query, err := BuildScanWithFilter(tx, tableID, whereClause, ctx)

	if err != nil {
		t.Fatalf("createQuery failed: %v", err)
	}

	if query == nil {
		t.Fatal("Query is nil")
	}

	// Test that the query can be opened and closed
	err = query.Open()
	if err != nil {
		t.Fatalf("Failed to open query: %v", err)
	}
	defer query.Close()
}

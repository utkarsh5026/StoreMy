package planner

import (
	"os"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/plan"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/types"
	"testing"
)

// Helper function to execute delete plan and cast result to DMLResult
func executeDeletePlan(t *testing.T, plan *DeletePlan) (*DMLResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*DMLResult)
	if !ok {
		t.Fatalf("Result is not a DMLResult, got %T", resultAny)
	}

	return result, nil
}

// Helper function to create and populate a test table with sample data
func createAndPopulateTestTable(t *testing.T, ctx *registry.DatabaseContext, tid *transaction.TransactionID) {
	// Create table
	createStmt := statements.NewCreateStatement("test_table", false)
	createStmt.AddField("id", types.IntType, false, nil)
	createStmt.AddField("name", types.StringType, false, nil)
	createStmt.AddField("active", types.BoolType, false, nil)
	createStmt.AddField("price", types.FloatType, false, nil)

	tableManager := ctx.TableManager()

	createPlan := NewCreateTablePlan(createStmt, ctx, tid)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

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

	insertPlan := NewInsertPlan(insertStmt, tid, ctx)
	_, err = insertPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to populate test table: %v", err)
	}

	// Register cleanup to close table file
	cleanupTable(t, tableManager, "test_table")
}

func TestNewDeletePlan(t *testing.T) {
	stmt := statements.NewDeleteStatement("test_table", "")
	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	plan := NewDeletePlan(stmt, tid, ctx)

	if plan == nil {
		t.Fatal("NewDeletePlan returned nil")
	}

	if plan.statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.tid != tid {
		t.Error("TransactionID not properly assigned")
	}
}

func TestDeletePlan_Execute_DeleteAll(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	// DELETE FROM test_table WHERE id = 2
	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: types.Equals,
		Constant:  "2",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tid, ctx)
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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	// DELETE FROM test_table WHERE active = true
	whereClause := &plan.FilterNode{
		Field:     "active",
		Predicate: types.Equals,
		Constant:  "true",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tid, ctx)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	// DELETE FROM test_table WHERE id = 999 (no match)
	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: types.Equals,
		Constant:  "999",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tid, ctx)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	// DELETE FROM test_table WHERE id > 1
	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: types.GreaterThan,
		Constant:  "1",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tid, ctx)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	stmt := statements.NewDeleteStatement("nonexistent_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	// DELETE FROM test_table WHERE invalid_field = 'value'
	whereClause := &plan.FilterNode{
		Field:     "invalid_field",
		Predicate: types.Equals,
		Constant:  "value",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)

	plan := NewDeletePlan(stmt, tid, ctx)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	// Create table but don't populate it
	createStmt := statements.NewCreateStatement("test_table", false)
	createStmt.AddField("id", types.IntType, false, nil)
	createStmt.AddField("name", types.StringType, false, nil)

	createPlan := NewCreateTablePlan(createStmt, ctx, tid)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Register cleanup to close table file
	cleanupTable(t, ctx.TableManager(), "test_table")

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

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

func TestDeletePlan_getTableID(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()

	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	expectedTableID, _ := ctx.TableManager().GetTableID("test_table")
	if tableID != expectedTableID {
		t.Errorf("Expected table ID %d, got %d", expectedTableID, tableID)
	}
}

func TestDeletePlan_getTableID_Error(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	stmt := statements.NewDeleteStatement("nonexistent_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()

	if err == nil {
		t.Fatal("Expected error when table does not exist")
	}

	if tableID != -1 {
		t.Errorf("Expected table ID -1, got %d", tableID)
	}

	expectedError := "table nonexistent_table not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestDeletePlan_createTableScan(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	scanOp, err := plan.createTableScan(tableID)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: types.Equals,
		Constant:  "1",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	scanOp, err := plan.createTableScan(tableID)
	if err != nil {
		t.Fatalf("createTableScan failed: %v", err)
	}

	filterOp, err := plan.addWhereFilter(scanOp)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	query, err := plan.createQuery(tableID)
	if err != nil {
		t.Fatalf("createQuery failed: %v", err)
	}

	tuplesToDelete, err := collectAllTuples(query)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	stmt := statements.NewDeleteStatement("test_table", "")
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	query, err := plan.createQuery(tableID)

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
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createAndPopulateTestTable(t, ctx, tid)

	whereClause := &plan.FilterNode{
		Field:     "id",
		Predicate: types.Equals,
		Constant:  "2",
	}

	stmt := statements.NewDeleteStatement("test_table", "")
	stmt.SetWhereClause(whereClause)
	plan := NewDeletePlan(stmt, tid, ctx)

	tableID, err := plan.getTableID()
	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	query, err := plan.createQuery(tableID)

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

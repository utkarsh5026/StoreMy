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

func setupSelectTest(t *testing.T) (*registry.DatabaseContext, *transaction.TransactionID, func()) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)

	cleanup := func() {
		os.Chdir(oldDir)
	}

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	return ctx, tid, cleanup
}

func setupSelectTestWithData(t *testing.T) (*registry.DatabaseContext, *transaction.TransactionID, func()) {
	ctx, tid, cleanup := setupSelectTest(t)

	createSelectTestTable(t, ctx, tid)
	insertSelectTestData(t, ctx, tid)

	return ctx, tid, cleanup
}

func executeSelectPlan(t *testing.T, plan *SelectPlan) (*QueryResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*QueryResult)
	if !ok {
		t.Fatalf("Result is not a QueryResult, got %T", resultAny)
	}

	return result, nil
}

func createSelectTestTable(t *testing.T, ctx *registry.DatabaseContext, tid *transaction.TransactionID) {
	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, false, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("email", types.StringType, false, nil)
	stmt.AddField("age", types.IntType, false, nil)
	stmt.AddField("active", types.BoolType, false, nil)
	stmt.AddField("salary", types.FloatType, false, nil)

	createPlan := NewCreateTablePlan(stmt, ctx, tid)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Register cleanup to close table file
	cleanupTable(t, ctx.TableManager(), "users")
}

func insertSelectTestData(t *testing.T, ctx *registry.DatabaseContext, tid *transaction.TransactionID) {
	insertStmt := statements.NewInsertStatement("users")

	values1 := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John Doe", types.StringMaxSize),
		types.NewStringField("john@example.com", types.StringMaxSize),
		&types.IntField{Value: 25},
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 50000.0},
	}
	insertStmt.AddValues(values1)

	values2 := []types.Field{
		&types.IntField{Value: 2},
		types.NewStringField("Jane Smith", types.StringMaxSize),
		types.NewStringField("jane@example.com", types.StringMaxSize),
		&types.IntField{Value: 30},
		&types.BoolField{Value: false},
		&types.Float64Field{Value: 60000.0},
	}
	insertStmt.AddValues(values2)

	values3 := []types.Field{
		&types.IntField{Value: 3},
		types.NewStringField("Bob Johnson", types.StringMaxSize),
		types.NewStringField("bob@example.com", types.StringMaxSize),
		&types.IntField{Value: 35},
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 75000.0},
	}
	insertStmt.AddValues(values3)

	insertPlan := NewInsertPlan(insertStmt, tid, ctx)
	_, err := insertPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
}

func TestNewSelectPlan(t *testing.T) {
	selectPlan := plan.NewSelectPlan()
	stmt := statements.NewSelectStatement(selectPlan)
	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	plan := NewSelectPlan(stmt, tid, ctx)

	if plan == nil {
		t.Fatal("NewSelectPlan returned nil")
	}

	if plan.statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.tid != tid {
		t.Error("TransactionID not properly assigned")
	}
}

func TestSelectPlan_Execute_SelectAll(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.TupleDesc == nil {
		t.Fatal("TupleDesc is nil")
	}

	if len(result.Tuples) != 3 {
		t.Errorf("Expected 3 tuples, got %d", len(result.Tuples))
	}

	if result.TupleDesc.NumFields() != 6 {
		t.Errorf("Expected 6 fields, got %d", result.TupleDesc.NumFields())
	}
}

func TestSelectPlan_Execute_WithProjection(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddProjectField("name", "")
	selectPlan.AddProjectField("email", "")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.TupleDesc.NumFields() != 2 {
		t.Errorf("Expected 2 fields after projection, got %d", result.TupleDesc.NumFields())
	}

	if len(result.Tuples) != 3 {
		t.Errorf("Expected 3 tuples, got %d", len(result.Tuples))
	}

	name, _ := result.TupleDesc.GetFieldName(0)
	if name != "name" {
		t.Errorf("Expected first field to be 'name', got '%s'", name)
	}

	email, _ := result.TupleDesc.GetFieldName(1)
	if email != "email" {
		t.Errorf("Expected second field to be 'email', got '%s'", email)
	}
}

func TestSelectPlan_Execute_WithFilter(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.active", types.Equals, "true")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 2 {
		t.Errorf("Expected 2 tuples (active=true), got %d", len(result.Tuples))
	}
}

func TestSelectPlan_Execute_WithFilterAndProjection(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.age", types.GreaterThan, "30")
	selectPlan.AddProjectField("name", "")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 tuple (age > 30), got %d", len(result.Tuples))
	}

	if result.TupleDesc.NumFields() != 1 {
		t.Errorf("Expected 1 field after projection, got %d", result.TupleDesc.NumFields())
	}

	name, _ := result.TupleDesc.GetFieldName(0)
	if name != "name" {
		t.Errorf("Expected field to be 'name', got '%s'", name)
	}
}

func TestSelectPlan_Execute_Error_NoTables(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	selectPlan := plan.NewSelectPlan()
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when no tables specified")
	}

	expectedError := "SELECT requires at least one table in FROM clause"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestSelectPlan_Execute_Error_TableNotFound(t *testing.T) {
	ctx, tid, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("nonexistent_table", "nonexistent_table")
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

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

func TestSelectPlan_Execute_Error_InvalidFilterField(t *testing.T) {
	ctx, tid, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	createSelectTestTable(t, ctx, tid)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.invalid_field", types.Equals, "value")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when filter field does not exist")
	}

	expectedError := "failed to create table scan: failed to build WHERE predicate: column invalid_field not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestSelectPlan_Execute_Error_InvalidSelectField(t *testing.T) {
	ctx, tid, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources
	createSelectTestTable(t, ctx, tid)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddProjectField("invalid_field", "")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when select field does not exist")
	}

	expectedError := "column invalid_field not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestSelectPlan_Execute_EmptyTableWithFilters(t *testing.T) {
	ctx, tid, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources
	createSelectTestTable(t, ctx, tid)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.active", types.Equals, "true")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 0 {
		t.Errorf("Expected 0 tuples from empty table, got %d", len(result.Tuples))
	}
}

func TestSelectPlan_Execute_MultipleFilters(t *testing.T) {
	ctx, tid, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	createSelectTestTable(t, ctx, tid)
	insertSelectTestData(t, ctx, tid)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.active", types.Equals, "true")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 2 {
		t.Errorf("Expected 2 tuples (active=true), got %d", len(result.Tuples))
	}
}

func TestSelectPlan_Execute_StringFilter(t *testing.T) {
	ctx, tid, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	createSelectTestTable(t, ctx, tid)
	insertSelectTestData(t, ctx, tid)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.name", types.Equals, "John Doe")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 tuple (name='John Doe'), got %d", len(result.Tuples))
	}
}

func TestSelectPlan_Execute_IntegerFilter(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.age", types.LessThan, "30")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 tuple (age < 30), got %d", len(result.Tuples))
	}
}

func TestSelectPlan_Execute_FloatFilter(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.salary", types.GreaterThanOrEqual, "60000.0")

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 2 {
		t.Errorf("Expected 2 tuples (salary >= 60000.0), got %d", len(result.Tuples))
	}
}

func TestQueryResult_Values(t *testing.T) {
	ctx, tid, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tid, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.TupleDesc == nil {
		t.Error("TupleDesc should not be nil")
	}

	if len(result.Tuples) == 0 {
		t.Error("Tuples should not be empty")
	}

	for i, tuple := range result.Tuples {
		if tuple == nil {
			t.Errorf("Tuple at index %d should not be nil", i)
		}
	}
}

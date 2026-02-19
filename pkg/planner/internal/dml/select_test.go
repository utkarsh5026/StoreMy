package dml

import (
	"os"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/plan"
	"storemy/pkg/planner/internal/ddl"
	"storemy/pkg/planner/internal/shared"
	"storemy/pkg/planner/internal/testutil"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/types"
	"testing"
)

func setupSelectTest(t *testing.T) (*registry.DatabaseContext, *transaction.TransactionContext, func()) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)

	cleanup := func() {
		os.Chdir(oldDir)
	}

	os.Mkdir("data", 0755)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	tx, _ := txRegistry.Begin()

	return ctx, tx, cleanup
}

func setupSelectTestWithData(t *testing.T) (*registry.DatabaseContext, *transaction.TransactionContext, func()) {
	ctx, tx, cleanup := setupSelectTest(t)

	createSelectTestTable(t, ctx, tx)
	insertSelectTestData(t, ctx, tx)

	return ctx, tx, cleanup
}

func executeSelectPlan(t *testing.T, plan *SelectPlan) (*shared.SelectQueryResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*shared.SelectQueryResult)
	if !ok {
		t.Fatalf("Result is not a result.SelectQueryResult, got %T", resultAny)
	}

	return result, nil
}

func createSelectTestTable(t *testing.T, ctx *registry.DatabaseContext, tx *transaction.TransactionContext) {
	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, false, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("email", types.StringType, false, nil)
	stmt.AddField("age", types.IntType, false, nil)
	stmt.AddField("active", types.BoolType, false, nil)
	stmt.AddField("salary", types.FloatType, false, nil)

	createPlan := ddl.NewCreateTablePlan(stmt, ctx, tx)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Register cleanup to close table file
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", tx.ID)
}

func insertSelectTestData(t *testing.T, ctx *registry.DatabaseContext, tx *transaction.TransactionContext) {
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

	insertPlan := NewInsertPlan(insertStmt, tx, ctx)
	_, err := insertPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}
}

func TestNewSelectPlan(t *testing.T) {
	dataDir := testutil.SetupTestDataDir(t)
	selectPlan := plan.NewSelectPlan()
	stmt := statements.NewSelectStatement(selectPlan)
	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	tx, _ := txRegistry.Begin()

	plan := NewSelectPlan(stmt, tx, ctx)

	if plan == nil {
		t.Fatal("NewSelectPlan returned nil")
	}

	if plan.statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.tx != tx {
		t.Error("TransactionID not properly assigned")
	}
}

func TestSelectPlan_Execute_SelectAll(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddProjectField("name", "")
	selectPlan.AddProjectField("email", "")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.active", primitives.Equals, "true")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.age", primitives.GreaterThan, "30")
	selectPlan.AddProjectField("name", "")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	dataDir := testutil.SetupTestDataDir(t)

	ctx, txRegistry := testutil.CreateTestContextWithCleanup(t, dataDir)
	tx, _ := txRegistry.Begin()

	selectPlan := plan.NewSelectPlan()
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("nonexistent_table", "nonexistent_table")
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	createSelectTestTable(t, ctx, tx)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.invalid_field", primitives.Equals, "value")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources
	createSelectTestTable(t, ctx, tx)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddProjectField("invalid_field", "")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources
	createSelectTestTable(t, ctx, tx)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.active", primitives.Equals, "true")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	createSelectTestTable(t, ctx, tx)
	insertSelectTestData(t, ctx, tx)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.active", primitives.Equals, "true")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTest(t)
	defer cleanup() // Always clean up resources

	createSelectTestTable(t, ctx, tx)
	insertSelectTestData(t, ctx, tx)

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.name", primitives.Equals, "John Doe")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.age", primitives.LessThan, "30")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	selectPlan.AddFilter("users.salary", primitives.GreaterThanOrEqual, "60000.0")

	planInstance := NewSelectPlan(stmt, tx, ctx)

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

func TestSelectQueryResult_Values(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup() // Always clean up resources

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	stmt := statements.NewSelectStatement(selectPlan)

	planInstance := NewSelectPlan(stmt, tx, ctx)

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

// Helper functions for join and aggregation tests

func setupJoinTestWithData(t *testing.T) (*registry.DatabaseContext, *transaction.TransactionContext, func()) {
	ctx, tx, cleanup := setupSelectTest(t)

	createJoinTestTables(t, ctx, tx)
	insertJoinTestData(t, ctx, tx)

	return ctx, tx, cleanup
}

func createJoinTestTables(t *testing.T, ctx *registry.DatabaseContext, tx *transaction.TransactionContext) {
	// Create users table
	usersStmt := statements.NewCreateStatement("users", false)
	usersStmt.AddField("id", types.IntType, false, nil)
	usersStmt.AddField("name", types.StringType, false, nil)
	usersStmt.AddField("dept_id", types.IntType, false, nil)

	usersPlan := ddl.NewCreateTablePlan(usersStmt, ctx, tx)
	_, err := usersPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}
	testutil.CleanupTable(t, ctx.CatalogManager(), "users", tx.ID)

	// Create departments table
	deptsStmt := statements.NewCreateStatement("departments", false)
	deptsStmt.AddField("id", types.IntType, false, nil)
	deptsStmt.AddField("dept_name", types.StringType, false, nil)

	deptsPlan := ddl.NewCreateTablePlan(deptsStmt, ctx, tx)
	_, err = deptsPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create departments table: %v", err)
	}
	testutil.CleanupTable(t, ctx.CatalogManager(), "departments", tx.ID)
}

func insertJoinTestData(t *testing.T, ctx *registry.DatabaseContext, tx *transaction.TransactionContext) {
	// Insert users
	usersStmt := statements.NewInsertStatement("users")
	usersStmt.AddValues([]types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("Alice", types.StringMaxSize),
		&types.IntField{Value: 10},
	})
	usersStmt.AddValues([]types.Field{
		&types.IntField{Value: 2},
		types.NewStringField("Bob", types.StringMaxSize),
		&types.IntField{Value: 20},
	})
	usersStmt.AddValues([]types.Field{
		&types.IntField{Value: 3},
		types.NewStringField("Charlie", types.StringMaxSize),
		&types.IntField{Value: 10},
	})

	usersPlan := NewInsertPlan(usersStmt, tx, ctx)
	_, err := usersPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to insert users: %v", err)
	}

	// Insert departments
	deptsStmt := statements.NewInsertStatement("departments")
	deptsStmt.AddValues([]types.Field{
		&types.IntField{Value: 10},
		types.NewStringField("Engineering", types.StringMaxSize),
	})
	deptsStmt.AddValues([]types.Field{
		&types.IntField{Value: 20},
		types.NewStringField("Sales", types.StringMaxSize),
	})

	deptsPlan := NewInsertPlan(deptsStmt, tx, ctx)
	_, err = deptsPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to insert departments: %v", err)
	}
}

// Join Tests

func TestSelectPlan_Execute_SimpleJoin(t *testing.T) {
	ctx, tx, cleanup := setupJoinTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "u")

	deptScan := plan.NewScanNode("departments", "d")
	selectPlan.AddJoin(deptScan, plan.InnerJoin, "u.dept_id", "d.id", primitives.Equals)

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should have 3 joined tuples (all users match departments)
	if len(result.Tuples) != 3 {
		t.Errorf("Expected 3 joined tuples, got %d", len(result.Tuples))
	}

	// Result should have fields from both tables (3 from users + 2 from departments = 5)
	if result.TupleDesc.NumFields() != 5 {
		t.Errorf("Expected 5 fields in joined result, got %d", result.TupleDesc.NumFields())
	}
}

func TestSelectPlan_Execute_JoinWithProjection(t *testing.T) {
	ctx, tx, cleanup := setupJoinTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "u")

	deptScan := plan.NewScanNode("departments", "d")
	selectPlan.AddJoin(deptScan, plan.InnerJoin, "u.dept_id", "d.id", primitives.Equals)

	selectPlan.AddProjectField("name", "")
	selectPlan.AddProjectField("dept_name", "")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 3 {
		t.Errorf("Expected 3 joined tuples, got %d", len(result.Tuples))
	}

	// Should only have the 2 projected fields
	if result.TupleDesc.NumFields() != 2 {
		t.Errorf("Expected 2 fields after projection, got %d", result.TupleDesc.NumFields())
	}
}

// Aggregation Tests

func TestSelectPlan_Execute_AggregationCount(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	selectPlan.AddProjectField("id", "COUNT")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should return 1 tuple with the count
	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 aggregate result, got %d", len(result.Tuples))
	}

	// Verify the count value is 3
	if len(result.Tuples) > 0 {
		countField, err := result.Tuples[0].GetField(0)
		if err != nil {
			t.Fatalf("Failed to get count field: %v", err)
		}

		intField, ok := countField.(*types.IntField)
		if !ok {
			t.Fatalf("Expected IntField, got %T", countField)
		}

		if intField.Value != 3 {
			t.Errorf("Expected count of 3, got %d", intField.Value)
		}
	}
}

func TestSelectPlan_Execute_AggregationSum(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	selectPlan.AddProjectField("age", "SUM")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 aggregate result, got %d", len(result.Tuples))
	}

	// Verify sum: 25 + 30 + 35 = 90
	if len(result.Tuples) > 0 {
		sumField, err := result.Tuples[0].GetField(0)
		if err != nil {
			t.Fatalf("Failed to get sum field: %v", err)
		}

		intField, ok := sumField.(*types.IntField)
		if !ok {
			t.Fatalf("Expected IntField, got %T", sumField)
		}

		if intField.Value != 90 {
			t.Errorf("Expected sum of 90, got %d", intField.Value)
		}
	}
}

func TestSelectPlan_Execute_AggregationWithGroupBy(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	selectPlan.AddProjectField("id", "COUNT")
	selectPlan.SetGroupBy("name")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	// Should return 3 groups (one for each user name)
	if len(result.Tuples) != 3 {
		t.Errorf("Expected 3 groups, got %d", len(result.Tuples))
	}

	// Result should have 2 fields: group field and aggregate field
	if result.TupleDesc.NumFields() != 2 {
		t.Errorf("Expected 2 fields (group + aggregate), got %d", result.TupleDesc.NumFields())
	}
}

func TestSelectPlan_Execute_AggregationMax(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	selectPlan.AddProjectField("salary", "MAX")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 aggregate result, got %d", len(result.Tuples))
	}

	// Verify max salary is 75000.0
	if len(result.Tuples) > 0 {
		maxField, err := result.Tuples[0].GetField(0)
		if err != nil {
			t.Fatalf("Failed to get max field: %v", err)
		}

		floatField, ok := maxField.(*types.Float64Field)
		if !ok {
			t.Fatalf("Expected Float64Field, got %T", maxField)
		}

		if floatField.Value != 75000.0 {
			t.Errorf("Expected max of 75000.0, got %f", floatField.Value)
		}
	}
}

func TestSelectPlan_Execute_AggregationMin(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	selectPlan.AddProjectField("age", "MIN")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 aggregate result, got %d", len(result.Tuples))
	}

	// Verify min age is 25
	if len(result.Tuples) > 0 {
		minField, err := result.Tuples[0].GetField(0)
		if err != nil {
			t.Fatalf("Failed to get min field: %v", err)
		}

		intField, ok := minField.(*types.IntField)
		if !ok {
			t.Fatalf("Expected IntField, got %T", minField)
		}

		if intField.Value != 25 {
			t.Errorf("Expected min of 25, got %d", intField.Value)
		}
	}
}

func TestSelectPlan_Execute_AggregationAvg(t *testing.T) {
	ctx, tx, cleanup := setupSelectTestWithData(t)
	defer cleanup()

	selectPlan := plan.NewSelectPlan()
	selectPlan.AddScan("users", "users")
	selectPlan.AddProjectField("age", "AVG")

	stmt := statements.NewSelectStatement(selectPlan)
	planInstance := NewSelectPlan(stmt, tx, ctx)

	result, err := executeSelectPlan(t, planInstance)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if len(result.Tuples) != 1 {
		t.Errorf("Expected 1 aggregate result, got %d", len(result.Tuples))
	}

	// Verify avg: (25 + 30 + 35) / 3 = 30
	if len(result.Tuples) > 0 {
		avgField, err := result.Tuples[0].GetField(0)
		if err != nil {
			t.Fatalf("Failed to get avg field: %v", err)
		}

		intField, ok := avgField.(*types.IntField)
		if !ok {
			t.Fatalf("Expected IntField, got %T", avgField)
		}

		if intField.Value != 30 {
			t.Errorf("Expected avg of 30, got %d", intField.Value)
		}
	}
}

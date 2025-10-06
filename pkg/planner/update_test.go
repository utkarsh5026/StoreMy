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

func executeUpdatePlan(t *testing.T, plan *UpdatePlan) (*DMLResult, error) {
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

func createUpdateTestTable(t *testing.T, ctx *registry.DatabaseContext, tid *transaction.TransactionID) {
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

func insertUpdateTestData(t *testing.T, ctx *registry.DatabaseContext, tid *transaction.TransactionID) {
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

func TestNewUpdatePlan(t *testing.T) {
	stmt := statements.NewUpdateStatement("users", "users")
	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	plan := NewUpdatePlan(stmt, tid, ctx)

	if plan == nil {
		t.Fatal("NewUpdatePlan returned nil")
	}

	if plan.statement != stmt {
		t.Error("Statement not properly assigned")
	}
	if plan.tid != tid {
		t.Error("TransactionID not properly assigned")
	}
}

func TestUpdatePlan_Execute_UpdateSingleField(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("name", types.NewStringField("Updated Name", types.StringMaxSize))

	filter := plan.NewFilterNode("users", "users.id", types.Equals, "1")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	expectedMessage := "1 row(s) updated"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestUpdatePlan_Execute_UpdateMultipleFields(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("name", types.NewStringField("Updated Name", types.StringMaxSize))
	stmt.AddSetClause("age", &types.IntField{Value: 26})
	stmt.AddSetClause("salary", &types.Float64Field{Value: 55000.0})

	filter := plan.NewFilterNode("users", "users.id", types.Equals, "1")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)
	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	expectedMessage := "1 row(s) updated"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestUpdatePlan_Execute_UpdateMultipleRows(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("active", &types.BoolField{Value: false})

	filter := plan.NewFilterNode("users", "users.active", types.Equals, "true")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "2 row(s) updated"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestUpdatePlan_Execute_UpdateAllRows(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("email", types.NewStringField("updated@example.com", types.StringMaxSize))

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 3 {
		t.Errorf("Expected 3 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "3 row(s) updated"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestUpdatePlan_Execute_UpdateWithIntegerFilter(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("salary", &types.Float64Field{Value: 80000.0})

	filter := plan.NewFilterNode("users", "users.age", types.GreaterThan, "30")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected (age > 30), got %d", result.RowsAffected)
	}
}

func TestUpdatePlan_Execute_UpdateWithFloatFilter(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("active", &types.BoolField{Value: true})

	filter := plan.NewFilterNode("users", "users.salary", types.LessThan, "65000.0")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected (salary < 65000.0), got %d", result.RowsAffected)
	}
}

func TestUpdatePlan_Execute_NoMatchingRows(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)
	insertUpdateTestData(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("name", types.NewStringField("No Match", types.StringMaxSize))

	filter := plan.NewFilterNode("users", "users.age", types.GreaterThan, "100")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "0 row(s) updated"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestUpdatePlan_Execute_Error_TableNotFound(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	stmt := statements.NewUpdateStatement("nonexistent_table", "nonexistent_table")
	stmt.AddSetClause("name", types.NewStringField("Test", types.StringMaxSize))

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

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

func TestUpdatePlan_Execute_Error_InvalidField(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("invalid_field", types.NewStringField("Test", types.StringMaxSize))

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when field does not exist")
	}

	expectedError := "column invalid_field not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestUpdatePlan_Execute_Error_InvalidWhereField(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("name", types.NewStringField("Test", types.StringMaxSize))

	filter := plan.NewFilterNode("users", "users.invalid_field", types.Equals, "value")
	stmt.SetWhereClause(filter)

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when WHERE field does not exist")
	}

	expectedError := "failed to build WHERE predicate: field users.invalid_field not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestUpdatePlan_Execute_EmptyTable(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("name", types.NewStringField("Test", types.StringMaxSize))

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	result, err := executeUpdatePlan(t, updatePlan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected on empty table, got %d", result.RowsAffected)
	}

	expectedMessage := "0 row(s) updated"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestUpdatePlan_getTableMetadata(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	tableID, tupleDesc, err := updatePlan.getTableMetadata()

	if err != nil {
		t.Fatalf("getTableMetadata failed: %v", err)
	}

	expectedTableID, _ := ctx.TableManager().GetTableID("users")
	if tableID != expectedTableID {
		t.Errorf("Expected table ID %d, got %d", expectedTableID, tableID)
	}

	if tupleDesc == nil {
		t.Fatal("TupleDesc is nil")
	}

	if tupleDesc.NumFields() != 6 {
		t.Errorf("Expected 6 fields, got %d", tupleDesc.NumFields())
	}
}

func TestUpdatePlan_findFieldIndex(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	_, tupleDesc, err := updatePlan.getTableMetadata()
	if err != nil {
		t.Fatalf("getTableMetadata failed: %v", err)
	}

	tests := []struct {
		fieldName     string
		expectedIndex int
		expectError   bool
	}{
		{"id", 0, false},
		{"name", 1, false},
		{"email", 2, false},
		{"age", 3, false},
		{"active", 4, false},
		{"salary", 5, false},
		{"invalid_field", -1, true},
	}

	for _, test := range tests {
		index, err := updatePlan.findFieldIndex(test.fieldName, tupleDesc)

		if test.expectError {
			if err == nil {
				t.Errorf("Expected error for field %s, but got none", test.fieldName)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for field %s: %v", test.fieldName, err)
			}
			if index != test.expectedIndex {
				t.Errorf("Expected index %d for field %s, got %d", test.expectedIndex, test.fieldName, index)
			}
		}
	}
}

func TestUpdatePlan_buildUpdateMap(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, "")
	tid := transaction.NewTransactionID()

	createUpdateTestTable(t, ctx, tid)

	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("name", types.NewStringField("New Name", types.StringMaxSize))
	stmt.AddSetClause("age", &types.IntField{Value: 99})

	updatePlan := NewUpdatePlan(stmt, tid, ctx)

	_, tupleDesc, err := updatePlan.getTableMetadata()
	if err != nil {
		t.Fatalf("getTableMetadata failed: %v", err)
	}

	updateMap, err := updatePlan.buildUpdateMap(tupleDesc)

	if err != nil {
		t.Fatalf("buildUpdateMap failed: %v", err)
	}

	if len(updateMap) != 2 {
		t.Errorf("Expected update map length 2, got %d", len(updateMap))
	}

	if _, exists := updateMap[1]; !exists {
		t.Error("Expected name field (index 1) in update map")
	}

	if _, exists := updateMap[3]; !exists {
		t.Error("Expected age field (index 3) in update map")
	}
}

func TestSetClause_Values(t *testing.T) {
	setClause := statements.SetClause{
		FieldName: "test_field",
		Value:     types.NewStringField("test_value", types.StringMaxSize),
	}

	if setClause.FieldName != "test_field" {
		t.Errorf("Expected FieldName 'test_field', got '%s'", setClause.FieldName)
	}

	if setClause.Value == nil {
		t.Error("Expected Value to be non-nil")
	}
}

func TestUpdateStatement_String(t *testing.T) {
	stmt := statements.NewUpdateStatement("users", "u")
	stmt.AddSetClause("name", types.NewStringField("John", types.StringMaxSize))
	stmt.AddSetClause("age", &types.IntField{Value: 30})

	filter := plan.NewFilterNode("u", "u.id", types.Equals, "1")
	stmt.SetWhereClause(filter)

	str := stmt.String()
	expected := "UPDATE users u SET name = John, age = 30 WHERE Filter[u.u.id = 1]"
	if str != expected {
		t.Errorf("Expected string %q, got %q", expected, str)
	}
}

func TestUpdateStatement_WithoutWhere(t *testing.T) {
	stmt := statements.NewUpdateStatement("users", "users")
	stmt.AddSetClause("active", &types.BoolField{Value: true})

	str := stmt.String()
	expected := "UPDATE users SET active = true"
	if str != expected {
		t.Errorf("Expected string %q, got %q", expected, str)
	}
}

func TestUpdateStatement_WithoutAlias(t *testing.T) {
	stmt := statements.NewUpdateStatement("users", "")
	stmt.AddSetClause("name", types.NewStringField("Test", types.StringMaxSize))

	str := stmt.String()
	expected := "UPDATE users SET name = Test"
	if str != expected {
		t.Errorf("Expected string %q, got %q", expected, str)
	}
}

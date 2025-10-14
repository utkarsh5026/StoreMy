package planner

import (
	"os"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/types"
	"testing"
)

// Helper function to execute insert plan and cast result to DMLResult
func executeInsertPlan(t *testing.T, plan *InsertPlan) (*DMLResult, error) {
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

// Helper function to create a test table
func createTestTable(t *testing.T, ctx *registry.DatabaseContext, tx TransactionCtx) {
	stmt := statements.NewCreateStatement("test_table", false)
	stmt.AddField("id", types.IntType, false, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("active", types.BoolType, false, nil)
	stmt.AddField("price", types.FloatType, false, nil)

	createPlan := NewCreateTablePlan(stmt, ctx, tx)
	_, err := createPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Register cleanup to close table file
	cleanupTable(t, ctx.CatalogManager(), "test_table", tx.ID)
}

func TestNewInsertPlan(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewInsertStatement("test_table")
	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	if plan == nil {
		t.Fatal("NewInsertPlan returned nil")
	}

	if plan.statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.transactionCtx != transCtx {
		t.Error("TransactionID not properly assigned")
	}
}

func TestInsertPlan_Execute_SingleRow(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	values := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 99.99},
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}

	expectedMessage := "1 row(s) inserted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestInsertPlan_Execute_MultipleRows(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")

	values1 := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 99.99},
	}
	stmt.AddValues(values1)

	values2 := []types.Field{
		&types.IntField{Value: 2},
		types.NewStringField("Jane", types.StringMaxSize),
		&types.BoolField{Value: false},
		&types.Float64Field{Value: 149.99},
	}
	stmt.AddValues(values2)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 2 {
		t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "2 row(s) inserted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestInsertPlan_Execute_WithSpecificFields(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	stmt.AddFieldNames([]string{"id", "name", "active", "price"})

	values := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 99.99},
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 1 {
		t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
	}
}

func TestInsertPlan_Execute_Error_TableNotFound(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	stmt := statements.NewInsertStatement("nonexistent_table")
	values := []types.Field{
		&types.IntField{Value: 1},
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

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

func TestInsertPlan_Execute_Error_ValueCountMismatch(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	values := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when value count doesn't match field count")
	}

	expectedError := "value count mismatch: expected 4, got 2"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestInsertPlan_Execute_Error_InvalidFieldName(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	stmt.AddFieldNames([]string{"id", "invalid_field"})

	values := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when field name is invalid")
	}

	expectedError := "column invalid_field not found"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestInsertPlan_Execute_Error_ValueCountMismatchWithFields(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	stmt.AddFieldNames([]string{"id", "name"})

	values := []types.Field{
		&types.IntField{Value: 1},
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when value count doesn't match specified field count")
	}

	expectedError := "value count mismatch: expected 2, got 1"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}
}

func TestInsertPlan_Execute_Error_MissingValueForField(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	stmt.AddFieldNames([]string{"id", "price"})

	values := []types.Field{
		&types.IntField{Value: 1},
		&types.Float64Field{Value: 99.99},
	}
	stmt.AddValues(values)

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when missing value for required field")
	}

	if err.Error() != "missing value for field index 1" && err.Error() != "missing value for field index 2" {
		t.Errorf("Expected error about missing field value, got %q", err.Error())
	}
}

func TestInsertPlan_Execute_EmptyValues(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")

	plan := NewInsertPlan(stmt, transCtx, ctx)

	result, err := executeInsertPlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result.RowsAffected != 0 {
		t.Errorf("Expected 0 rows affected, got %d", result.RowsAffected)
	}

	expectedMessage := "0 row(s) inserted"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}
}

func TestInsertPlan_getTableID(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")

	tableID, err := resolveTableID(stmt.TableName, transCtx, ctx)

	if err != nil {
		t.Fatalf("getTableID failed: %v", err)
	}

	expectedTableID, _ := ctx.CatalogManager().GetTableID(transCtx, "test_table")
	if tableID != expectedTableID {
		t.Errorf("Expected table ID %d, got %d", expectedTableID, tableID)
	}
}

func TestInsertPlan_getTupleDesc(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")

	md, err := resolveTableMetadata(stmt.TableName, transCtx, ctx)
	tupleDesc := md.TupleDesc

	if err != nil {
		t.Fatalf("getTupleDesc failed: %v", err)
	}

	if tupleDesc == nil {
		t.Fatal("TupleDesc is nil")
	}

	if tupleDesc.NumFields() != 4 {
		t.Errorf("Expected 4 fields, got %d", tupleDesc.NumFields())
	}
}

func TestInsertPlan_createFieldMapping(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	stmt.AddFieldNames([]string{"name", "id"})
	plan := NewInsertPlan(stmt, transCtx, ctx)

	md, err := resolveTableMetadata(stmt.TableName, transCtx, ctx)
	tupleDesc := md.TupleDesc
	if err != nil {
		t.Fatalf("getTupleDesc failed: %v", err)
	}

	fieldMapping, err := plan.createFieldMapping(tupleDesc)

	if err != nil {
		t.Fatalf("createFieldMapping failed: %v", err)
	}

	if len(fieldMapping) != 2 {
		t.Errorf("Expected field mapping length 2, got %d", len(fieldMapping))
	}

	if fieldMapping[0] != 1 {
		t.Errorf("Expected field mapping[0] = 1 (name field index), got %d", fieldMapping[0])
	}

	if fieldMapping[1] != 0 {
		t.Errorf("Expected field mapping[1] = 0 (id field index), got %d", fieldMapping[1])
	}
}

func TestInsertPlan_createFieldMapping_EmptyFields(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")
	plan := NewInsertPlan(stmt, transCtx, ctx)

	md, err := resolveTableMetadata(stmt.TableName, transCtx, ctx)
	tupleDesc := md.TupleDesc
	if err != nil {
		t.Fatalf("getTupleDesc failed: %v", err)
	}

	fieldMapping, err := plan.createFieldMapping(tupleDesc)

	if err != nil {
		t.Fatalf("createFieldMapping failed: %v", err)
	}

	if fieldMapping != nil {
		t.Error("Expected field mapping to be nil when no fields specified")
	}
}

func TestInsertPlan_validateValueCount(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	createTestTable(t, ctx, transCtx)

	stmt := statements.NewInsertStatement("test_table")

	md, err := resolveTableMetadata(stmt.TableName, transCtx, ctx)
	tupleDesc := md.TupleDesc
	if err != nil {
		t.Fatalf("getTupleDesc failed: %v", err)
	}

	values := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
		&types.BoolField{Value: true},
		&types.Float64Field{Value: 99.99},
	}

	err = validateValueCount(values, tupleDesc, nil, nil)
	if err != nil {
		t.Errorf("validateValueCount failed: %v", err)
	}

	fieldMapping := []int{0, 1}
	values2 := []types.Field{
		&types.IntField{Value: 1},
		types.NewStringField("John", types.StringMaxSize),
	}

	err = validateValueCount(values2, tupleDesc, fieldMapping, nil)
	if err != nil {
		t.Errorf("validateValueCount with field mapping failed: %v", err)
	}
}

func TestDMLResult_String(t *testing.T) {
	result := &DMLResult{
		RowsAffected: 5,
		Message:      "Test message",
	}

	expected := "5 row(s) affected: Test message"
	actual := result.String()
	if actual != expected {
		t.Errorf("Expected string representation %q, got %q", expected, actual)
	}
}

func TestDMLResult_Values(t *testing.T) {
	tests := []struct {
		name         string
		rowsAffected int
		message      string
	}{
		{"Single row", 1, "1 row(s) inserted"},
		{"Multiple rows", 5, "5 row(s) inserted"},
		{"Zero rows", 0, "0 row(s) inserted"},
		{"Custom message", 3, "Custom operation message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DMLResult{
				RowsAffected: tt.rowsAffected,
				Message:      tt.message,
			}

			if result.RowsAffected != tt.rowsAffected {
				t.Errorf("Expected RowsAffected %d, got %d", tt.rowsAffected, result.RowsAffected)
			}

			if result.Message != tt.message {
				t.Errorf("Expected Message %q, got %q", tt.message, result.Message)
			}
		})
	}
}

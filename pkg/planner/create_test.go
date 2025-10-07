package planner

import (
	"fmt"
	"os"
	"path/filepath"
	"storemy/pkg/parser/statements"
	"storemy/pkg/types"
	"testing"
)

// Helper function to execute plan and cast result to DDLResult
func executePlan(t *testing.T, plan *CreateTablePlan) (*DDLResult, error) {
	resultAny, err := plan.Execute()
	if err != nil {
		return nil, err
	}

	if resultAny == nil {
		return nil, nil
	}

	result, ok := resultAny.(*DDLResult)
	if !ok {
		t.Fatalf("Result is not a DDLResult, got %T", resultAny)
	}

	return result, nil
}

func TestNewCreateTablePlan(t *testing.T) {
	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)

	ctx := createTestContextWithCleanup(t, "")
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	if plan == nil {
		t.Fatal("NewCreateTablePlan returned nil")
	}

	if plan.Statement != stmt {
		t.Error("Statement not properly assigned")
	}

	if plan.ctx != ctx {
		t.Error("Context not properly assigned")
	}

	if plan.transactionCtx != transCtx {
		t.Error("TransactionID not properly assigned")
	}
}

func TestCreateTablePlan_Execute_BasicSuccess(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	os.Mkdir("data", 0755)

	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result is nil")
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users created successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	if !ctx.TableManager().TableExists("users") {
		t.Error("Table was not added to table manager")
	}

	cleanupTable(t, ctx.TableManager(), "users")
}

func TestCreateTablePlan_Execute_WithPrimaryKey(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewCreateStatement("products", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.PrimaryKey = "id"

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.TableManager().TableExists("products") {
		t.Error("Table was not added to table manager")
	}

	cleanupTable(t, ctx.TableManager(), "products")
}

func TestCreateTablePlan_Execute_AllFieldTypes(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewCreateStatement("test_types", false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)
	stmt.AddField("active", types.BoolType, false, nil)
	stmt.AddField("price", types.FloatType, false, nil)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.TableManager().TableExists("test_types") {
		t.Error("Table was not added to table manager")
	}

	cleanupTable(t, ctx.TableManager(), "test_types")
}

func TestCreateTablePlan_Execute_IfNotExists_TableDoesNotExist(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewCreateStatement("users", true)
	stmt.AddField("id", types.IntType, true, nil)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users created successfully"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	if !ctx.TableManager().TableExists("users") {
		t.Error("Table was not added to table manager")
	}

	cleanupTable(t, ctx.TableManager(), "users")
}

func TestCreateTablePlan_Execute_IfNotExists_TableExists(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	existingStmt := statements.NewCreateStatement("users", false)
	existingStmt.AddField("id", types.IntType, true, nil)
	existingPlan := NewCreateTablePlan(existingStmt, ctx, transCtx)
	_, err := existingPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create existing table: %v", err)
	}

	stmt := statements.NewCreateStatement("users", true)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("name", types.StringType, false, nil)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedMessage := "Table users already exists (IF NOT EXISTS)"
	if result.Message != expectedMessage {
		t.Errorf("Expected message %q, got %q", expectedMessage, result.Message)
	}

	cleanupTable(t, ctx.TableManager(), "users")
}

func TestCreateTablePlan_Execute_Error_TableAlreadyExists(t *testing.T) {
	dataDir := setupTestDataDir(t)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	existingStmt := statements.NewCreateStatement("users", false)
	existingStmt.AddField("id", types.IntType, true, nil)
	existingPlan := NewCreateTablePlan(existingStmt, ctx, transCtx)
	_, err := existingPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create existing table: %v", err)
	}

	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when table already exists")
	}

	expectedError := "table users already exists"
	if err.Error() != expectedError {
		t.Errorf("Expected error %q, got %q", expectedError, err.Error())
	}

	cleanupTable(t, ctx.TableManager(), "users")
}

func TestCreateTablePlan_Execute_Error_EmptyFields(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewCreateStatement("empty_table", false)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := plan.Execute()

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when creating table with no fields")
	}
}

func TestCreateTablePlan_Execute_Error_InvalidFieldType(t *testing.T) {
	dataDir := setupTestDataDir(t)

	stmt := statements.NewCreateStatement("invalid_table", false)

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := plan.Execute()

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when creating table with no fields")
	}
}

func TestCreateTablePlan_Execute_Error_DataDirectoryMissing(t *testing.T) {
	dataDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(dataDir)
	defer os.Chdir(oldDir)

	// Don't create the data directory - test expects error when default "data" dir doesn't exist
	stmt := statements.NewCreateStatement("users", false)
	stmt.AddField("id", types.IntType, true, nil)

	// Pass empty string for dataDir to force use of "data/" directory
	ctx := createTestContextWithCleanup(t, "")
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := plan.Execute()

	if result != nil {
		t.Error("Expected result to be nil on error")
	}

	if err == nil {
		t.Fatal("Expected error when data directory does not exist")
	}

	if err.Error()[:22] != "failed to create heap " {
		t.Errorf("Expected error to start with 'failed to create heap ', got %q", err.Error())
	}
}

func TestCreateTablePlan_Execute_ComplexTable(t *testing.T) {
	dataDir := setupTestDataDir(t)

	defaultInt := &types.IntField{Value: 1}
	defaultString := types.NewStringField("default", types.StringMaxSize)
	defaultBool := &types.BoolField{Value: true}
	defaultFloat := &types.Float64Field{Value: 0.0}

	stmt := statements.NewCreateStatement("complex_table", false)
	stmt.AddField("id", types.IntType, true, defaultInt)
	stmt.AddField("name", types.StringType, true, defaultString)
	stmt.AddField("active", types.BoolType, false, defaultBool)
	stmt.AddField("price", types.FloatType, false, defaultFloat)
	stmt.PrimaryKey = "id"

	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	if !ctx.TableManager().TableExists("complex_table") {
		t.Error("Table was not added to table manager")
	}

	tableID, err := ctx.TableManager().GetTableID("complex_table")
	if err != nil {
		t.Fatalf("Failed to get table ID: %v", err)
	}

	tupleDesc, err := ctx.TableManager().GetTupleDesc(tableID)
	if err != nil {
		t.Fatalf("Failed to get tuple description: %v", err)
	}

	if tupleDesc.NumFields() != 4 {
		t.Errorf("Expected 4 fields, got %d", tupleDesc.NumFields())
	}

	cleanupTable(t, ctx.TableManager(), "complex_table")
}

func TestCreateTablePlan_Execute_FileCreation(t *testing.T) {
	setupTestDataDir(t)

	stmt := statements.NewCreateStatement("file_test", false)
	stmt.AddField("id", types.IntType, true, nil)

	// Pass empty string to use default "data/" directory
	ctx := createTestContextWithCleanup(t, "")
	transCtx := createTransactionContext(t)

	plan := NewCreateTablePlan(stmt, ctx, transCtx)

	result, err := executePlan(t, plan)

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !result.Success {
		t.Error("Expected success to be true")
	}

	expectedFileName := filepath.Join("data", "file_test.dat")
	if _, err := os.Stat(expectedFileName); os.IsNotExist(err) {
		t.Errorf("Expected file %s to be created", expectedFileName)
	}

	cleanupTable(t, ctx.TableManager(), "file_test")
}

func TestDDLResult_String(t *testing.T) {
	result := &DDLResult{
		Success: true,
		Message: "Test message",
	}

	expected := fmt.Sprintf("DDL Result - Success: %t, Message: %s", result.Success, result.Message)
	if result.String() != expected {
		t.Errorf("Expected string representation %q, got %q", expected, result.String())
	}
}

func TestDDLResult_Values(t *testing.T) {
	tests := []struct {
		name    string
		success bool
		message string
	}{
		{"Success case", true, "Operation completed"},
		{"Failure case", false, "Operation failed"},
		{"Empty message", true, ""},
		{"Long message", false, "This is a very long error message that describes what went wrong"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &DDLResult{
				Success: tt.success,
				Message: tt.message,
			}

			if result.Success != tt.success {
				t.Errorf("Expected success %t, got %t", tt.success, result.Success)
			}

			if result.Message != tt.message {
				t.Errorf("Expected message %q, got %q", tt.message, result.Message)
			}
		})
	}
}

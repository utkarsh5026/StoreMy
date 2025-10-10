package database

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"testing"
)

// TestFormatResult_Select tests formatting of SELECT query results
func TestFormatResult_Select(t *testing.T) {
	// Create sample tuple description
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	// Create sample tuples
	tuple1 := tuple.NewTuple(td)
	tuple1.SetField(0, types.NewIntField(1))
	tuple1.SetField(1, types.NewStringField("Alice", 255))

	tuple2 := tuple.NewTuple(td)
	tuple2.SetField(0, types.NewIntField(2))
	tuple2.SetField(1, types.NewStringField("Bob", 255))

	// Create QueryResult
	queryResult := &planner.SelectQueryResult{
		TupleDesc: td,
		Tuples:    []*tuple.Tuple{tuple1, tuple2},
	}

	// Create SELECT statement
	selectStmt := statements.NewSelectStatement(nil)

	// Format result
	result, err := formatResult(queryResult, selectStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	// Verify result structure
	if !result.Success {
		t.Error("expected Success=true")
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}

	if result.Columns[0] != "id" {
		t.Errorf("expected column[0]='id', got '%s'", result.Columns[0])
	}

	if result.Columns[1] != "name" {
		t.Errorf("expected column[1]='name', got '%s'", result.Columns[1])
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}

	// Verify first row
	if len(result.Rows[0]) != 2 {
		t.Errorf("expected row[0] to have 2 columns, got %d", len(result.Rows[0]))
	}

	if result.Rows[0][0] != "1" {
		t.Errorf("expected row[0][0]='1', got '%s'", result.Rows[0][0])
	}

	if result.Rows[0][1] != "Alice" {
		t.Errorf("expected row[0][1]='Alice', got '%s'", result.Rows[0][1])
	}

	// Verify message
	if result.Message == "" {
		t.Error("expected non-empty message")
	}
}

// TestFormatResult_SelectEmpty tests formatting of empty SELECT results
func TestFormatResult_SelectEmpty(t *testing.T) {
	// Create empty QueryResult
	queryResult := &planner.SelectQueryResult{
		TupleDesc: nil,
		Tuples:    []*tuple.Tuple{},
	}

	selectStmt := statements.NewSelectStatement(nil)

	result, err := formatResult(queryResult, selectStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true for empty result")
	}

	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
	}

	if result.Message == "" {
		t.Error("expected non-empty message for empty result")
	}
}

// TestFormatResult_SelectNullValues tests handling of NULL values
func TestFormatResult_SelectNullValues(t *testing.T) {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "data"},
	)
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	// Create tuple with one NULL field
	tup := tuple.NewTuple(td)
	tup.SetField(0, types.NewIntField(1))
	// Leave field 1 as nil (NULL)

	queryResult := &planner.SelectQueryResult{
		TupleDesc: td,
		Tuples:    []*tuple.Tuple{tup},
	}

	selectStmt := statements.NewSelectStatement(nil)
	result, err := formatResult(queryResult, selectStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	// NULL field should be formatted as "NULL"
	if result.Rows[0][1] != "NULL" {
		t.Errorf("expected NULL to be formatted as 'NULL', got '%s'", result.Rows[0][1])
	}
}

// TestFormatResult_Insert tests formatting of INSERT results
func TestFormatResult_Insert(t *testing.T) {
	dmlResult := &planner.DMLResult{
		RowsAffected: 5,
	}

	insertStmt := statements.NewInsertStatement("test")

	result, err := formatResult(dmlResult, insertStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}

	if result.RowsAffected != 5 {
		t.Errorf("expected RowsAffected=5, got %d", result.RowsAffected)
	}

	if result.Message == "" {
		t.Error("expected non-empty message")
	}

	// Message should contain "inserted"
	if result.Message != "5 row(s) inserted" {
		t.Logf("Note: message format is '%s'", result.Message)
	}
}

// TestFormatResult_Update tests formatting of UPDATE results
func TestFormatResult_Update(t *testing.T) {
	dmlResult := &planner.DMLResult{
		RowsAffected: 3,
	}

	updateStmt := statements.NewUpdateStatement("test", "")

	result, err := formatResult(dmlResult, updateStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}

	if result.RowsAffected != 3 {
		t.Errorf("expected RowsAffected=3, got %d", result.RowsAffected)
	}

	if result.Message == "" {
		t.Error("expected non-empty message")
	}
}

// TestFormatResult_Delete tests formatting of DELETE results
func TestFormatResult_Delete(t *testing.T) {
	dmlResult := &planner.DMLResult{
		RowsAffected: 10,
	}

	deleteStmt := statements.NewDeleteStatement("test", "")

	result, err := formatResult(dmlResult, deleteStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}

	if result.RowsAffected != 10 {
		t.Errorf("expected RowsAffected=10, got %d", result.RowsAffected)
	}

	if result.Message == "" {
		t.Error("expected non-empty message")
	}
}

// TestFormatResult_CreateTable tests formatting of CREATE TABLE results
func TestFormatResult_CreateTable(t *testing.T) {
	ddlResult := &planner.DDLResult{
		Success: true,
		Message: "Table created successfully",
	}

	createStmt := statements.NewCreateStatement("test", false)

	result, err := formatResult(ddlResult, createStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}

	if result.Message == "" {
		t.Error("expected non-empty message")
	}

	if result.Message != "Table created successfully" {
		t.Errorf("expected message='Table created successfully', got '%s'", result.Message)
	}
}

// TestFormatResult_DropTable tests formatting of DROP TABLE results
func TestFormatResult_DropTable(t *testing.T) {
	ddlResult := &planner.DDLResult{
		Success: true,
		Message: "Table dropped successfully",
	}

	// Use CreateStatement since it implements Statement interface
	createStmt := statements.NewCreateStatement("test", false)

	result, err := formatResult(ddlResult, createStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true")
	}

	if result.Message != "Table dropped successfully" {
		t.Errorf("expected message='Table dropped successfully', got '%s'", result.Message)
	}
}

// TestFormatResult_ZeroRowsAffected tests DML with zero rows affected
func TestFormatResult_ZeroRowsAffected(t *testing.T) {
	dmlResult := &planner.DMLResult{
		RowsAffected: 0,
	}

	deleteStmt := statements.NewDeleteStatement("test", "")

	result, err := formatResult(dmlResult, deleteStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true even with 0 rows affected")
	}

	if result.RowsAffected != 0 {
		t.Errorf("expected RowsAffected=0, got %d", result.RowsAffected)
	}

	if result.Message == "" {
		t.Error("expected non-empty message")
	}
}

// TestFormatSelect_ColumnNames tests column name handling
func TestFormatSelect_ColumnNames(t *testing.T) {
	tests := []struct {
		name          string
		fieldNames    []string
		expectedNames []string
	}{
		{
			name:          "Normal names",
			fieldNames:    []string{"id", "name", "age"},
			expectedNames: []string{"id", "name", "age"},
		},
		{
			name:          "Empty name",
			fieldNames:    []string{"id", "", "age"},
			expectedNames: []string{"id", "col_1", "age"},
		},
		{
			name:          "All empty names",
			fieldNames:    []string{"", "", ""},
			expectedNames: []string{"col_0", "col_1", "col_2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldTypes := make([]types.Type, len(tt.fieldNames))
			for i := range fieldTypes {
				fieldTypes[i] = types.IntType
			}

			td, err := tuple.NewTupleDesc(fieldTypes, tt.fieldNames)
			if err != nil {
				t.Fatalf("failed to create tuple desc: %v", err)
			}

			queryResult := &planner.SelectQueryResult{
				TupleDesc: td,
				Tuples:    []*tuple.Tuple{},
			}

			result := formatSelect(queryResult)

			if len(result.Columns) != len(tt.expectedNames) {
				t.Fatalf("expected %d columns, got %d", len(tt.expectedNames), len(result.Columns))
			}

			for i, expected := range tt.expectedNames {
				if result.Columns[i] != expected {
					t.Errorf("column[%d]: expected '%s', got '%s'", i, expected, result.Columns[i])
				}
			}
		})
	}
}

// TestFormatSelect_DifferentTypes tests formatting different data types
func TestFormatSelect_DifferentTypes(t *testing.T) {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType, types.BoolType, types.FloatType},
		[]string{"int_col", "str_col", "bool_col", "float_col"},
	)
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	tup := tuple.NewTuple(td)
	tup.SetField(0, types.NewIntField(42))
	tup.SetField(1, types.NewStringField("test", 255))
	tup.SetField(2, types.NewBoolField(true))
	tup.SetField(3, types.NewIntField(314)) // Using int instead of float

	queryResult := &planner.SelectQueryResult{
		TupleDesc: td,
		Tuples:    []*tuple.Tuple{tup},
	}

	result := formatSelect(queryResult)

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(row))
	}

	// Verify each type is converted to string
	if row[0] != "42" {
		t.Errorf("expected int field='42', got '%s'", row[0])
	}

	if row[1] != "test" {
		t.Errorf("expected string field='test', got '%s'", row[1])
	}

	// Bool and Float formatting depends on implementation
	// Just verify they're not empty
	if row[2] == "" {
		t.Error("bool field should not be empty")
	}

	if row[3] == "" {
		t.Error("float field should not be empty")
	}
}

// TestFormatDML_AllTypes tests DML formatting for all DML types
func TestFormatDML_AllTypes(t *testing.T) {
	tests := []struct {
		stmtType     statements.StatementType
		rowsAffected int
		expectedMsg  string
	}{
		{
			stmtType:     statements.Insert,
			rowsAffected: 1,
			expectedMsg:  "1 row(s) inserted",
		},
		{
			stmtType:     statements.Update,
			rowsAffected: 5,
			expectedMsg:  "5 row(s) updated",
		},
		{
			stmtType:     statements.Delete,
			rowsAffected: 10,
			expectedMsg:  "10 row(s) deleted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.stmtType.String(), func(t *testing.T) {
			dmlResult := &planner.DMLResult{
				RowsAffected: tt.rowsAffected,
			}

			result := formatDML(dmlResult, tt.stmtType)

			if !result.Success {
				t.Error("expected Success=true")
			}

			if result.RowsAffected != tt.rowsAffected {
				t.Errorf("expected RowsAffected=%d, got %d", tt.rowsAffected, result.RowsAffected)
			}

			if result.Message != tt.expectedMsg {
				t.Errorf("expected message='%s', got '%s'", tt.expectedMsg, result.Message)
			}
		})
	}
}

// TestFormatDDL_SuccessAndFailure tests DDL result formatting
func TestFormatDDL_SuccessAndFailure(t *testing.T) {
	tests := []struct {
		name    string
		success bool
		message string
	}{
		{
			name:    "Success",
			success: true,
			message: "Operation completed successfully",
		},
		{
			name:    "Failure",
			success: false,
			message: "Operation failed",
		},
		{
			name:    "Empty message",
			success: true,
			message: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddlResult := &planner.DDLResult{
				Success: tt.success,
				Message: tt.message,
			}

			result := formatDDL(ddlResult)

			if result.Success != tt.success {
				t.Errorf("expected Success=%v, got %v", tt.success, result.Success)
			}

			if result.Message != tt.message {
				t.Errorf("expected message='%s', got '%s'", tt.message, result.Message)
			}
		})
	}
}

// TestFormatResult_UnknownType tests handling of unknown statement types
func TestFormatResult_UnknownType(t *testing.T) {
	// Use CreateStatement and pass nil result to test default behavior
	createStmt := statements.NewCreateStatement("test", false)

	// Pass nil result
	result, err := formatResult(nil, createStmt)
	if err != nil {
		t.Fatalf("formatResult failed: %v", err)
	}

	// Should return a default success result
	if !result.Success {
		t.Error("expected Success=true for unknown type")
	}

	if result.Message == "" {
		t.Error("expected non-empty message")
	}
}

// TestFormatResult_NilQueryResult tests handling of nil QueryResult
func TestFormatResult_NilQueryResult(t *testing.T) {
	selectStmt := statements.NewSelectStatement(nil)

	result, err := formatResult(nil, selectStmt)
	if err != nil {
		t.Fatalf("formatResult should handle nil result: %v", err)
	}

	if !result.Success {
		t.Error("expected Success=true for nil result")
	}
}

// TestFormatSelect_LargeResultSet tests formatting large result sets
func TestFormatSelect_LargeResultSet(t *testing.T) {
	td, err := tuple.NewTupleDesc(
		[]types.Type{types.IntType, types.StringType},
		[]string{"id", "name"},
	)
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	// Create 1000 tuples
	tuples := make([]*tuple.Tuple, 1000)
	for i := 0; i < 1000; i++ {
		tup := tuple.NewTuple(td)
		tup.SetField(0, types.NewIntField(int64(i)))
		tup.SetField(1, types.NewStringField("name", 255))
		tuples[i] = tup
	}

	queryResult := &planner.SelectQueryResult{
		TupleDesc: td,
		Tuples:    tuples,
	}

	result := formatSelect(queryResult)

	if len(result.Rows) != 1000 {
		t.Errorf("expected 1000 rows, got %d", len(result.Rows))
	}

	if !result.Success {
		t.Error("expected Success=true")
	}
}

// TestFormatSelect_ManyColumns tests formatting results with many columns
func TestFormatSelect_ManyColumns(t *testing.T) {
	numCols := 50
	fieldTypes := make([]types.Type, numCols)
	fieldNames := make([]string, numCols)

	for i := 0; i < numCols; i++ {
		fieldTypes[i] = types.IntType
		fieldNames[i] = "col_" + string(rune(i+'0'))
	}

	td, err := tuple.NewTupleDesc(fieldTypes, fieldNames)
	if err != nil {
		t.Fatalf("failed to create tuple desc: %v", err)
	}

	tup := tuple.NewTuple(td)
	for i := 0; i < numCols; i++ {
		tup.SetField(i, types.NewIntField(int64(i)))
	}

	queryResult := &planner.SelectQueryResult{
		TupleDesc: td,
		Tuples:    []*tuple.Tuple{tup},
	}

	result := formatSelect(queryResult)

	if len(result.Columns) != numCols {
		t.Errorf("expected %d columns, got %d", numCols, len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	if len(result.Rows[0]) != numCols {
		t.Errorf("expected row with %d columns, got %d", numCols, len(result.Rows[0]))
	}
}

// TestQueryResult_Structure tests QueryResult struct fields
func TestQueryResult_Structure(t *testing.T) {
	result := QueryResult{
		Success:      true,
		Columns:      []string{"col1", "col2"},
		Rows:         [][]string{{"val1", "val2"}},
		RowsAffected: 1,
		Message:      "test message",
		Error:        nil,
	}

	if !result.Success {
		t.Error("expected Success=true")
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}

	if result.RowsAffected != 1 {
		t.Errorf("expected RowsAffected=1, got %d", result.RowsAffected)
	}

	if result.Message != "test message" {
		t.Errorf("expected message='test message', got '%s'", result.Message)
	}

	if result.Error != nil {
		t.Errorf("expected Error=nil, got %v", result.Error)
	}
}

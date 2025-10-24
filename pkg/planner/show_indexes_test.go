package planner

import (
	"storemy/pkg/parser/statements"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
	"testing"
)

func TestShowIndexesPlan_Execute_NoIndexes(t *testing.T) {
	dataDir := setupTestDataDir(t)
	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create SHOW INDEXES statement
	stmt := statements.NewShowIndexesStatement("")

	// Create plan
	plan := NewShowIndexesPlan(stmt, ctx, transCtx)

	// Execute
	result, err := plan.Execute()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify result type
	selectResult, ok := result.(*SelectQueryResult)
	if !ok {
		t.Fatalf("Expected SelectQueryResult, got: %T", result)
	}

	// Allow any number of results since system might have indexes
	t.Logf("Found %d indexes in catalog", len(selectResult.Tuples))

	// Verify schema
	if selectResult.TupleDesc.NumFields() != 5 {
		t.Errorf("Expected 5 fields in result, got: %d", selectResult.TupleDesc.NumFields())
	}
}

func TestShowIndexesPlan_Execute_WithIndexes(t *testing.T) {
	dataDir := setupTestDataDir(t)
	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create a test table
	tableName := "test_users"
	createTestTableForIndex(t, ctx, transCtx, tableName)

	// Create an index on the table
	createIndexStmt := statements.NewCreateIndexStatement(
		"idx_test_name",
		tableName,
		"name",
		index.HashIndex,
		false,
	)

	createIndexPlan := NewCreateIndexPlan(createIndexStmt, ctx, transCtx)
	_, err := createIndexPlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Create SHOW INDEXES statement
	stmt := statements.NewShowIndexesStatement("")

	// Create plan
	plan := NewShowIndexesPlan(stmt, ctx, transCtx)

	// Execute
	result, err := plan.Execute()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify result type
	selectResult, ok := result.(*SelectQueryResult)
	if !ok {
		t.Fatalf("Expected SelectQueryResult, got: %T", result)
	}

	// Should return at least 1 row (our index)
	if len(selectResult.Tuples) < 1 {
		t.Errorf("Expected at least 1 index, got: %d", len(selectResult.Tuples))
	}

	// Verify schema
	if selectResult.TupleDesc.NumFields() != 5 {
		t.Errorf("Expected 5 fields in result, got: %d", selectResult.TupleDesc.NumFields())
	}

	// Verify the index we created is in the results
	foundIndex := false
	for _, tup := range selectResult.Tuples {
		indexNameField, err := tup.GetField(0)
		if err != nil {
			t.Fatalf("Failed to get index_name field: %v", err)
		}
		indexNameStringField, ok := indexNameField.(*types.StringField)
		if !ok {
			t.Fatalf("Expected StringField for index_name, got %T", indexNameField)
		}
		indexName := indexNameStringField.Value

		if indexName == "idx_test_name" {
			foundIndex = true

			// Verify other fields
			tableNameField, err := tup.GetField(1)
			if err != nil {
				t.Fatalf("Failed to get table_name field: %v", err)
			}
			tableNameStringField, ok := tableNameField.(*types.StringField)
			if !ok {
				t.Fatalf("Expected StringField for table_name, got %T", tableNameField)
			}
			returnedTableName := tableNameStringField.Value

			columnNameField, err := tup.GetField(2)
			if err != nil {
				t.Fatalf("Failed to get column_name field: %v", err)
			}
			columnNameStringField, ok := columnNameField.(*types.StringField)
			if !ok {
				t.Fatalf("Expected StringField for column_name, got %T", columnNameField)
			}
			columnName := columnNameStringField.Value

			indexTypeField, err := tup.GetField(3)
			if err != nil {
				t.Fatalf("Failed to get index_type field: %v", err)
			}
			indexTypeStringField, ok := indexTypeField.(*types.StringField)
			if !ok {
				t.Fatalf("Expected StringField for index_type, got %T", indexTypeField)
			}
			indexType := indexTypeStringField.Value

			if returnedTableName != tableName {
				t.Errorf("Expected table name %s, got: %s", tableName, returnedTableName)
			}
			if columnName != "name" {
				t.Errorf("Expected column name 'name', got: %s", columnName)
			}
			if indexType != "HASH" {
				t.Errorf("Expected index type 'HASH', got: %s", indexType)
			}

			break
		}
	}

	if !foundIndex {
		t.Error("Expected to find 'idx_test_name' in results")
	}
}

func TestShowIndexesPlan_Execute_FilterByTable(t *testing.T) {
	dataDir := setupTestDataDir(t)
	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create first test table
	table1Name := "test_table_1"
	createTestTableForIndex(t, ctx, transCtx, table1Name)

	// Create an index on the first table
	createIndexStmt1 := statements.NewCreateIndexStatement(
		"idx_table1_name",
		table1Name,
		"name",
		index.HashIndex,
		false,
	)
	createIndexPlan1 := NewCreateIndexPlan(createIndexStmt1, ctx, transCtx)
	_, err := createIndexPlan1.Execute()
	if err != nil {
		t.Fatalf("Failed to create index on table1: %v", err)
	}

	// Create second test table
	table2Name := "test_table_2"
	stmt := statements.NewCreateStatement(table2Name, false)
	stmt.AddField("id", types.IntType, true, nil)
	stmt.AddField("email", types.StringType, false, nil)
	stmt.PrimaryKey = "id"
	createTablePlan := NewCreateTablePlan(stmt, ctx, transCtx)
	_, err = createTablePlan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test table 2: %v", err)
	}

	// Create an index on the second table
	createIndexStmt2 := statements.NewCreateIndexStatement(
		"idx_table2_email",
		table2Name,
		"email",
		index.BTreeIndex,
		false,
	)
	createIndexPlan2 := NewCreateIndexPlan(createIndexStmt2, ctx, transCtx)
	_, err = createIndexPlan2.Execute()
	if err != nil {
		t.Fatalf("Failed to create index on table2: %v", err)
	}

	// SHOW INDEXES FROM table1
	showStmt := statements.NewShowIndexesStatement(table1Name)
	plan := NewShowIndexesPlan(showStmt, ctx, transCtx)

	result, err := plan.Execute()
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	selectResult, ok := result.(*SelectQueryResult)
	if !ok {
		t.Fatalf("Expected SelectQueryResult, got: %T", result)
	}

	// Should only return indexes for table1
	if len(selectResult.Tuples) != 1 {
		t.Errorf("Expected 1 index for table1, got: %d", len(selectResult.Tuples))
	}

	// Verify it's the right index
	if len(selectResult.Tuples) > 0 {
		indexNameField, err := selectResult.Tuples[0].GetField(0)
		if err != nil {
			t.Fatalf("Failed to get index_name field: %v", err)
		}
		indexNameStringField, ok := indexNameField.(*types.StringField)
		if !ok {
			t.Fatalf("Expected StringField for index_name, got %T", indexNameField)
		}
		indexName := indexNameStringField.Value
		if indexName != "idx_table1_name" {
			t.Errorf("Expected index 'idx_table1_name', got: %s", indexName)
		}
	}
}

func TestShowIndexesPlan_Execute_NonExistentTable(t *testing.T) {
	dataDir := setupTestDataDir(t)
	ctx := createTestContextWithCleanup(t, dataDir)
	transCtx := createTransactionContext(t)

	// Create SHOW INDEXES statement for non-existent table
	stmt := statements.NewShowIndexesStatement("nonexistent_table")

	// Create plan
	plan := NewShowIndexesPlan(stmt, ctx, transCtx)

	// Execute - should return an error
	_, err := plan.Execute()
	if err == nil {
		t.Error("Expected error for non-existent table, got nil")
	}
}

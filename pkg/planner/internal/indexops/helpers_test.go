package indexops

import (
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/registry"
	"storemy/pkg/storage/index"
	"testing"
)

// createTestIndex creates an index for testing purposes
// This helper is specific to indexops tests and uses NewCreateIndexPlan
func createTestIndex(t *testing.T, ctx *registry.DatabaseContext, transCtx *transaction.TransactionContext, tableName, indexName, columnName string, indexType index.IndexType) {
	t.Helper()
	stmt := statements.NewCreateIndexStatement(indexName, tableName, columnName, indexType, false)
	plan := NewCreateIndexPlan(stmt, ctx, transCtx)
	_, err := plan.Execute()
	if err != nil {
		t.Fatalf("Failed to create test index: %v", err)
	}
}

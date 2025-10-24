package dml

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

type tableMetadata struct {
	TableID   int
	TupleDesc *tuple.TupleDescription
}

// resolveTableMetadata retrieves table ID and schema in a single operation.
// This is the primary table lookup method used by all planner components.
func resolveTableMetadata(tableName string, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (*tableMetadata, error) {
	catalogMgr := ctx.CatalogManager()
	tableID, err := catalogMgr.GetTableID(tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	sch, err := catalogMgr.GetTableSchema(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}

	return &tableMetadata{
		TableID:   tableID,
		TupleDesc: sch.TupleDesc,
	}, nil
}

// resolveTableID converts a table name to its internal numeric identifier.
// Convenience wrapper around resolveTableMetadata when only the ID is needed.
func resolveTableID(tableName string, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (int, error) {
	md, err := resolveTableMetadata(tableName, tx, ctx)
	if err != nil {
		return -1, err
	}

	return md.TableID, nil
}

// collectAllTuples executes an iterator and materializes all results into memory.
// This is used by operators that require full result sets (e.g., ORDER BY, aggregation).
func collectAllTuples(it iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := it.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %v", err)
	}
	defer it.Close()
	return iterator.Map(it, func(t *tuple.Tuple) (*tuple.Tuple, error) {
		return t, nil
	})
}

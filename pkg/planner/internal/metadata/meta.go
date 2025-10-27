package metadata

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/iterator"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

type TableMetadata struct {
	TableID   primitives.FileID
	TupleDesc *tuple.TupleDescription
}

// resolveTableMetadata retrieves table ID and schema in a single operation.
// This is the primary table lookup method used by all planner components.
func ResolveTableMetadata(tableName string, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (*TableMetadata, error) {
	catalogMgr := ctx.CatalogManager()
	tableID, err := catalogMgr.GetTableID(tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	sch, err := catalogMgr.GetTableSchema(tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}

	return &TableMetadata{
		TableID:   tableID,
		TupleDesc: sch.TupleDesc,
	}, nil
}

// resolveTableID converts a table name to its internal numeric identifier.
// Convenience wrapper around resolveTableMetadata when only the ID is needed.
func ResolveTableID(tableName string, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (primitives.FileID, error) {
	md, err := ResolveTableMetadata(tableName, tx, ctx)
	if err != nil {
		return 0, err
	}

	return md.TableID, nil
}

// collectAllTuples executes an iterator and materializes all results into memory.
// This is used by operators that require full result sets (e.g., ORDER BY, aggregation).
func CollectAllTuples(it iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := it.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %v", err)
	}
	defer it.Close()
	return iterator.Map(it, func(t *tuple.Tuple) (*tuple.Tuple, error) {
		return t, nil
	})
}

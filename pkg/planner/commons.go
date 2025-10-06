package planner

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/parser/plan"
	"storemy/pkg/registry"
	"storemy/pkg/tuple"
)

type tableMetadata struct {
	TableID   int
	TupleDesc *tuple.TupleDescription
}

// resolveTableMetadata retrieves table ID and schema in a single operation.
// This is the primary table lookup method used by all planner components.
func resolveTableMetadata(tableName string, ctx *registry.DatabaseContext) (*tableMetadata, error) {
	tableID, err := ctx.TableManager().GetTableID(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	tupleDesc, err := ctx.TableManager().GetTupleDesc(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}

	return &tableMetadata{
		TableID:   tableID,
		TupleDesc: tupleDesc,
	}, nil
}

// resolveTableID converts a table name to its internal numeric identifier.
// Convenience wrapper around resolveTableMetadata when only the ID is needed.
func resolveTableID(tableName string, ctx *registry.DatabaseContext) (int, error) {
	md, err := resolveTableMetadata(tableName, ctx)
	if err != nil {
		return -1, err
	}

	return md.TableID, nil
}

// findFieldIndex locates a field by name in the tuple descriptor.
// Performs case-sensitive linear search through the schema definition.
func findFieldIndex(fieldName string, tupleDesc *tuple.TupleDescription) (int, error) {
	for i := 0; i < tupleDesc.NumFields(); i++ {
		name, _ := tupleDesc.GetFieldName(i)
		if name == fieldName {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", fieldName)
}

// collectAllTuples executes an iterator and materializes all results into memory.
// This is used by operators that require full result sets (e.g., ORDER BY, aggregation).
func collectAllTuples(it iterator.DbIterator) ([]*tuple.Tuple, error) {
	if err := it.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %v", err)
	}
	defer it.Close()

	var tuples []*tuple.Tuple

	for {
		hasNext, err := it.HasNext()
		if err != nil {
			return nil, fmt.Errorf("error during iteration: %v", err)
		}

		if !hasNext {
			break
		}

		t, err := it.Next()
		if err != nil {
			return nil, fmt.Errorf("error fetching tuple: %v", err)
		}

		tuples = append(tuples, t)
	}

	return tuples, nil
}

// buildScanWithFilter constructs a scan operator with optional WHERE clause filtering.
// This is the foundation for all SELECT query execution plans.
//
// Query execution pipeline:
//  1. SeqScan - reads pages via page-level locks (see LockManager)
//  2. Filter (optional) - applies WHERE predicates
func buildScanWithFilter(
	tid *transaction.TransactionID,
	tableID int,
	whereClause *plan.FilterNode,
	ctx *registry.DatabaseContext,
) (iterator.DbIterator, error) {
	scanOp, err := query.NewSeqScan(tid, tableID, ctx.TableManager())
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}

	if whereClause == nil {
		return scanOp, nil
	}

	predicate, err := buildPredicateFromFilterNode(whereClause, scanOp.GetTupleDesc())
	if err != nil {
		return nil, fmt.Errorf("failed to build WHERE predicate: %v", err)
	}

	filterOp, err := query.NewFilter(predicate, scanOp)
	if err != nil {
		return nil, fmt.Errorf("failed to create filter: %v", err)
	}

	return filterOp, nil
}

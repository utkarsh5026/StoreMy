package planner

import (
	"fmt"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/parser/plan"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

type tableMetadata struct {
	TableID   int
	TupleDesc *tuple.TupleDescription
}

// resolveTableMetadata retrieves table ID and schema in a single operation.
// This is the primary table lookup method used by all planner components.
func resolveTableMetadata(tableName string, tid TID, ctx DbContext) (*tableMetadata, error) {
	catalogMgr := ctx.CatalogManager()
	tableID, err := catalogMgr.GetTableID(tid, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	sch, err := catalogMgr.GetTableSchema(tid, tableID)
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
func resolveTableID(tableName string, tid TID, ctx DbContext) (int, error) {
	md, err := resolveTableMetadata(tableName, tid, ctx)
	if err != nil {
		return -1, err
	}

	return md.TableID, nil
}

// collectAllTuples executes an iterator and materializes all results into memory.
// This is used by operators that require full result sets (e.g., ORDER BY, aggregation).
func collectAllTuples(it DbIterator) ([]*tuple.Tuple, error) {
	if err := it.Open(); err != nil {
		return nil, fmt.Errorf("failed to open iterator: %v", err)
	}
	defer it.Close()
	return iterator.Map(it, func(t *tuple.Tuple) (*tuple.Tuple, error) {
		return t, nil
	})
}

// buildScanWithFilter constructs a scan operator with optional WHERE clause filtering.
// This is the foundation for all SELECT query execution plans.
//
// Query execution pipeline:
//  1. SeqScan - reads pages via page-level locks (see LockManager)
//  2. Filter (optional) - applies WHERE predicates
func buildScanWithFilter(tid TID, tableID int, whereClause *plan.FilterNode, ctx DbContext,
) (DbIterator, error) {
	scanOp, err := query.NewSeqScan(tid, tableID, ctx.CatalogManager())
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

// buildPredicateFromFilterNode constructs a query predicate from a filter node in the execution plan.
// It takes a filter node containing field name, predicate type, and constant value, along with
// tuple description for type information, and returns a fully constructed predicate for query execution.
func buildPredicateFromFilterNode(filter *plan.FilterNode, td TupleDesc) (*query.Predicate, error) {
	fieldName := filter.Field
	if dotIndex := strings.LastIndex(fieldName, "."); dotIndex != -1 {
		fieldName = fieldName[dotIndex+1:]
	}

	fieldIndex, err := td.FindFieldIndex(fieldName)
	if err != nil {
		return nil, err
	}

	fieldType, _ := td.TypeAtIndex(fieldIndex)
	constantField, err := types.CreateFieldFromConstant(fieldType, filter.Constant)
	if err != nil {
		return nil, err
	}

	return query.NewPredicate(fieldIndex, filter.Predicate, constantField), nil
}

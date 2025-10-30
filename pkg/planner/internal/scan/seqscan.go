package scan

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/execution/scanner"
	"storemy/pkg/iterator"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

// BuildScanWithFilter builds an iterator for reading tuples from the specified table.
//
// Behavior:
//   - If a non-nil whereClause is provided, the function attempts to construct an index-backed scan
//     (via tryBuildIndexScan). If an index scan can be used, it is returned.
//   - If index scan construction fails or no applicable index is available, the function falls back
//     to creating a sequential table scan and, if a whereClause was provided, wraps that scan with
//     a Filter operator that evaluates the predicate derived from the whereClause.
//
// Parameters:
// - tx: the transaction context used for the scan.
// - tableID: the identifier of the table to scan.
// - whereClause: optional filter node describing a simple WHERE predicate; may be nil.
// - ctx: database context providing catalog and page store access.
//
// Returns:
// - iterator.DbIterator: an iterator that produces tuples from the table (possibly filtered).
// - error: non-nil on failure to prepare the scan or predicate.
func BuildScanWithFilter(tx *transaction.TransactionContext, tableID primitives.FileID, whereClause *plan.FilterNode, ctx *registry.DatabaseContext) (iterator.DbIterator, error) {
	heapFile, err := getHeapFileForTable(ctx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table file: %v", err)
	}

	if whereClause != nil {
		idxBuilder := IndexScannerBuilder{tx: tx, ctx: ctx, tableID: tableID}
		indexOp, usedIndex, err := idxBuilder.TryBuildIndexScan(whereClause)
		if err != nil {
			fmt.Printf("Warning: index scan failed, falling back to sequential scan: %v\n", err)
		} else if usedIndex {
			return indexOp, nil
		}
	}

	scanOp, err := scanner.NewSeqScan(tx, tableID, heapFile, ctx.PageStore())
	if err != nil {
		return nil, fmt.Errorf("failed to create table scan: %v", err)
	}

	if whereClause == nil {
		return scanOp, nil
	}

	return createFilter(scanOp, whereClause)
}

// buildPredicateFromFilterNode converts a planner FilterNode into a query.Predicate.
//
// The function:
//   - Resolves dotted field names by taking the last segment (e.g. "table.col" -> "col").
//   - Looks up the field index and type in the provided TupleDescription.
//   - Converts the filter constant to a Field value with the appropriate type.
//   - Constructs and returns a query.Predicate that compares the field at the resolved index
//     using the operator specified in the FilterNode.
//
// Parameters:
// - filter: the planner filter node describing field, predicate operator and constant.
// - td: tuple description used to resolve field indices and types.
//
// Returns:
// - *query.Predicate: the predicate ready to be used by filter or index operators.
// - error: non-nil if field resolution or constant conversion fails.
func buildPredicateFromFilterNode(filter *plan.FilterNode, td *tuple.TupleDescription) (*query.Predicate, error) {
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

// getHeapFileForTable retrieves the HeapFile associated with the specified table.
//
// This function queries the catalog manager to obtain the table file and verifies
// that it is of type HeapFile, which is required for sequential and index scans.
//
// Parameters:
// - ctx: database context providing access to the catalog manager.
// - tableID: the identifier of the table whose file should be retrieved.
//
// Returns:
// - *heap.HeapFile: the heap file for the specified table.
// - error: non-nil if the table file cannot be retrieved or is not a HeapFile.
func getHeapFileForTable(ctx *registry.DatabaseContext, tableID primitives.FileID) (*heap.HeapFile, error) {
	cm := ctx.CatalogManager()
	file, err := cm.GetTableFile(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table file: %v", err)
	}

	heapFile, ok := file.(*heap.HeapFile)
	if !ok {
		return nil, fmt.Errorf("unsupported file type for table %d", tableID)
	}

	return heapFile, nil
}

// createFilter wraps a scan iterator with a Filter operator based on the provided WHERE clause.
//
// Parameters:
// - scanOp: the underlying scan iterator that produces tuples to be filtered.
// - whereClause: the filter node describing the WHERE predicate to apply.
//
// Returns:
// - iterator.DbIterator: a filter iterator that wraps the scan and applies the predicate.
// - error: non-nil if predicate construction or filter creation fails.
func createFilter(scanOp iterator.DbIterator, whereClause *plan.FilterNode) (iterator.DbIterator, error) {
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

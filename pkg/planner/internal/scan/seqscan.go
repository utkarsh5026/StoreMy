package scan

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	"storemy/pkg/plan"
	"storemy/pkg/registry"
	"storemy/pkg/storage/heap"
	"storemy/pkg/tuple"
	"storemy/pkg/types"
	"strings"
)

func BuildScanWithFilter(tx *transaction.TransactionContext, tableID int, whereClause *plan.FilterNode, ctx *registry.DatabaseContext,
) (iterator.DbIterator, error) {
	cm := ctx.CatalogManager()
	file, err := cm.GetTableFile(tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table file: %v", err)
	}

	heapFile, ok := file.(*heap.HeapFile)
	if !ok {
		return nil, fmt.Errorf("unsupported file type for table %d", tableID)
	}

	// Try to use index scan if we have a WHERE clause
	if whereClause != nil {
		indexOp, usedIndex, err := tryBuildIndexScan(tx, tableID, heapFile, whereClause, ctx)
		if err != nil {
			// Log but don't fail - fall back to sequential scan
			fmt.Printf("Warning: index scan failed, falling back to sequential scan: %v\n", err)
		} else if usedIndex {
			return indexOp, nil
		}
	}

	// Fall back to sequential scan
	scanOp, err := query.NewSeqScan(tx, tableID, heapFile, ctx.PageStore())
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

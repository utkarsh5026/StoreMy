package planner

import (
	"fmt"
	"math"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/parser/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
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
func resolveTableMetadata(tableName string, tx TransactionCtx, ctx DbContext) (*tableMetadata, error) {
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
func resolveTableID(tableName string, tx TransactionCtx, ctx DbContext) (int, error) {
	md, err := resolveTableMetadata(tableName, tx, ctx)
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
// Query execution pipeline (index-aware):
//  1. Try to use index scan if WHERE clause matches an indexed column
//  2. Fall back to SeqScan if no suitable index exists
//  3. Apply additional filters if needed
func buildScanWithFilter(tx *transaction.TransactionContext, tableID int, whereClause *plan.FilterNode, ctx DbContext,
) (DbIterator, error) {
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

// tryBuildIndexScan attempts to construct an index scan operator for the given filter.
// Returns (operator, true, nil) if index scan was successfully created.
// Returns (nil, false, nil) if no suitable index exists.
// Returns (nil, false, error) if an error occurred during index scan creation.
//
// Index selection logic:
//  1. Get all indexes for the table
//  2. Find an index on the filtered column
//  3. Check if the predicate is index-friendly (=, <, >, <=, >=)
//  4. Create appropriate IndexScan operator (equality or range)
func tryBuildIndexScan(
	tx *transaction.TransactionContext,
	tableID int,
	heapFile *heap.HeapFile,
	filter *plan.FilterNode,
	ctx DbContext,
) (DbIterator, bool, error) {
	cm := ctx.CatalogManager()

	indexes, err := cm.GetIndexesByTable(tx, tableID)
	if err != nil || len(indexes) == 0 {
		return nil, false, nil
	}

	fieldName := extractFieldName(filter.Field)
	var selectedIndex *systemtable.IndexMetadata
	for _, idx := range indexes {
		if strings.EqualFold(idx.ColumnName, fieldName) {
			selectedIndex = idx
			break
		}
	}

	if selectedIndex == nil {
		return nil, false, nil
	}

	// Step 3: Check if predicate is index-friendly and create appropriate scan
	switch filter.Predicate {
	case primitives.Equals:
		// Can use index.Search() for both Hash and BTree indexes
		return buildIndexEqualityScan(tx, heapFile, selectedIndex, filter, ctx)

	case primitives.GreaterThan, primitives.LessThan,
		primitives.GreaterThanOrEqual, primitives.LessThanOrEqual:
		// Can only use range search for BTree indexes
		if selectedIndex.IndexType == index.BTreeIndex {
			return buildIndexRangeScan(tx, heapFile, selectedIndex, filter, ctx)
		}
	}

	// Predicate not suitable for index scan
	return nil, false, nil
}

// buildIndexEqualityScan creates an IndexScan operator for equality predicates (=).
// Works with both Hash and BTree indexes.
func buildIndexEqualityScan(
	tx *transaction.TransactionContext,
	heapFile *heap.HeapFile,
	indexMeta *systemtable.IndexMetadata,
	filter *plan.FilterNode,
	ctx DbContext,
) (DbIterator, bool, error) {
	// Get the column type from the heap file schema
	fieldName := extractFieldName(filter.Field)
	fieldIndex, err := heapFile.GetTupleDesc().FindFieldIndex(fieldName)
	if err != nil {
		return nil, false, fmt.Errorf("field %s not found in schema: %w", fieldName, err)
	}

	fieldType, _ := heapFile.GetTupleDesc().TypeAtIndex(fieldIndex)

	// Parse the search key value
	searchKey, err := types.CreateFieldFromConstant(fieldType, filter.Constant)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse search key: %w", err)
	}

	// Load the index
	idx, err := loadIndex(indexMeta, tx, ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load index: %w", err)
	}

	// Create index scan operator
	indexScan, err := query.NewIndexEqualityScan(tx, idx, heapFile, ctx.PageStore(), searchKey)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create index scan: %w", err)
	}

	return indexScan, true, nil
}

// buildIndexRangeScan creates an IndexScan operator for range predicates (<, >, <=, >=).
// Only works with BTree indexes.
func buildIndexRangeScan(
	tx *transaction.TransactionContext,
	heapFile *heap.HeapFile,
	indexMeta *systemtable.IndexMetadata,
	filter *plan.FilterNode,
	ctx DbContext,
) (DbIterator, bool, error) {
	// Get the column type from the heap file schema
	fieldName := extractFieldName(filter.Field)
	fieldIndex, err := heapFile.GetTupleDesc().FindFieldIndex(fieldName)
	if err != nil {
		return nil, false, fmt.Errorf("field %s not found in schema: %w", fieldName, err)
	}

	fieldType, _ := heapFile.GetTupleDesc().TypeAtIndex(fieldIndex)

	// Parse the comparison value
	compareValue, err := types.CreateFieldFromConstant(fieldType, filter.Constant)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse comparison value: %w", err)
	}

	// Determine the range bounds based on the predicate
	var startKey, endKey types.Field
	switch filter.Predicate {
	case primitives.GreaterThan:
		// WHERE x > 5: range is (5, max]
		startKey = compareValue
		endKey = getMaxValueForType(fieldType)
	case primitives.GreaterThanOrEqual:
		// WHERE x >= 5: range is [5, max]
		startKey = compareValue
		endKey = getMaxValueForType(fieldType)
	case primitives.LessThan:
		// WHERE x < 5: range is [min, 5)
		startKey = getMinValueForType(fieldType)
		endKey = compareValue
	case primitives.LessThanOrEqual:
		// WHERE x <= 5: range is [min, 5]
		startKey = getMinValueForType(fieldType)
		endKey = compareValue
	default:
		return nil, false, fmt.Errorf("unsupported range predicate: %v", filter.Predicate)
	}

	// Load the index
	idx, err := loadIndex(indexMeta, tx, ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to load index: %w", err)
	}

	// Create index scan operator
	indexScan, err := query.NewIndexRangeScan(tx, idx, heapFile, ctx.PageStore(), startKey, endKey)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create range scan: %w", err)
	}

	return indexScan, true, nil
}

// loadIndex loads an index from disk based on its metadata.
// Returns an appropriate index wrapped in an adapter for interface compatibility.
func loadIndex(indexMeta *systemtable.IndexMetadata, tx *transaction.TransactionContext, ctx DbContext) (index.Index, error) {
	switch indexMeta.IndexType {
	case index.HashIndex:
		// Open the hash index file
		hashFile, err := hash.NewHashFile(indexMeta.FilePath, types.IntType, hash.DefaultBuckets)
		if err != nil {
			return nil, fmt.Errorf("failed to open hash index file: %w", err)
		}
		hashIdx := hashindex.NewHashIndex(indexMeta.IndexID, hashFile.GetKeyType(), hashFile, ctx.PageStore(), tx)
		return hashIdx, nil

	case index.BTreeIndex:
		// Open the btree index file
		btreeFile, err := btree.NewBTreeFile(indexMeta.FilePath, types.IntType)
		if err != nil {
			return nil, fmt.Errorf("failed to open btree index file: %w", err)
		}
		btreeIdx := btreeindex.NewBTree(indexMeta.IndexID, btreeFile.GetKeyType(), btreeFile, tx, ctx.PageStore())
		return btreeIdx, nil

	default:
		return nil, fmt.Errorf("unsupported index type: %s", indexMeta.IndexType)
	}
}

// getMinValueForType returns the minimum value for a given type (for range queries).
func getMinValueForType(t types.Type) types.Field {
	switch t {
	case types.IntType:
		return types.NewIntField(math.MaxInt64) // math.MinInt64
	case types.FloatType:
		return types.NewFloat64Field(-math.MaxFloat64) // -math.MaxFloat64
	case types.StringType:
		return types.NewStringField("", types.StringMaxSize)
	case types.BoolType:
		return types.NewBoolField(false)
	default:
		return nil
	}
}

// getMaxValueForType returns the maximum value for a given type (for range queries).
func getMaxValueForType(t types.Type) types.Field {
	switch t {
	case types.IntType:
		return types.NewIntField(math.MaxInt64) // math.MaxInt64
	case types.FloatType:
		return types.NewFloat64Field(math.MaxFloat64) // math.MaxFloat64
	case types.StringType:
		// For strings, use a very high Unicode character repeated
		maxStr := strings.Repeat("\U0010FFFF", types.StringMaxSize/4)
		return types.NewStringField(maxStr, types.StringMaxSize)
	case types.BoolType:
		return types.NewBoolField(true)
	default:
		return nil
	}
}

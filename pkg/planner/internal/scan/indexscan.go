package scan

import (
	"fmt"
	"math"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/iterator"
	btreeindex "storemy/pkg/memory/wrappers/btree_index"
	hashindex "storemy/pkg/memory/wrappers/hash_index"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/storage/heap"
	"storemy/pkg/storage/index"
	"storemy/pkg/storage/index/btree"
	"storemy/pkg/storage/index/hash"
	"storemy/pkg/types"
	"strings"
)

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
	tableID primitives.FileID,
	heapFile *heap.HeapFile,
	filter *plan.FilterNode,
	ctx *registry.DatabaseContext,
) (iterator.DbIterator, bool, error) {
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
	ctx *registry.DatabaseContext,
) (iterator.DbIterator, bool, error) {
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
	ctx *registry.DatabaseContext,
) (iterator.DbIterator, bool, error) {
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
func loadIndex(indexMeta *systemtable.IndexMetadata, tx *transaction.TransactionContext, ctx *registry.DatabaseContext) (index.Index, error) {
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

// extractFieldName extracts the field name from a qualified name.
// Handles both simple (field) and qualified (table.field) names.
func extractFieldName(qualifiedName string) string {
	parts := strings.Split(qualifiedName, ".")
	return parts[len(parts)-1]
}

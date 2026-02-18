package scan

import (
	"fmt"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/execution/query"
	"storemy/pkg/execution/scanner"
	"storemy/pkg/iterator"
	"storemy/pkg/plan"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/types"
	"strings"
)

// IndexScannerBuilder is responsible for constructing optimized index scan operators
// from filter predicates. It analyzes available indexes and determines if a table scan
// can be replaced with a more efficient index scan.
type IndexScannerBuilder struct {
	tx      *transaction.TransactionContext
	ctx     *registry.DatabaseContext
	tableID primitives.FileID
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
func (b *IndexScannerBuilder) TryBuildIndexScan(filter *plan.FilterNode) (iterator.DbIterator, bool, error) {
	indexCol, err := b.getIndexColumn(filter)
	if err != nil {
		return nil, false, err
	}

	if indexCol == primitives.InvalidColumnID {
		return nil, false, nil
	}

	indexCfg, err := b.createIndexConfig(indexCol)
	if err != nil {
		return nil, false, err
	}

	pred, err := buildPredicateFromFilterNode(filter, indexCfg.HeapFile.GetTupleDesc())
	if err != nil {
		return nil, false, err
	}

	switch filter.Predicate {
	case primitives.Equals:
		return b.buildIndexEqualityScan(*indexCfg, *pred)

	case primitives.GreaterThan, primitives.LessThan,
		primitives.GreaterThanOrEqual, primitives.LessThanOrEqual:
		return b.buildIndexRangeScan(*indexCfg, *pred)
	}
	return nil, false, nil
}

// buildIndexEqualityScan creates an IndexScan operator for equality predicates (=).
// Works with both Hash and BTree indexes.
func (b *IndexScannerBuilder) buildIndexEqualityScan(cfg scanner.IndexScanConfig, pred query.Predicate) (iterator.DbIterator, bool, error) {
	indexScan, err := scanner.NewIndexEqualityScan(cfg, pred.Value())
	if err != nil {
		return nil, false, fmt.Errorf("failed to create index scan: %w", err)
	}
	return indexScan, true, nil
}

// buildIndexRangeScan creates an IndexScan operator for range predicates (<, >, <=, >=).
// Only works with BTree indexes.
// For strict inequalities (< and >), a post-filter is applied to exclude the boundary value.
func (b *IndexScannerBuilder) buildIndexRangeScan(cfg scanner.IndexScanConfig, pred query.Predicate) (iterator.DbIterator, bool, error) {
	start, end, needsFilter, err := getRangeVal(pred.Operation(), pred.Value())
	if err != nil {
		return nil, false, fmt.Errorf("failed to get range values: %w", err)
	}
	indexScan, err := scanner.NewIndexRangeScan(cfg, start, end)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create range scan: %w", err)
	}

	// For strict inequalities (< or >), wrap with a filter to exclude boundary value
	if needsFilter {
		filterOp, err := query.NewFilter(&pred, indexScan)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create boundary filter: %w", err)
		}
		return filterOp, true, nil
	}

	return indexScan, true, nil
}

// createIndexConfig constructs an IndexScanConfig by loading the necessary components:
// the heap file containing table data and the index for the specified column.
// Returns an error if the heap file or index cannot be loaded.
func (b *IndexScannerBuilder) createIndexConfig(column primitives.ColumnID) (*scanner.IndexScanConfig, error) {
	heapFile, err := getHeapFileForTable(b.ctx, b.tableID)
	if err != nil {
		return nil, err
	}

	idx, err := b.ctx.IndexManager().LoadIndexForCol(b.tx, column, b.tableID)
	if err != nil {
		return nil, fmt.Errorf("error in loading index")
	}

	return &scanner.IndexScanConfig{
		Tx:       b.tx,
		Index:    idx,
		HeapFile: heapFile,
		Store:    b.ctx.PageStore(),
	}, nil
}

// getIndexColumn retrieves the column ID for the field referenced in the filter node.
// It looks up the table schema and matches the filter's field name to a column.
// Returns InvalidColumnID if the column is not found, or an error if schema lookup fails.
func (b *IndexScannerBuilder) getIndexColumn(filter *plan.FilterNode) (primitives.ColumnID, error) {
	cm := b.ctx.CatalogManager()
	sch, err := cm.GetTableSchema(b.tx, b.tableID)
	if err != nil {
		return primitives.InvalidColumnID, err
	}

	fieldName := extractFieldName(filter.Field)
	indexCol := slices.IndexFunc(sch.Columns, func(c schema.ColumnMetadata) bool {
		return fieldName == c.Name
	})

	if indexCol == -1 {
		return primitives.InvalidColumnID, nil
	}

	return sch.Columns[indexCol].Position, nil
}

// extractFieldName extracts the field name from a qualified name.
// Handles both simple (field) and qualified (table.field) names.
func extractFieldName(qualifiedName string) string {
	parts := strings.Split(qualifiedName, ".")
	return parts[len(parts)-1]
}

// getRangeVal converts a range predicate into start/end keys for index scanning.
// Returns (startKey, endKey, needsPostFilter, error).
// needsPostFilter is true for strict inequalities (< and >) which require filtering
// the boundary value after the index scan.
func getRangeVal(pred primitives.Predicate, compareValue types.Field) (startKey, endKey types.Field, needsPostFilter bool, err error) {
	fieldType := compareValue.Type()

	switch pred {
	case primitives.GreaterThan:
		// WHERE x > 5: scan [5, max] but filter out x == 5
		startKey = compareValue
		endKey = types.GetMaxValueFor(fieldType)
		needsPostFilter = true
	case primitives.GreaterThanOrEqual:
		// WHERE x >= 5: scan [5, max]
		startKey = compareValue
		endKey = types.GetMaxValueFor(fieldType)
	case primitives.LessThan:
		// WHERE x < 5: scan [min, 5] but filter out x == 5
		startKey = types.GetMinValueFor(fieldType)
		endKey = compareValue
		needsPostFilter = true
	case primitives.LessThanOrEqual:
		// WHERE x <= 5: scan [min, 5]
		startKey = types.GetMinValueFor(fieldType)
		endKey = compareValue
	default:
		return nil, nil, false, fmt.Errorf("unsupported range predicate: %v", pred)
	}

	return startKey, endKey, needsPostFilter, nil
}

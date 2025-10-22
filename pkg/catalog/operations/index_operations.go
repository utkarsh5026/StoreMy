package operations

import (
	"fmt"
	"storemy/pkg/catalog/catalogio"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"strings"
)

type (
	indexData = *systemtable.IndexMetadata
)

// IndexOperations handles all index-related catalog operations.
// It depends only on the CatalogAccess interface, making it testable
// and decoupled from the concrete SystemCatalog implementation.
type IndexOperations struct {
	*BaseOperations[indexData]
}

// NewIndexOperations creates a new IndexOperations instance.
//
// Parameters:
//   - access: CatalogAccess implementation (typically SystemCatalog)
//   - indexTableID: ID of the CATALOG_INDEXES system table
func NewIndexOperations(access catalogio.CatalogAccess, indexTableID int) *IndexOperations {
	base := NewBaseOperations(
		access,
		indexTableID,
		systemtable.Indexes.Parse,
		func(im indexData) *tuple.Tuple {
			return systemtable.Indexes.CreateTuple(*im)
		},
	)

	return &IndexOperations{
		BaseOperations: base,
	}
}

// GetIndexesByTable retrieves all indexes for a given table from CATALOG_INDEXES.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table whose indexes to retrieve
//
// Returns a slice of IndexMetadata for all indexes on the table, or an error if the catalog cannot be read.
func (io *IndexOperations) GetIndexesByTable(tx *transaction.TransactionContext, tableID int) ([]indexData, error) {
	return io.FindAll(tx, func(im indexData) bool {
		return im.TableID == tableID
	})
}

// GetIndexByName retrieves index metadata from CATALOG_INDEXES by index name.
// Index name matching is case-insensitive.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexName: Name of the index to look up
//
// Returns IndexMetadata or an error if the index is not found.
func (io *IndexOperations) GetIndexByName(tx *transaction.TransactionContext, indexName string) (indexData, error) {
	return io.FindOne(tx, func(im indexData) bool {
		return strings.EqualFold(im.IndexName, indexName)
	})
}

// GetIndexByID retrieves index metadata from CATALOG_INDEXES by index ID.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexID: ID of the index to look up
//
// Returns IndexMetadata or an error if the index is not found.
func (io *IndexOperations) GetIndexByID(tx *transaction.TransactionContext, indexID int) (indexData, error) {
	return io.FindOne(tx, func(im indexData) bool {
		return im.IndexID == indexID
	})
}

// DeleteIndexFromCatalog removes an index entry from CATALOG_INDEXES.
//
// Parameters:
//   - tx: Transaction context for deletions
//   - indexID: ID of the index to remove
//
// Returns an error if the index cannot be deleted.
func (io *IndexOperations) DeleteIndexFromCatalog(tx *transaction.TransactionContext, indexID int) error {
	if err := io.DeleteBy(tx, func(im indexData) bool {
		return im.IndexID == indexID
	}); err != nil {
		return fmt.Errorf("failed to delete index: %w", err)
	}
	return nil
}

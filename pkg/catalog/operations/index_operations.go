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
	IndexData = *systemtable.IndexMetadata
)

// IndexOperations handles all index-related catalog operations.
// It depends only on the CatalogAccess interface, making it testable
// and decoupled from the concrete SystemCatalog implementation.
type IndexOperations struct {
	reader       catalogio.CatalogReader
	writer       catalogio.CatalogWriter
	indexTableID int // ID of CATALOG_INDEXES system table
}

// NewIndexOperations creates a new IndexOperations instance.
//
// Parameters:
//   - access: CatalogAccess implementation (typically SystemCatalog)
//   - indexTableID: ID of the CATALOG_INDEXES system table
func NewIndexOperations(access catalogio.CatalogAccess, indexTableID int) *IndexOperations {
	return &IndexOperations{
		reader:       access,
		writer:       access,
		indexTableID: indexTableID,
	}
}

// GetIndexesByTable retrieves all indexes for a given table from CATALOG_INDEXES.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table whose indexes to retrieve
//
// Returns a slice of IndexMetadata for all indexes on the table, or an error if the catalog cannot be read.
func (io *IndexOperations) GetIndexesByTable(tx *transaction.TransactionContext, tableID int) ([]IndexData, error) {
	var indexes []IndexData

	err := io.reader.IterateTable(io.indexTableID, tx, func(tup *tuple.Tuple) error {
		im, err := systemtable.Indexes.Parse(tup)
		if err != nil {
			return fmt.Errorf("error in parsing the index table")
		}
		if im.TableID == tableID {
			indexes = append(indexes, im)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return indexes, nil
}

// GetIndexByName retrieves index metadata from CATALOG_INDEXES by index name.
// Index name matching is case-insensitive.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexName: Name of the index to look up
//
// Returns IndexMetadata or an error if the index is not found.
func (io *IndexOperations) GetIndexByName(tx *transaction.TransactionContext, indexName string) (IndexData, error) {
	return io.findIndexMetadata(tx, func(im IndexData) bool {
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
func (io *IndexOperations) GetIndexByID(tx *transaction.TransactionContext, indexID int) (IndexData, error) {
	return io.findIndexMetadata(tx, func(im IndexData) bool {
		return im.IndexID == indexID
	})
}

// findIndexMetadata is a generic helper for searching CATALOG_INDEXES with a custom predicate.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - pred: Predicate function that returns true when the desired index is found
//
// Returns the matching IndexMetadata or an error if not found or if catalog access fails.
func (io *IndexOperations) findIndexMetadata(tx *transaction.TransactionContext, pred func(im IndexData) bool) (IndexData, error) {
	var result *systemtable.IndexMetadata

	err := io.reader.IterateTable(io.indexTableID, tx, func(indexTuple *tuple.Tuple) error {
		index, err := systemtable.Indexes.Parse(indexTuple)
		if err != nil {
			return err
		}

		if pred(index) {
			result = index
			return fmt.Errorf("found")
		}

		return nil
	})

	if err != nil && err.Error() == "found" {
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("index not found in catalog")
}

// DeleteIndexFromCatalog removes an index entry from CATALOG_INDEXES.
//
// Parameters:
//   - tx: Transaction context for deletions
//   - indexID: ID of the index to remove
//
// Returns an error if the index cannot be deleted.
func (io *IndexOperations) DeleteIndexFromCatalog(tx *transaction.TransactionContext, indexID int) error {
	var tuplesToDelete []*tuple.Tuple

	err := io.reader.IterateTable(io.indexTableID, tx, func(t *tuple.Tuple) error {
		index, err := systemtable.Indexes.Parse(t)
		if err != nil {
			return err
		}

		if index.IndexID == indexID {
			tuplesToDelete = append(tuplesToDelete, t)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to find index tuples: %w", err)
	}

	for _, tup := range tuplesToDelete {
		if err := io.writer.DeleteRow(io.indexTableID, tx, tup); err != nil {
			return fmt.Errorf("failed to delete index tuple: %w", err)
		}
	}

	return nil
}

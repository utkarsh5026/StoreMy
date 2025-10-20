package catalog

import (
	"fmt"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/tuple"
	"strings"
)

// GetIndexesByTable retrieves all indexes for a given table from CATALOG_INDEXES.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//   - tableID: ID of the table whose indexes to retrieve
//
// Returns a slice of IndexMetadata for all indexes on the table, or an error if the catalog cannot be read.
func (sc *SystemCatalog) GetIndexesByTable(tx *transaction.TransactionContext, tableID int) ([]*systemtable.IndexMetadata, error) {
	var indexes []*systemtable.IndexMetadata
	err := sc.iterateTable(sc.SystemTabs.IndexesTableID, tx, func(tup *tuple.Tuple) error {
		im, err := systemtable.Indexes.Parse(tup)
		if err != nil {
			return err
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
//   - tid: Transaction ID for reading catalog
//   - indexName: Name of the index to look up
//
// Returns IndexMetadata or an error if the index is not found.
func (sc *SystemCatalog) GetIndexByName(tx *transaction.TransactionContext, indexName string) (*systemtable.IndexMetadata, error) {
	return sc.findIndexMetadata(tx, func(im *systemtable.IndexMetadata) bool {
		return strings.EqualFold(im.IndexName, indexName)
	})
}

// GetIndexByID retrieves index metadata from CATALOG_INDEXES by index ID.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//   - indexID: ID of the index to look up
//
// Returns IndexMetadata or an error if the index is not found.
func (sc *SystemCatalog) GetIndexByID(tx *transaction.TransactionContext, indexID int) (*systemtable.IndexMetadata, error) {
	return sc.findIndexMetadata(tx, func(im *systemtable.IndexMetadata) bool {
		return im.IndexID == indexID
	})
}

// findIndexMetadata is a generic helper for searching CATALOG_INDEXES with a custom predicate.
//
// Parameters:
//   - tid: Transaction ID for reading catalog
//   - pred: Predicate function that returns true when the desired index is found
//
// Returns the matching IndexMetadata or an error if not found or if catalog access fails.
func (sc *SystemCatalog) findIndexMetadata(tx *transaction.TransactionContext, pred func(im *systemtable.IndexMetadata) bool) (*systemtable.IndexMetadata, error) {
	var result *systemtable.IndexMetadata

	err := sc.iterateTable(sc.SystemTabs.IndexesTableID, tx, func(indexTuple *tuple.Tuple) error {
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
func (sc *SystemCatalog) DeleteIndexFromCatalog(tx *transaction.TransactionContext, indexID int) error {
	indexesFile, err := sc.cache.GetDbFile(sc.SystemTabs.IndexesTableID)
	if err != nil {
		return err
	}

	var tuplesToDelete []*tuple.Tuple

	sc.iterateTable(sc.SystemTabs.IndexesTableID, tx, func(t *tuple.Tuple) error {
		index, err := systemtable.Indexes.Parse(t)
		if err != nil {
			return err
		}

		if int64(index.IndexID) == int64(indexID) {
			tuplesToDelete = append(tuplesToDelete, t)
		}

		return nil
	})

	for _, tup := range tuplesToDelete {
		if err := sc.tupMgr.DeleteTuple(tx, indexesFile, tup); err != nil {
			return err
		}
	}
	return nil
}

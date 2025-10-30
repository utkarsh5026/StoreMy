package indexops

import (
	"fmt"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/indexmanager"
)

type IndexOps struct {
	tx *transaction.TransactionContext
	cm *catalogmanager.CatalogManager
	im *indexmanager.IndexManager
}

func NewIndexOps(tx *transaction.TransactionContext, cm *catalogmanager.CatalogManager, im *indexmanager.IndexManager) *IndexOps {
	return &IndexOps{
		tx: tx,
		cm: cm,
		im: im,
	}
}

// DeleteIndex removes the index from catalog and deletes its physical file.
//
// Process:
//  1. Calls CatalogManager.DropIndex() to remove from CATALOG_INDEXES
//  2. DropIndex returns the file path of the index to delete
//  3. Uses IndexManager.DeletePhysicalIndex() to remove file
//
// Returns:
//   - nil on success (even if file deletion fails - warns instead)
//   - Error if catalog update fails
//
// Note: File deletion is best-effort. If it fails, a warning is printed
// but the operation succeeds (catalog entry is already removed).
func (i *IndexOps) DeleteIndexFromSystem(name string) error {
	filePath, err := i.cm.DropIndex(i.tx, name)
	if err != nil {
		return fmt.Errorf("failed to drop index %s: %v", name, err)
	}

	if err := i.im.NewFileOps(filePath).DeletePhysicalIndex(); err != nil {
		return fmt.Errorf("failed to delete index file %s: %v", filePath, err)
	}
	return nil
}

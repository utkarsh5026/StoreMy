package indexops

import (
	"fmt"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/indexmanager"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
)

type IndexColConfig struct {
	IndexName string
	IndexType index.IndexType
	// Table and column information
	TableID     primitives.FileID
	TableName   string
	ColumnIndex primitives.ColumnID
	ColumnName  string
	ColumnType  types.Type
}

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

// DeleteIndexFromSystem removes the index from catalog and deletes its physical file.
//
// Process:
//  1. Calls CatalogManager.DropIndex() to remove from CATALOG_INDEXES
//  2. DropIndex returns the file path of the index to delete
//  3. Uses IndexManager.DeletePhysicalIndex() to remove file
//
// Returns:
//   - nil on success
//   - Error if catalog update or file deletion fails
func (i *IndexOps) DeleteIndexFromSystem(name string) error {
	metadata, err := i.cm.DropIndex(i.tx, name)
	if err != nil {
		return fmt.Errorf("failed to drop index from catalog: %w", err)
	}

	if err := i.im.DeletePhysicalIndex(metadata.FilePath); err != nil {
		return fmt.Errorf("failed to delete index file %s: %w", metadata.FilePath, err)
	}
	return nil
}

// CreateIndex is deprecated. Use the DDL planner's CreateIndexPlan instead.
// This method is incomplete and should not be used.
func (i *IndexOps) CreateIndex(path primitives.Filepath, colType types.Type, indexType index.IndexType) (primitives.FileID, error) {
	return 0, fmt.Errorf("CreateIndex on IndexOps is deprecated - use DDL planner instead")
}

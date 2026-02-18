package indexops

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/registry"
	"storemy/pkg/storage/index"
	"storemy/pkg/types"
)

// IndexCreationConfig contains all parameters needed to create and populate an index.
type IndexCreationConfig struct {
	Ctx *registry.DatabaseContext
	Tx  *transaction.TransactionContext

	// Index metadata
	IndexName string
	IndexID   primitives.FileID // Will be overwritten with actual ID from physical file
	IndexType index.IndexType

	// File path for the physical index
	FilePath primitives.Filepath

	// Table and column information
	TableID     primitives.FileID
	TableName   string
	ColumnIndex primitives.ColumnID
	ColumnName  string
	ColumnType  types.Type
}

// createPhysicalIndexWithCleanup creates a physical index file on disk and returns its actual ID.
// The caller is responsible for registering the index in the catalog using this ID.
//
// Returns the actual index ID from the physical file.
func createPhysicalIndexWithCleanup(cf *IndexCreationConfig) (primitives.FileID, error) {
	im := cf.Ctx.IndexManager()

	actualIndexID, err := im.CreatePhysicalIndex(cf.FilePath, cf.ColumnType, cf.IndexType)
	if err != nil {
		return 0, fmt.Errorf("failed to create physical index: %v", err)
	}

	return actualIndexID, nil
}

// populateIndexWithCleanup populates an index with existing table data.
// If population fails at any step, it automatically cleans up both the physical
// index file and the catalog entry.
func populateIndexWithCleanup(cf *IndexCreationConfig) error {
	cm := cf.Ctx.CatalogManager()
	im := cf.Ctx.IndexManager()

	tableFile, err := cm.GetTableFile(cf.TableID)
	if err != nil {
		_ = cf.FilePath.Remove()
		if _, dropErr := cm.NewIndexOps(cf.Tx).DropIndex(cf.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after file access failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to get table file: %v", err)
	}

	if err := im.PopulateIndex(cf.Tx, cf.FilePath, cf.IndexID, tableFile, cf.ColumnIndex, cf.ColumnType, cf.IndexType); err != nil {
		_ = cf.FilePath.Remove()
		if _, dropErr := cm.NewIndexOps(cf.Tx).DropIndex(cf.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after population failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to populate index: %v", err)
	}

	return nil
}

// CreatePhysicalIndexAndGetID creates a physical index file and returns its actual ID.
// The caller should use this ID when registering the index in the catalog.
//
// This implements the proper architecture:
//  1. IndexManager creates physical file and returns actual ID
//  2. Caller registers in catalog using that ID
//  3. Caller calls PopulateIndexWithData to fill the index
func CreatePhysicalIndexAndGetID(config *IndexCreationConfig) (primitives.FileID, error) {
	return createPhysicalIndexWithCleanup(config)
}

// PopulateIndexWithData populates an index with existing table data.
// This should be called after the index is registered in the catalog.
func PopulateIndexWithData(config *IndexCreationConfig) error {
	return populateIndexWithCleanup(config)
}

// GenerateIndexFilePath creates a standardized file path for an index file.
// Format: {dataDir}/{tableName}_{indexName}.idx
//
// This centralizes the index file naming convention to ensure consistency
// across index creation operations.
func GenerateIndexFilePath(ctx *registry.DatabaseContext, tableName, indexName string) primitives.Filepath {
	fileName := fmt.Sprintf("%s_%s.idx", tableName, indexName)
	if dataDir := ctx.DataDir(); dataDir != "" {
		return primitives.Filepath(fmt.Sprintf("%s/%s", dataDir, fileName))
	}
	return primitives.Filepath(fileName)
}

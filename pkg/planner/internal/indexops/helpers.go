package indexops

import (
	"fmt"
	"os"
	"storemy/pkg/concurrency/transaction"
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
	IndexID   int
	IndexType index.IndexType

	// File path for the physical index
	FilePath string

	// Table and column information
	TableID     int
	ColumnIndex int
	ColumnType  types.Type
}

// createPhysicalIndexWithCleanup creates a physical index file on disk.
// If creation fails, it automatically removes the index from the catalog.
func createPhysicalIndexWithCleanup(config *IndexCreationConfig) error {
	im := config.Ctx.IndexManager()
	cm := config.Ctx.CatalogManager()

	if err := im.CreatePhysicalIndex(config.FilePath, config.ColumnType, config.IndexType); err != nil {
		if _, dropErr := cm.DropIndex(config.Tx, config.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after index creation failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to create physical index: %v", err)
	}
	return nil
}

// populateIndexWithCleanup populates an index with existing table data.
// If population fails at any step, it automatically cleans up both the physical
// index file and the catalog entry.
func populateIndexWithCleanup(config *IndexCreationConfig) error {
	cm := config.Ctx.CatalogManager()
	im := config.Ctx.IndexManager()

	tableFile, err := cm.GetTableFile(config.TableID)
	if err != nil {
		os.Remove(config.FilePath)
		if _, dropErr := cm.DropIndex(config.Tx, config.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after file access failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to get table file: %v", err)
	}

	if err := im.PopulateIndex(config.Tx, config.FilePath, config.IndexID, tableFile, config.ColumnIndex, config.ColumnType, config.IndexType); err != nil {
		os.Remove(config.FilePath)
		if _, dropErr := cm.DropIndex(config.Tx, config.IndexName); dropErr != nil {
			fmt.Printf("Warning: failed to cleanup catalog entry after population failure: %v\n", dropErr)
		}
		return fmt.Errorf("failed to populate index: %v", err)
	}

	return nil
}

// CreateAndPopulateIndex creates a physical index file and populates it with existing table data.
// It handles all cleanup automatically on failure.
//
// This is the recommended way to create indexes as it ensures proper error handling
// and cleanup in all failure scenarios.
func CreateAndPopulateIndex(config *IndexCreationConfig) error {
	if err := createPhysicalIndexWithCleanup(config); err != nil {
		return err
	}

	if err := populateIndexWithCleanup(config); err != nil {
		return err
	}

	return nil
}

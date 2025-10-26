package catalogmanager

import (
	"fmt"
	"path/filepath"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/storage/index"
	"storemy/pkg/tuple"
)

// CreateIndex creates a new index and registers it in the catalog.
//
// Steps:
//  1. Validates table and column exist
//  2. Validates index name is unique
//  3. Generates index ID from file path
//  4. Registers index metadata in CATALOG_INDEXES
//  5. Returns index ID and file path
//
// Note: This only registers the index in the catalog - the actual index file
// must be created separately using the returned file path.
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - indexName: Unique name for the index
//   - tableName: Name of the table to index
//   - columnName: Name of the column to index
//   - indexType: Type of index (B-Tree, Hash, etc.)
//
// Returns:
//   - indexID: Generated ID for the index
//   - filePath: Path where index file should be created
//   - error: Error if validation or registration fails
func (cm *CatalogManager) CreateIndex(
	tx TxContext,
	indexName,
	tableName,
	columnName string,
	indexType index.IndexType,
) (indexID int, filePath string, err error) {
	tableID, err := cm.GetTableID(tx, tableName)
	if err != nil {
		return 0, "", fmt.Errorf("table %s not found: %w", tableName, err)
	}

	tableSchema, err := cm.GetTableSchema(tx, tableID)
	if err != nil {
		return 0, "", fmt.Errorf("failed to get table schema: %w", err)
	}

	columnExists := false
	for _, col := range tableSchema.Columns {
		if col.Name == columnName {
			columnExists = true
			break
		}
	}
	if !columnExists {
		return 0, "", fmt.Errorf("column %s does not exist in table %s", columnName, tableName)
	}

	if cm.IndexExists(tx, indexName) {
		return 0, "", fmt.Errorf("index %s already exists", indexName)
	}

	fileName := fmt.Sprintf("%s_%s.idx", tableName, indexName)
	filePath = filepath.Join(cm.dataDir, fileName)

	indexesFile, err := cm.tableCache.GetDbFile(cm.SystemTabs.IndexesTableID)
	if err != nil {
		return 0, "", fmt.Errorf("failed to get indexes catalog file: %w", err)
	}

	indexID = hashFilePath(filePath)
	metadata := systemtable.IndexMetadata{
		IndexID:    indexID,
		IndexName:  indexName,
		TableID:    tableID,
		ColumnName: columnName,
		IndexType:  indexType,
		FilePath:   filePath,
		CreatedAt:  getCurrentTimestamp(),
	}

	tup := systemtable.Indexes.CreateTuple(metadata)
	if err := cm.tupMgr.InsertTuple(tx, indexesFile, tup); err != nil {
		return 0, "", fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return indexID, filePath, nil
}

// DropIndex removes an index from the catalog.
//
// Steps:
//  1. Validates index exists
//  2. Removes index metadata from CATALOG_INDEXES
//  3. Returns the file path for deletion
//
// Note: This only removes catalog metadata - the actual index file must be
// deleted separately using the returned file path.
//
// Parameters:
//   - tx: Transaction context for catalog deletion
//   - indexName: Name of the index to drop
//
// Returns:
//   - filePath: Path to the index file (for deletion)
//   - error: Error if index not found or deletion fails
func (cm *CatalogManager) DropIndex(tx TxContext, indexName string) (filePath string, err error) {
	metadata, err := cm.GetIndexByName(tx, indexName)
	if err != nil {
		return "", fmt.Errorf("index %s not found: %w", indexName, err)
	}

	if err := cm.indexOps.DeleteIndexFromCatalog(tx, metadata.IndexID); err != nil {
		return "", fmt.Errorf("failed to remove index from catalog: %w", err)
	}
	return metadata.FilePath, nil
}

// GetIndexesByTable returns all indexes for a given table.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table
//
// Returns:
//   - []*IndexMetadata: List of index metadata
//   - error: Error if catalog read fails
func (cm *CatalogManager) GetIndexesByTable(tx TxContext, tableID int) ([]*systemtable.IndexMetadata, error) {
	return cm.indexOps.GetIndexesByTable(tx, tableID)
}

// GetAllIndexes retrieves all indexes from the catalog.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//
// Returns:
//   - []*IndexMetadata: List of all index metadata
//   - error: Error if catalog read fails
func (cm *CatalogManager) GetAllIndexes(tx TxContext) ([]*systemtable.IndexMetadata, error) {
	return cm.indexOps.FindAll(tx, func(_ *systemtable.IndexMetadata) bool {
		return true
	})
}

// GetIndexByName retrieves index metadata by index name.
//
// Index name matching is case-sensitive.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexName: Name of the index
//
// Returns:
//   - *IndexMetadata: Index metadata
//   - error: Error if index not found
func (cm *CatalogManager) GetIndexByName(tx TxContext, indexName string) (*systemtable.IndexMetadata, error) {
	return cm.indexOps.GetIndexByName(tx, indexName)
}

// IndexExists checks if an index with the given name exists.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexName: Name of the index
//
// Returns:
//   - bool: true if index exists, false otherwise
func (cm *CatalogManager) IndexExists(tx TxContext, indexName string) bool {
	_, err := cm.GetIndexByName(tx, indexName)
	return err == nil
}

// GetIndexesForTable retrieves all indexes for a table as IndexInfo structs.
//
// This is similar to GetIndexesByTable but returns a simpler IndexInfo type
// that's more convenient for query planning.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table
//
// Returns:
//   - []*IndexInfo: List of index information
//   - error: Error if catalog read fails
func (cm *CatalogManager) GetIndexesForTable(tx TxContext, tableID int) ([]*IndexInfo, error) {
	var indexes []*IndexInfo

	err := cm.iterateTable(cm.SystemTabs.IndexesTableID, tx, func(t *tuple.Tuple) error {
		// Parse index metadata properly using systemtable.Indexes
		im, err := systemtable.Indexes.Parse(t)
		if err != nil {
			return err
		}

		if im.TableID == tableID {
			// Convert IndexMetadata to IndexInfo
			indexes = append(indexes, &IndexInfo{
				IndexID:    im.IndexID,
				TableID:    im.TableID,
				IndexName:  im.IndexName,
				IndexType:  im.IndexType,
				ColumnName: im.ColumnName,
				FilePath:   im.FilePath,
			})
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return indexes, nil
}

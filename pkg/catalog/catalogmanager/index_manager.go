package catalogmanager

import (
	"fmt"
	"path/filepath"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"storemy/pkg/utils/functools"
	"time"
)

type indexCol struct {
	indexName, columnName, tableName string
	indexType                        index.IndexType
}

// CreateIndex creates a new index and registers it in the catalog.
//
// This method stores the index metadata in the catalog using the provided indexID.
// The indexID should be obtained from IndexManager.CreatePhysicalIndex() to ensure
// proper separation of concerns between physical storage and metadata layers.
//
// Steps:
//  1. Validates table and column exist
//  2. Validates index name is unique
//  3. Registers index metadata with provided indexID in CATALOG_INDEXES
//  4. Returns the file path
//
// Architecture Note:
// The caller should follow this flow:
//  1. Generate filePath = filepath.Join(dataDir, fmt.Sprintf("%s_%s.idx", tableName, indexName))
//  2. Call IndexManager.CreatePhysicalIndex(filePath, ...) to get actualIndexID
//  3. Call CatalogManager.CreateIndex(tx, actualIndexID, indexName, ...) to register metadata
//
// Parameters:
//   - tx: Transaction context for catalog update
//   - indexID: The actual file ID from the physical index file (from IndexManager)
//   - indexName: Unique name for the index
//   - tableName: Name of the table to index
//   - columnName: Name of the column to index
//   - indexType: Type of index (B-Tree, Hash, etc.)
//
// Returns:
//   - filePath: Path where index file was created
//   - error: Error if validation or registration fails
func (cm *CatalogManager) CreateIndex(tx TxContext, indexID primitives.IndexID, indexName, tableName, columnName string, indexType index.IndexType) (filePath primitives.Filepath, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tableID, err := cm.GetTableID(tx, tableName)
	if err != nil {
		return "", fmt.Errorf("table %s not found: %w", tableName, err)
	}

	indexCol := &indexCol{
		indexName:  indexName,
		tableName:  tableName,
		columnName: columnName,
		indexType:  indexType,
	}
	return cm.registerIndexWithTableID(tx, tableID, indexID, indexCol)
}

// registerIndexWithTableID is an internal helper that creates an index when tableID is already known.
// This is used by both CreateIndex (which looks up tableID) and RegisterTable (which already has tableID).
func (cm *CatalogManager) registerIndexWithTableID(tx TxContext, tableID primitives.TableID, indexID primitives.IndexID, indexCol *indexCol) (filePath primitives.Filepath, err error) {
	cols, err := cm.colOps.LoadColumnMetadata(tx, tableID)
	if err != nil {
		return "", fmt.Errorf("failed to get table schema: %w", err)
	}

	if err := cm.validateIndexCreation(tx, cols, indexCol); err != nil {
		return "", err
	}

	metadata := cm.buildIndexMetadata(tableID, indexID, indexCol)

	if err := cm.persistIndexMetadata(tx, *metadata); err != nil {
		return "", fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return metadata.FilePath, nil
}

func (cm *CatalogManager) validateIndexCreation(tx TxContext, cols []schema.ColumnMetadata, indexCol *indexCol) error {
	if columnExists := slices.ContainsFunc(cols, func(col schema.ColumnMetadata) bool {
		return col.Name == indexCol.columnName
	}); !columnExists {
		return fmt.Errorf("column %s does not exist in table %s", indexCol.columnName, indexCol.tableName)
	}

	if cm.IndexExists(tx, indexCol.indexName) {
		return fmt.Errorf("index %s already exists", indexCol.indexName)
	}

	return nil
}

func (cm *CatalogManager) persistIndexMetadata(tx TxContext, metadata systemtable.IndexMetadata) error {
	indexesFile, err := cm.tableCache.GetDbFile(cm.SystemTabs.IndexesTableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes catalog file: %w", err)
	}

	tup := systemtable.Indexes.CreateTuple(metadata)
	if err := cm.tupMgr.InsertTuple(tx, indexesFile, tup); err != nil {
		return fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return nil
}

func (cm *CatalogManager) buildIndexMetadata(tableID primitives.TableID, indexID primitives.IndexID, indexCol *indexCol) *systemtable.IndexMetadata {
	fileName := fmt.Sprintf("%s_%s.idx", indexCol.tableName, indexCol.indexName)
	filePath := filepath.Join(cm.dataDir, fileName)
	metadata := systemtable.IndexMetadata{
		IndexID:    indexID,
		IndexName:  indexCol.indexName,
		TableID:    tableID,
		ColumnName: indexCol.columnName,
		IndexType:  indexCol.indexType,
		FilePath:   primitives.Filepath(filePath),
		CreatedAt:  time.Now(),
	}
	return &metadata
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
func (cm *CatalogManager) DropIndex(tx TxContext, indexName string) (filePath primitives.Filepath, err error) {
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
func (cm *CatalogManager) GetIndexesByTable(tx TxContext, tableID primitives.TableID) ([]*systemtable.IndexMetadata, error) {
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
func (cm *CatalogManager) GetIndexesForTable(tx TxContext, tableID primitives.TableID) ([]*IndexInfo, error) {
	var indexes []*IndexInfo

	idxs, err := cm.indexOps.GetIndexesByTable(tx, tableID)
	if err != nil {
		return indexes, err
	}

	return functools.Map(idxs, func(im *systemtable.IndexMetadata) *IndexInfo {
		return &IndexInfo{
			IndexID:    im.IndexID,
			TableID:    im.TableID,
			IndexName:  im.IndexName,
			IndexType:  im.IndexType,
			ColumnName: im.ColumnName,
			FilePath:   im.FilePath,
		}
	}), nil

}

package catalogmanager

import (
	"fmt"
	"path/filepath"
	"slices"
	"storemy/pkg/catalog/operations"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systemtable"
	"storemy/pkg/catalog/tablecache"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/primitives"
	"storemy/pkg/storage/index"
	"sync"
	"time"
)

type indexCol struct {
	indexName, columnName, tableName string
	indexType                        index.IndexType
}

type IndexCatalogOperation struct {
	tx           *transaction.TransactionContext
	cache        *tablecache.TableCache
	indexOps     *operations.IndexOperations
	colOps       *operations.ColumnOperations
	indexTableID primitives.FileID
	mu           sync.RWMutex
	cm           *CatalogManager
}

func (ic *IndexCatalogOperation) DropIndex(name string) (*systemtable.IndexMetadata, error) {
	metadata, err := ic.GetIndexByName(name)
	if err != nil {
		return nil, fmt.Errorf("index %s not found: %w", name, err)
	}

	if err := ic.indexOps.DeleteIndexFromCatalog(ic.tx, metadata.IndexID); err != nil {
		return nil, fmt.Errorf("failed to remove index from catalog: %w", err)
	}
	return metadata, nil
}

// GetIndexByName retrieves index metadata by index name.
//
// Index name matching is case-sensitive.
//
// Parameters:
//   - indexName: Name of the index
//
// Returns:
//   - *IndexMetadata: Index metadata
//   - error: Error if index not found
func (ic *IndexCatalogOperation) GetIndexByName(indexName string) (*systemtable.IndexMetadata, error) {
	return ic.indexOps.GetIndexByName(ic.tx, indexName)
}

func (cm *CatalogManager) NewIndexOps(tx *transaction.TransactionContext) *IndexCatalogOperation {
	return &IndexCatalogOperation{
		tx:           tx,
		cache:        cm.tableCache,
		indexOps:     cm.indexOps,
		colOps:       cm.colOps,
		indexTableID: cm.SystemTabs.IndexesTableID,
		cm:           cm,
	}
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
// Parameters:
//   - indexID: The actual file ID from the physical index file (from IndexManager)
//   - indexName: Unique name for the index
//   - tableName: Name of the table to index
//   - columnName: Name of the column to index
//   - indexType: Type of index (B-Tree, Hash, etc.)
//
// Returns:
//   - filePath: Path where index file was created
//   - error: Error if validation or registration fails
func (io *IndexCatalogOperation) CreateIndex(indexID primitives.FileID, indexName, tableName, columnName string, indexType index.IndexType) (filePath primitives.Filepath, err error) {
	io.mu.Lock()
	defer io.mu.Unlock()

	tableID, err := io.cm.GetTableID(io.tx, tableName)
	if err != nil {
		return "", fmt.Errorf("table %s not found: %w", tableName, err)
	}

	indexCol := &indexCol{
		indexName:  indexName,
		tableName:  tableName,
		columnName: columnName,
		indexType:  indexType,
	}
	return io.registerIndexWithTableID(tableID, indexID, indexCol)
}

// registerIndexWithTableID is an internal helper that creates an index when tableID is already known.
// This is used by both CreateIndex (which looks up tableID) and RegisterTable (which already has tableID).
func (io *IndexCatalogOperation) registerIndexWithTableID(tableID primitives.FileID, indexID primitives.FileID, indexCol *indexCol) (filePath primitives.Filepath, err error) {
	cols, err := io.colOps.LoadColumnMetadata(io.tx, tableID)
	if err != nil {
		return "", fmt.Errorf("failed to get table schema: %w", err)
	}

	if err := io.validateIndexCreation(cols, indexCol); err != nil {
		return "", err
	}

	metadata := io.buildIndexMetadata(tableID, indexID, indexCol)

	if err := io.persistIndexMetadata(*metadata); err != nil {
		return "", fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return metadata.FilePath, nil
}

func (io *IndexCatalogOperation) validateIndexCreation(cols []schema.ColumnMetadata, indexCol *indexCol) error {
	if columnExists := slices.ContainsFunc(cols, func(col schema.ColumnMetadata) bool {
		return col.Name == indexCol.columnName
	}); !columnExists {
		return fmt.Errorf("column %s does not exist in table %s", indexCol.columnName, indexCol.tableName)
	}

	if io.IndexExists(indexCol.indexName) {
		return fmt.Errorf("index %s already exists", indexCol.indexName)
	}

	return nil
}

func (io *IndexCatalogOperation) persistIndexMetadata(metadata systemtable.IndexMetadata) error {
	indexesFile, err := io.cache.GetDbFile(io.indexTableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes catalog file: %w", err)
	}

	tup := systemtable.Indexes.CreateTuple(metadata)
	if err := io.cm.tupMgr.InsertTuple(io.tx, indexesFile, tup); err != nil {
		return fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return nil
}

func (io *IndexCatalogOperation) buildIndexMetadata(tableID primitives.FileID, indexID primitives.FileID, indexCol *indexCol) *systemtable.IndexMetadata {
	fileName := fmt.Sprintf("%s_%s.idx", indexCol.tableName, indexCol.indexName)
	filePath := filepath.Join(io.cm.dataDir, fileName)
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

// GetIndexesByTable returns all indexes for a given table.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - tableID: ID of the table
//
// Returns:
//   - []*IndexMetadata: List of index metadata
//   - error: Error if catalog read fails
func (io *IndexCatalogOperation) GetIndexesByTable(tableID primitives.FileID) ([]*systemtable.IndexMetadata, error) {
	return io.indexOps.GetIndexesByTable(io.tx, tableID)
}

// GetAllIndexes retrieves all indexes from the catalog.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//
// Returns:
//   - []*IndexMetadata: List of all index metadata
//   - error: Error if catalog read fails
func (io *IndexCatalogOperation) GetAllIndexes() ([]*systemtable.IndexMetadata, error) {
	return io.indexOps.FindAll(io.tx, func(_ *systemtable.IndexMetadata) bool {
		return true
	})
}

// IndexExists checks if an index with the given name exists.
//
// Parameters:
//   - tx: Transaction context for reading catalog
//   - indexName: Name of the index
//
// Returns:
//   - bool: true if index exists, false otherwise
func (io *IndexCatalogOperation) IndexExists(indexName string) bool {
	_, err := io.GetIndexByName(indexName)
	return err == nil
}

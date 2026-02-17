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
	"storemy/pkg/types"
	"time"
)

type indexCol struct {
	indexName, columnName, tableName string
	indexType                        index.IndexType
}

// IndexCatalogOperation provides transaction-scoped index operations.
//
// Design: This is a lightweight orchestration layer that coordinates between
// catalog operations, validation, and the CatalogManager's state management.
// The CatalogManager owns all locks and mutable state.
type IndexCatalogOperation struct {
	tx           *transaction.TransactionContext
	cache        *tablecache.TableCache
	indexOps     *operations.IndexOperations
	colOps       *operations.ColumnOperations
	indexTableID primitives.FileID
	cm           *CatalogManager
}

// DropIndex removes an index from the catalog.
//
// Returns the metadata so caller can handle physical file cleanup.
// CatalogManager's lock is used for thread safety.
func (ic *IndexCatalogOperation) DropIndex(name string) (*systemtable.IndexMetadata, error) {
	if name == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	ic.cm.mu.Lock()
	defer ic.cm.mu.Unlock()

	metadata, err := ic.indexOps.GetIndexByName(ic.tx, name)
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
// Uses CatalogManager's read lock for thread safety.
//
// Parameters:
//   - indexName: Name of the index
//
// Returns:
//   - *IndexMetadata: Index metadata
//   - error: Error if index not found
func (ic *IndexCatalogOperation) GetIndexByName(indexName string) (*systemtable.IndexMetadata, error) {
	ic.cm.mu.RLock()
	defer ic.cm.mu.RUnlock()

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
//  1. Validates input parameters
//  2. Validates table and column exist
//  3. Validates index name is unique
//  4. Registers index metadata with provided indexID in CATALOG_INDEXES
//  5. Returns the file path
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
	if err := io.validateCreateIndexInput(indexID, indexName, tableName, columnName); err != nil {
		return "", err
	}

	io.cm.mu.Lock()
	defer io.cm.mu.Unlock()

	if exists, _ := io.indexOps.GetIndexByName(io.tx, indexName); exists != nil {
		return "", fmt.Errorf("index %s already exists", indexName)
	}

	tableID, err := io.getTableIDUnsafe(tableName)
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

// validateCreateIndexInput validates all input parameters.
func (io *IndexCatalogOperation) validateCreateIndexInput(
	indexID primitives.FileID,
	indexName, tableName, columnName string,
) error {
	if indexID == 0 {
		return fmt.Errorf("indexID cannot be zero")
	}
	if indexName == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if len(indexName) > 255 {
		return fmt.Errorf("index name too long (max 255 characters)")
	}
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if columnName == "" {
		return fmt.Errorf("column name cannot be empty")
	}
	return nil
}

// getTableIDUnsafe retrieves table ID without locking (caller must hold lock).
func (io *IndexCatalogOperation) getTableIDUnsafe(tableName string) (primitives.FileID, error) {
	if id, err := io.cache.GetTableID(tableName); err == nil {
		return id, nil
	}

	metadata, err := io.cm.tableOps.GetTableMetadataByName(io.tx, tableName)
	if err != nil {
		return 0, err
	}
	return metadata.TableID, nil
}

// registerIndexWithTableID is an internal helper that creates an index when tableID is already known.
// This is used by both CreateIndex (which looks up tableID) and RegisterTable (which already has tableID).
// Caller must hold cm.mu lock.
func (io *IndexCatalogOperation) registerIndexWithTableID(tableID, indexID primitives.FileID, indexCol *indexCol) (filePath primitives.Filepath, err error) {
	cols, err := io.colOps.LoadColumnMetadata(io.tx, tableID)
	if err != nil {
		return "", fmt.Errorf("failed to get table schema: %w", err)
	}

	if err := io.validateColumnExists(cols, indexCol); err != nil {
		return "", err
	}

	metadata := io.buildIndexMetadata(tableID, indexID, indexCol)

	if err := io.persistIndexMetadata(*metadata); err != nil {
		return "", fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return metadata.FilePath, nil
}

// validateColumnExists validates that the column exists in the table.
// Caller must hold cm.mu lock.
func (io *IndexCatalogOperation) validateColumnExists(cols []schema.ColumnMetadata, indexCol *indexCol) error {
	columnExists := slices.ContainsFunc(cols, func(col schema.ColumnMetadata) bool {
		return col.Name == indexCol.columnName
	})

	if !columnExists {
		return fmt.Errorf("column %s does not exist in table %s", indexCol.columnName, indexCol.tableName)
	}

	return nil
}

// persistIndexMetadata writes index metadata to catalog.
// Caller must hold cm.mu lock.
func (io *IndexCatalogOperation) persistIndexMetadata(metadata systemtable.IndexMetadata) error {
	indexesFile, err := io.cache.GetDbFile(io.indexTableID)
	if err != nil {
		return fmt.Errorf("failed to get indexes catalog file: %w", err)
	}

	tup := systemtable.Indexes.CreateTuple(metadata)
	if err := io.cm.tupMgr.InsertTuple(io.tx, indexesFile, tup); err != nil {
		return fmt.Errorf("failed to insert index tuple: %w", err)
	}

	return nil
}

// buildIndexMetadata constructs index metadata from components.
func (io *IndexCatalogOperation) buildIndexMetadata(tableID, indexID primitives.FileID, indexCol *indexCol) *systemtable.IndexMetadata {
	fileName := fmt.Sprintf("%s_%s.idx", indexCol.tableName, indexCol.indexName)
	filePath := filepath.Join(io.cm.dataDir, fileName)
	return &systemtable.IndexMetadata{
		IndexID:    indexID,
		IndexName:  indexCol.indexName,
		TableID:    tableID,
		ColumnName: indexCol.columnName,
		IndexType:  indexCol.indexType,
		FilePath:   primitives.Filepath(filePath),
		CreatedAt:  time.Now(),
	}
}

// GetIndexesByTable returns all indexes for a given table.
//
// Uses CatalogManager's read lock for thread safety.
//
// Parameters:
//   - tableID: ID of the table
//
// Returns:
//   - []*IndexMetadata: List of index metadata
//   - error: Error if catalog read fails
func (io *IndexCatalogOperation) GetIndexesByTable(tableID primitives.FileID) ([]*systemtable.IndexMetadata, error) {
	io.cm.mu.RLock()
	defer io.cm.mu.RUnlock()

	return io.indexOps.GetIndexesByTable(io.tx, tableID)
}

// GetAllIndexes retrieves all indexes from the catalog.
//
// Uses CatalogManager's read lock for thread safety.
//
// Returns:
//   - []*IndexMetadata: List of all index metadata
//   - error: Error if catalog read fails
func (io *IndexCatalogOperation) GetAllIndexes() ([]*systemtable.IndexMetadata, error) {
	io.cm.mu.RLock()
	defer io.cm.mu.RUnlock()

	return io.indexOps.FindAll(io.tx, func(_ *systemtable.IndexMetadata) bool {
		return true
	})
}

// IndexExists checks if an index with the given name exists.
//
// Uses CatalogManager's read lock for thread safety.
//
// Parameters:
//   - indexName: Name of the index
//
// Returns:
//   - bool: true if index exists, false otherwise
func (io *IndexCatalogOperation) IndexExists(indexName string) bool {
	io.cm.mu.RLock()
	defer io.cm.mu.RUnlock()

	_, err := io.indexOps.GetIndexByName(io.tx, indexName)
	return err == nil
}

// ValidationResult contains the resolved metadata needed for index creation.
type ValidationResult struct {
	TableID     primitives.FileID
	ColumnIndex primitives.ColumnID
	ColumnType  types.Type
	TableName   string
	ColumnName  string
}

// ValidateIndexCreation performs all validation checks for creating a new index.
// This consolidates validation logic that was previously scattered across the planner layer.
//
// Validation steps:
//  1. Validates table exists and retrieves tableID
//  2. Validates column exists in table and retrieves column index and type
//  3. Validates index name is unique (not already used)
//
// Uses CatalogManager's read lock for thread safety during validation.
//
// Parameters:
//   - indexName: Name for the new index (must be unique)
//   - tableName: Name of the table to index
//   - columnName: Name of the column to index
//
// Returns:
//   - *ValidationResult: Resolved metadata needed for index creation
//   - error: Validation failure with descriptive message
func (io *IndexCatalogOperation) ValidateIndexCreation(indexName, tableName, columnName string) (*ValidationResult, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}
	if columnName == "" {
		return nil, fmt.Errorf("column name cannot be empty")
	}

	io.cm.mu.RLock()
	defer io.cm.mu.RUnlock()

	// Check index name is unique
	if exists, _ := io.indexOps.GetIndexByName(io.tx, indexName); exists != nil {
		return nil, fmt.Errorf("index %s already exists", indexName)
	}

	// Validate table exists
	tableID, err := io.getTableIDUnsafe(tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Validate column exists and get metadata
	cols, err := io.colOps.LoadColumnMetadata(io.tx, tableID)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %w", err)
	}

	var columnIndex primitives.ColumnID
	var columnType types.Type
	found := false
	for i, col := range cols {
		if col.Name == columnName {
			columnIndex = primitives.ColumnID(i)
			columnType = col.FieldType
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("column %s does not exist in table %s", columnName, tableName)
	}

	return &ValidationResult{
		TableID:     tableID,
		ColumnIndex: columnIndex,
		ColumnType:  columnType,
		TableName:   tableName,
		ColumnName:  columnName,
	}, nil
}

// ValidateIndexDeletion performs all validation checks for dropping an index.
// This consolidates validation logic that was previously in the planner layer.
//
// Validation steps:
//  1. Validates index exists and retrieves metadata
//  2. If tableName specified, validates index belongs to that table
//
// Uses CatalogManager's read lock for thread safety during validation.
//
// Parameters:
//   - indexName: Name of the index to drop
//   - tableName: Optional table name for ownership validation (empty string to skip)
//
// Returns:
//   - *systemtable.IndexMetadata: Metadata of the index to be dropped
//   - error: Validation failure with descriptive message
func (io *IndexCatalogOperation) ValidateIndexDeletion(indexName, tableName string) (*systemtable.IndexMetadata, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	io.cm.mu.RLock()
	defer io.cm.mu.RUnlock()

	// Check index exists
	metadata, err := io.indexOps.GetIndexByName(io.tx, indexName)
	if err != nil {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	// Validate table ownership if specified
	if tableName != "" {
		tableMetadata, err := io.cm.tableOps.GetTableMetadataByID(io.tx, metadata.TableID)
		if err != nil {
			return nil, fmt.Errorf("failed to verify table ownership: %w", err)
		}
		if tableMetadata.TableName != tableName {
			return nil, fmt.Errorf("index %s does not belong to table %s", indexName, tableName)
		}
	}

	return metadata, nil
}


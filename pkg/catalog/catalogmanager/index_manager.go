package catalogmanager

import (
	"fmt"
	"path/filepath"
	"slices"
	"storemy/pkg/catalog/schema"
	"storemy/pkg/catalog/systable"
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

// DropIndex removes an index from the catalog.
//
// Returns the metadata so caller can handle physical file cleanup.
func (cm *CatalogManager) DropIndex(tx *transaction.TransactionContext, name string) (*systable.IndexMetadata, error) {
	if name == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	metadata, err := cm.IndexTable.GetIndexByName(tx, name)
	if err != nil {
		return nil, fmt.Errorf("index %s not found: %w", name, err)
	}

	if err := cm.IndexTable.DeleteIndexFromCatalog(tx, metadata.IndexID); err != nil {
		return nil, fmt.Errorf("failed to remove index from catalog: %w", err)
	}
	return &metadata, nil
}

// GetIndexByName retrieves index metadata by name (case-sensitive).
func (cm *CatalogManager) GetIndexByName(tx *transaction.TransactionContext, indexName string) (*systable.IndexMetadata, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	m, err := cm.IndexTable.GetIndexByName(tx, indexName)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// CreateIndex creates a new index and registers it in the catalog.
//
// The indexID should be obtained from IndexManager.CreatePhysicalIndex() to ensure
// proper separation of concerns between physical storage and metadata layers.
func (cm *CatalogManager) CreateIndex(tx *transaction.TransactionContext, indexID primitives.FileID, indexName, tableName, columnName string, indexType index.IndexType) (filePath primitives.Filepath, err error) {
	if err := validateCreateIndexParams(indexID, indexName, tableName, columnName); err != nil {
		return "", err
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, err := cm.IndexTable.GetIndexByName(tx, indexName); err == nil {
		return "", fmt.Errorf("index %s already exists", indexName)
	}

	tableID, err := cm.getTableIDUnsafe(tx, tableName)
	if err != nil {
		return "", fmt.Errorf("table %s not found: %w", tableName, err)
	}

	ic := &indexCol{
		indexName:  indexName,
		tableName:  tableName,
		columnName: columnName,
		indexType:  indexType,
	}
	return cm.registerIndexWithTableID(tx, tableID, indexID, ic)
}

// validateCreateIndexParams validates all input parameters for CreateIndex.
func validateCreateIndexParams(indexID primitives.FileID, indexName, tableName, columnName string) error {
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
func (cm *CatalogManager) getTableIDUnsafe(tx *transaction.TransactionContext, tableName string) (primitives.FileID, error) {
	if id, err := cm.tableCache.GetTableID(tableName); err == nil {
		return id, nil
	}

	metadata, err := cm.TablesTable.GetByName(tx, tableName)
	if err != nil {
		return 0, err
	}
	return metadata.TableID, nil
}

// registerIndexWithTableID is an internal helper used when tableID is already known.
// Caller must hold cm.mu write lock.
func (cm *CatalogManager) registerIndexWithTableID(tx *transaction.TransactionContext, tableID, indexID primitives.FileID, ic *indexCol) (filePath primitives.Filepath, err error) {
	cols, err := cm.ColumnTable.LoadColumnMetadata(tx, tableID)
	if err != nil {
		return "", fmt.Errorf("failed to get table schema: %w", err)
	}

	if err := cm.validateColumnExists(cols, ic); err != nil {
		return "", err
	}

	metadata := cm.buildIndexMetadata(tableID, indexID, ic)

	if err := cm.persistIndexMetadata(tx, *metadata); err != nil {
		return "", fmt.Errorf("failed to register index in catalog: %w", err)
	}

	return metadata.FilePath, nil
}

// validateColumnExists checks that the column exists in the table.
func (cm *CatalogManager) validateColumnExists(cols []schema.ColumnMetadata, ic *indexCol) error {
	columnExists := slices.ContainsFunc(cols, func(col schema.ColumnMetadata) bool {
		return col.Name == ic.columnName
	})

	if !columnExists {
		return fmt.Errorf("column %s does not exist in table %s", ic.columnName, ic.tableName)
	}

	return nil
}

// persistIndexMetadata writes index metadata to catalog.
// Caller must hold cm.mu write lock.
func (cm *CatalogManager) persistIndexMetadata(tx *transaction.TransactionContext, metadata systable.IndexMetadata) error {
	if err := cm.IndexTable.Insert(tx, metadata); err != nil {
		return fmt.Errorf("failed to insert index tuple: %w", err)
	}
	return nil
}

// buildIndexMetadata constructs index metadata from components.
func (cm *CatalogManager) buildIndexMetadata(tableID, indexID primitives.FileID, ic *indexCol) *systable.IndexMetadata {
	fileName := fmt.Sprintf("%s_%s.idx", ic.tableName, ic.indexName)
	filePath := filepath.Join(cm.dataDir, fileName)
	return &systable.IndexMetadata{
		IndexID:    indexID,
		IndexName:  ic.indexName,
		TableID:    tableID,
		ColumnName: ic.columnName,
		IndexType:  ic.indexType,
		FilePath:   primitives.Filepath(filePath),
		CreatedAt:  time.Now(),
	}
}

// GetIndexesByTable returns all indexes for a given table.
func (cm *CatalogManager) GetIndexesByTable(tx *transaction.TransactionContext, tableID primitives.FileID) ([]*systable.IndexMetadata, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	results, err := cm.IndexTable.GetIndexesByTable(tx, tableID)
	if err != nil {
		return nil, err
	}
	ptrs := make([]*systable.IndexMetadata, len(results))
	for i := range results {
		r := results[i]
		ptrs[i] = &r
	}
	return ptrs, nil
}

// GetAllIndexes retrieves all indexes from the catalog.
func (cm *CatalogManager) GetAllIndexes(tx *transaction.TransactionContext) ([]*systable.IndexMetadata, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	results, err := cm.IndexTable.FindAll(tx, func(_ systable.IndexMetadata) bool {
		return true
	})
	if err != nil {
		return nil, err
	}
	ptrs := make([]*systable.IndexMetadata, len(results))
	for i := range results {
		r := results[i]
		ptrs[i] = &r
	}
	return ptrs, nil
}

// IndexExists checks if an index with the given name exists.
func (cm *CatalogManager) IndexExists(tx *transaction.TransactionContext, indexName string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	_, err := cm.IndexTable.GetIndexByName(tx, indexName)
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
//
// Validation steps:
//  1. Validates index name is unique
//  2. Validates table exists and retrieves tableID
//  3. Validates column exists in table and retrieves column index and type
func (cm *CatalogManager) ValidateIndexCreation(tx *transaction.TransactionContext, indexName, tableName, columnName string) (*ValidationResult, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}
	if columnName == "" {
		return nil, fmt.Errorf("column name cannot be empty")
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if _, err := cm.IndexTable.GetIndexByName(tx, indexName); err == nil {
		return nil, fmt.Errorf("index %s already exists", indexName)
	}

	tableID, err := cm.getTableIDUnsafe(tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	cols, err := cm.ColumnTable.LoadColumnMetadata(tx, tableID)
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
//
// Validation steps:
//  1. Validates index exists and retrieves metadata
//  2. If tableName specified, validates index belongs to that table
func (cm *CatalogManager) ValidateIndexDeletion(tx *transaction.TransactionContext, indexName, tableName string) (*systable.IndexMetadata, error) {
	if indexName == "" {
		return nil, fmt.Errorf("index name cannot be empty")
	}

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	metadata, err := cm.IndexTable.GetIndexByName(tx, indexName)
	if err != nil {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	if tableName != "" {
		tableMetadata, err := cm.TablesTable.GetByID(tx, metadata.TableID)
		if err != nil {
			return nil, fmt.Errorf("failed to verify table ownership: %w", err)
		}
		if tableMetadata.TableName != tableName {
			return nil, fmt.Errorf("index %s does not belong to table %s", indexName, tableName)
		}
	}

	return &metadata, nil
}

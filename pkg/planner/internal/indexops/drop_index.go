package indexops

import (
	"fmt"
	"storemy/pkg/concurrency/transaction"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/registry"
)

// DropIndexPlan represents the execution plan for DROP INDEX statement.
// It handles index removal from both the catalog and disk storage.
//
// Execution flow:
//  1. Validates index exists (respects IF EXISTS clause)
//  2. Removes index metadata from CATALOG_INDEXES
//  3. Deletes physical index file from disk
//  4. Returns DDL result with success message
//
// Transaction Semantics:
//   - Catalog changes are transactional (rollback on failure)
//   - File deletion is best-effort (warns if fails but doesn't rollback)
//   - Uses page-level locks via CatalogManager for CATALOG_INDEXES access
//
// Example:
//
//	DROP INDEX idx_users_email;                    -- Fails if not exists
//	DROP INDEX IF EXISTS idx_users_email;           -- Succeeds even if missing
//	DROP INDEX idx_users_email ON users;            -- Validates table ownership
type DropIndexPlan struct {
	Statement *statements.DropIndexStatement  // Parsed DROP INDEX statement
	ctx       *registry.DatabaseContext       // Database context for catalog/index access
	tx        *transaction.TransactionContext // Current transaction for catalog operations
}

// NewDropIndexPlan creates a new DROP INDEX plan instance.
//
// Parameters:
//   - stmt: Parsed DROP INDEX statement containing index name and IF EXISTS flag
//   - ctx: Database context providing access to CatalogManager and IndexManager
//   - *transaction.TransactionContext: Active transaction for catalog modifications
//
// Returns:
//
//	Plan ready for execution via Execute() method
func NewDropIndexPlan(
	stmt *statements.DropIndexStatement,
	ctx *registry.DatabaseContext, tx *transaction.TransactionContext,
) *DropIndexPlan {
	return &DropIndexPlan{
		Statement: stmt,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the DROP INDEX operation within the current transaction.
//
// Steps:
//  1. Checks if index exists via CatalogManager.IndexExists()
//  2. If missing and IF EXISTS specified, returns success without error
//  3. Validates table ownership if ON clause specified
//  4. Removes index entry from CATALOG_INDEXES table
//  5. Deletes physical index file from disk (best-effort)
//
// Returns:
//   - DDLResult with success message on completion
//   - Error if index doesn't exist (without IF EXISTS) or validation fails
func (p *DropIndexPlan) Execute() (result.Result, error) {
	cm := p.ctx.CatalogManager()
	idxName := p.Statement.IndexName

	if !cm.IndexExists(p.tx, idxName) {
		if p.Statement.IfExists {
			return result.NewDDLResult(true, fmt.Sprintf("Index %s does not exist (IF EXISTS)", idxName)), nil
		}
		return nil, fmt.Errorf("index %s does not exist", idxName)
	}

	if err := p.validateTable(); err != nil {
		return nil, err
	}

	if err := p.deleteIndex(); err != nil {
		return nil, err
	}

	return result.NewDDLResult(true, fmt.Sprintf("Index %s dropped successfully", idxName)), nil
}

// deleteIndex removes the index from catalog and deletes its physical file.
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
func (p *DropIndexPlan) deleteIndex() error {
	cm := p.ctx.CatalogManager()
	filePath, err := cm.DropIndex(p.tx, p.Statement.IndexName)
	if err != nil {
		return fmt.Errorf("failed to drop index from catalog: %w", err)
	}

	im := p.ctx.IndexManager().NewFileOps(filePath)
	if err := im.DeletePhysicalIndex(); err != nil {
		fmt.Printf("Warning: failed to delete index file %s: %v\n", filePath, err)
	}
	return nil
}

// validateTable verifies the index belongs to the specified table (if provided).
//
// This validation runs when DROP INDEX statement includes ON clause:
//
//	DROP INDEX idx_name ON table_name;
//
// Process:
//  1. Retrieves index metadata from catalog
//  2. Gets table name for index's TableID
//  3. Compares with statement's TableName
//
// Returns:
//   - nil if validation passes or no table specified
//   - Error if table name doesn't match or metadata lookup fails
func (p *DropIndexPlan) validateTable() error {
	cm := p.ctx.CatalogManager()
	idx, err := cm.GetIndexByName(p.tx, p.Statement.IndexName)
	if err != nil {
		return fmt.Errorf("failed to get index metadata: %w", err)
	}

	tableName := p.Statement.TableName
	if tableName != "" {
		tn, err := cm.GetTableName(p.tx, idx.TableID)
		if err != nil {
			return fmt.Errorf("failed to verify table name: %w", err)
		}
		if tn != tableName {
			return fmt.Errorf("index %s does not belong to table %s",
				p.Statement.IndexName, tableName)
		}
	}
	return nil
}

package ddl

import (
	"fmt"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/primitives"
)

// DropTablePlan represents the execution plan for DROP TABLE statement.
// It handles complete table removal including indexes, catalog entries, and physical files.
//
// Execution flow:
//  1. Validates table exists (respects IF EXISTS clause)
//  2. Drops all associated indexes (including auto-created primary key indexes)
//  3. Removes table metadata from CATALOG_TABLES
//  4. CatalogManager handles heap file deletion and buffer pool eviction
//
// Example:
//
//	DROP TABLE users;                    -- Fails if not exists
//	DROP TABLE IF EXISTS users;          -- Succeeds even if missing
type DropTablePlan struct {
	Statement *statements.DropStatement
	ctx       DbContext
	tx        TxContext
}

// NewDropTablePlan creates a new DROP TABLE plan instance.
//
// Parameters:
//   - stmt: Parsed DROP TABLE statement containing table name and IF EXISTS flag
//   - ctx: Database context providing access to CatalogManager and IndexManager
//   - tx: Active transaction for catalog modifications
func NewDropTablePlan(
	stmt *statements.DropStatement, ctx DbContext, tx TxContext) *DropTablePlan {
	return &DropTablePlan{
		Statement: stmt,
		ctx:       ctx,
		tx:        tx,
	}
}

// Execute performs the DROP TABLE operation within the current transaction.
//
// Execution steps:
//  1. Checks if table exists via CatalogManager.TableExists()
//  2. If missing and IF EXISTS specified, returns success without error
//  3. Retrieves table ID for cascade index deletion
//  4. Drops all indexes associated with the table (CASCADE behavior)
//  5. Removes table entry from CATALOG_TABLES
//  6. CatalogManager handles heap file cleanup and buffer pool eviction
//
// Returns:
//   - DDLResult with success message on completion
//   - Error if table doesn't exist (without IF EXISTS) or operation fails
func (p *DropTablePlan) Execute() (result.Result, error) {
	cm := p.ctx.CatalogManager()
	tableName := p.Statement.TableName

	if !cm.TableExists(p.tx, tableName) {
		if p.Statement.IfExists {
			return &result.DDLResult{
				Success: true,
				Message: fmt.Sprintf("Table %s does not exist (IF EXISTS)", tableName),
			}, nil
		}
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	tableID, err := cm.GetTableID(p.tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table ID: %w", err)
	}

	if err := p.dropTableIndexes(tableID); err != nil {
		return nil, fmt.Errorf("failed to drop table indexes: %w", err)
	}

	if err := cm.DropTable(p.tx, tableName); err != nil {
		return nil, fmt.Errorf("failed to drop table: %w", err)
	}

	return &result.DDLResult{
		Success: true,
		Message: fmt.Sprintf("Table %s dropped successfully", tableName),
	}, nil
}

// dropTableIndexes drops all indexes associated with a table (CASCADE semantics).
//
// This includes:
//   - User-created indexes (CREATE INDEX statements)
//   - Auto-generated primary key indexes (internal B+ trees)
//   - Any unique constraint indexes (if implemented)
//
// Process:
//  1. Retrieves all indexes for the table via CatalogManager.GetIndexesByTable()
//  2. For each index:
//     a. Removes from CATALOG_INDEXES via CatalogManager.DropIndex()
//     b. Deletes physical index file via IndexManager.DeletePhysicalIndex()
//  3. Logs warnings for failures but continues (best-effort cleanup)
//
// Returns:
//   - nil on success (even if some index deletions fail)
//   - Never returns error (failures are logged as warnings)
func (p *DropTablePlan) dropTableIndexes(tableID primitives.FileID) error {
	cm := p.ctx.CatalogManager()
	im := p.ctx.IndexManager()
	ops := cm.NewIndexOps(p.tx)
	indexes, err := ops.GetIndexesByTable(tableID)
	if err != nil {
		fmt.Printf("Warning: failed to get indexes for table: %v\n", err)
		return nil
	}

	var droppedCount int
	for _, indexMeta := range indexes {
		metadata, err := ops.DropIndex(indexMeta.IndexName)
		if err != nil {
			return fmt.Errorf("failed to drop index from catalog: %w", err)
		}

		if err := im.NewFileOps(metadata.FilePath).DeletePhysicalIndex(); err != nil {
			return fmt.Errorf("failed to delete index file %s: %w", metadata.FilePath, err)
		}
		droppedCount++
	}

	if droppedCount > 0 {
		fmt.Printf("Dropped %d index(es) associated with the table\n", droppedCount)
	}

	return nil
}

package ddl

import (
	"fmt"
	"storemy/pkg/catalog/catalogmanager"
	"storemy/pkg/parser/statements"
	"storemy/pkg/planner/internal/result"
	"storemy/pkg/primitives"

	"golang.org/x/sync/errgroup"
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
// Returns:
//   - DDLResult with success message on completion
//   - Error if table doesn't exist (without IF EXISTS) or operation fails
func (p *DropTablePlan) Execute() (result.Result, error) {
	cm := p.ctx.CatalogManager()
	tableName := p.Statement.TableName

	if !cm.TableExists(p.tx, tableName) {
		if p.Statement.IfExists {
			return result.NewDDLResult(true, fmt.Sprintf("Table %s does not exist (IF EXISTS)", tableName)), nil
		}
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	tableID, err := cm.GetTableID(p.tx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table ID: %w", err)
	}

	if err := p.drop(tableID); err != nil {
		return nil, fmt.Errorf("failed to drop table indexes: %w", err)
	}

	return result.NewDDLResult(true, fmt.Sprintf("Table %s dropped successfully", tableName)), nil
}

func (p *DropTablePlan) drop(tableID primitives.FileID) error {
	cm := p.ctx.CatalogManager()

	if err := p.dropTableIndexes(tableID); err != nil {
		return fmt.Errorf("failed to drop table indexes: %w", err)
	}

	if err := cm.DropTable(p.tx, p.Statement.TableName); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	return nil
}

// dropTableIndexes drops all indexes associated with a table (CASCADE semantics).
//
// Returns:
//   - nil on success (even if some index deletions fail)
//   - Never returns error (failures are logged as warnings)
func (p *DropTablePlan) dropTableIndexes(tableID primitives.FileID) error {
	ops := p.ctx.CatalogManager().NewIndexOps(p.tx)

	indexes, err := ops.GetIndexesByTable(tableID)
	if err != nil {
		return nil
	}

	var g errgroup.Group
	for _, indexMeta := range indexes {
		g.Go(func() error {
			return p.dropIndex(ops, indexMeta.IndexName)
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("getting error when dropping index %w", err)
	}
	return nil
}

func (p *DropTablePlan) dropIndex(ops *catalogmanager.IndexCatalogOperation, indexName string) error {
	metadata, err := ops.DropIndex(indexName)
	if err != nil {
		return fmt.Errorf("failed to drop index from catalog: %w", err)
	}

	if err := p.ctx.IndexManager().NewFileOps(metadata.FilePath).DeletePhysicalIndex(); err != nil {
		return fmt.Errorf("failed to delete index file %s: %w", metadata.FilePath, err)
	}

	return nil
}
